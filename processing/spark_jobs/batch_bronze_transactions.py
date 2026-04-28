"""
Spark Batch: MySQL pos_transactions + online_transactions -> S3/Delta bronze.transactions
Merged into single table with channel column (offline/online).
Partition by report_date (yyyyMMdd). Append-only (T-1 day).
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_mysql_incremental,
    register_glue_table, write_delta_append,
)

### spark session
spark = get_spark_session("Bronze-Transactions")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def load_transactions():
    pos = read_mysql_incremental(spark, "pos_transactions", ymd)
    onl = read_mysql_incremental(spark, "online_transactions", ymd)
    pos_count, onl_count = pos.count(), onl.count()
    if pos_count == 0 and onl_count == 0:
        print(f"[transactions] No rows for {ymd}, skipping.")
        return
    print(f"[transactions] POS: {pos_count}, Online: {onl_count}")
    # POS already has trans_total_line + trans_total_sell_sku
    pos_df = pos.withColumn("channel", f.lit("offline"))
    # Online lacks those columns — fill with null
    onl_df = (
        onl
        .withColumn("channel", f.lit("online"))
        .withColumn("trans_total_line", f.lit(None).cast("int"))
        .withColumn("trans_total_sell_sku", f.lit(None).cast("int"))
    )
    unified = pos_df.unionByName(onl_df)
    df = (
        unified
        .withColumn("report_date",  f.date_format(f.coalesce(f.col("updated_at"), f.col("created_at")), "yyyyMMdd"))
        .withColumn("report_month", f.date_format(f.coalesce(f.col("updated_at"), f.col("created_at")), "yyyyMM"))
        .withColumn("_loaded_at", f.current_timestamp())
        .select(
            "report_date", "report_month", "channel",
            "branch_id", "transaction_id", "transaction_datetime", "payment_type",
            "trans_total_amount", "trans_total_line", "trans_total_sell_sku",
            "customer_id", "order_status", "delivery_date",
            "created_at", "updated_at", "_loaded_at",
        )
    )
    df.show(3)
    path = get_s3_path("bronze", "transactions")
    write_delta_append(df, path, partition_by="report_date")
    register_glue_table(spark, "bronze", "transactions", path)
    print(f"[transactions] Wrote {df.count()} rows for {ymd}.")


### Section 2: params
ymd = sys.argv[1]
ym = ymd[0:7]
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", ymd)
print("PARAM >>>", ym)
print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

run_date = ymd.replace("-", "")
run_month = ym.replace("-", "")


### Section 3: run
load_transactions()

### Section 4: stop
spark.stop()
