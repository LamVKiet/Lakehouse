"""
Spark Batch: MySQL pos/online transaction details -> S3/Delta bronze.transaction_details
Pre-aggregated per transaction_id: count lines, sum qty, collect metadata array.
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
spark = get_spark_session("Bronze-TransactionDetails")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def load_transaction_details():
    pos = read_mysql_incremental(spark, "pos_transaction_details", ymd, date_col="created_at")
    onl = read_mysql_incremental(spark, "online_transaction_details", ymd, date_col="created_at")
    pos_count, onl_count = pos.count(), onl.count()
    if pos_count == 0 and onl_count == 0:
        print(f"[transaction_details] No rows for {ymd}, skipping.")
        return
    print(f"[transaction_details] POS: {pos_count}, Online: {onl_count}")
    # POS detail has branch_id; online detail does not
    pos_df = pos.withColumn("channel", f.lit("offline"))
    onl_df = (
        onl
        .withColumn("branch_id", f.lit(None).cast("string"))
        .withColumn("channel", f.lit("online"))
    )
    unified = pos_df.unionByName(onl_df)
    # Aggregate detail rows -> 1 row per transaction
    agg_df = (
        unified
        .groupBy("transaction_id", "channel", "branch_id")
        .agg(
            f.first("created_at").alias("created_at"),
            f.countDistinct(
                f.when(f.col("is_promo") == 0, f.col("transaction_detail_id"))
            ).cast("bigint").alias("order_total_line"),
            f.sum(
                f.when(f.col("is_promo") == 0, f.col("trans_qty")).otherwise(0)
            ).cast("bigint").alias("order_total_sku"),
            f.countDistinct(
                f.when(f.col("is_promo") == 1, f.col("transaction_detail_id"))
            ).cast("bigint").alias("promo_total_line"),
            f.sum(
                f.when(f.col("is_promo") == 1, f.col("trans_qty")).otherwise(0)
            ).cast("bigint").alias("promo_total_sku"),
            f.max("is_promo").alias("is_promo"),
            f.collect_list(
                f.array(
                    f.col("transaction_detail_id"),
                    f.col("product_id"),
                    f.col("is_promo").cast("string"),
                    f.col("product_uom_code"),
                    f.col("trans_qty").cast("string"),
                    f.col("trans_line_amount").cast("string"),
                    f.coalesce(f.col("variant_size"),  f.lit("")),
                    f.coalesce(f.col("variant_color"), f.lit("")),
                )
            ).alias("trans_metadata"),
        )
    )
    df = (
        agg_df
        .withColumn("report_date", f.date_format(f.col("created_at"), "yyyyMMdd"))
        .withColumn("_loaded_at", f.current_timestamp())
        .select(
            "report_date", "channel", "branch_id", "transaction_id",
            "order_total_line", "order_total_sku", "promo_total_line", "promo_total_sku",
            "is_promo", "trans_metadata", "created_at", "_loaded_at",
        )
    )
    df.show(3)
    path = get_s3_path("bronze", "transaction_details")
    write_delta_append(df, path, partition_by="report_date")
    register_glue_table(spark, "bronze", "transaction_details", path)
    print(f"[transaction_details] Wrote {df.count()} rows for {ymd}.")


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
load_transaction_details()

### Section 4: stop
spark.stop()
