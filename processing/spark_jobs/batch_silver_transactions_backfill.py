"""
Spark Batch: bronze.transactions -> silver.transactions (Backfill — run once)
Reads ALL Bronze transactions, normalizes + writes Silver. NO retention logic
(retention is computed at Gold via JOIN with silver.customer_activity_monthly).
No date parameter needed.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import Window
import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, register_glue_table,
)

spark = get_spark_session("Silver-Transactions-Backfill")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


def backfill():
    bronze_path = get_s3_path("bronze", "transactions")
    bronze_df = spark.read.format("delta").load(bronze_path)
    row_count = bronze_df.count()
    if row_count == 0:
        print("[silver.transactions backfill] Bronze empty, skipping.")
        return
    print(f"[silver.transactions backfill] Bronze rows: {row_count}")

    w_dedup = Window.partitionBy("transaction_id", "branch_id").orderBy(f.col("created_at").desc())
    bronze_df = (
        bronze_df.withColumn("_rn", f.row_number().over(w_dedup))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
    )

    base_df = (
        bronze_df
        .withColumn("trans_month", f.date_format(f.col("transaction_datetime"), "yyyyMM"))
        .withColumn("trans_date",  f.to_date(f.col("transaction_datetime")))
        .withColumn("trans_time",  f.date_format(f.col("transaction_datetime"), "HH:mm:ss"))
        .withColumn("timestamp",   f.unix_timestamp(f.col("transaction_datetime")))
        .withColumn("ym",  f.date_format(f.col("transaction_datetime"), "yyyyMM"))
        .withColumn("ymd", f.date_format(f.col("transaction_datetime"), "yyyyMMdd"))
        .withColumn("trans_id", f.concat_ws("_",
            f.date_format(f.col("transaction_datetime"), "yyyyMMdd"),
            f.col("customer_id"),
            f.col("transaction_id"),
        ))
        .withColumn("order_total_amount", f.col("trans_total_amount"))
    )

    result = (
        base_df
        .withColumn("updated_at", f.col("created_at"))
        # Seed source_ts_ms from created_at (batch source has no Debezium binlog ts) so the
        # CDC daily MERGE guard `s.source_ts_ms > t.source_ts_ms` works: live CDC commits
        # (binlog ts ~ now) always outrank these historical seeds.
        .withColumn("source_ts_ms", f.expr("unix_millis(created_at)"))
        .withColumn("etl_datetime", f.current_timestamp())
        .select(
            "trans_month", "trans_date", "trans_time", "timestamp",
            "trans_id",
            "customer_id",
            "branch_id", "channel",
            "order_status", "payment_type",
            "order_total_amount",
            "created_at", "updated_at", "source_ts_ms",
            "etl_datetime", "ym", "ymd",
        )
    )
    result.cache()
    bf_count = result.count()

    (
        result.write.format("delta").mode("overwrite")
        .partitionBy("branch_id", "ym", "ymd")
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.enableDeletionVectors", "true")
        .save(SILVER_PATH)
    )
    register_glue_table(spark, "silver", "transactions", SILVER_PATH)
    print(f"[silver.transactions backfill] Complete: {bf_count} rows.")


today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

SILVER_PATH = get_s3_path("silver", "transactions")


backfill()
spark.stop()
