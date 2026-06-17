"""
Spark Batch: bronze.transactions -> silver.customer_activity_monthly (Backfill — run once)
Reads ALL Bronze transactions, aggregates per (customer_id, ym), full overwrite.
No date parameter needed. Run once after initial Bronze load.
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

spark = get_spark_session("Silver-CustomerActivityMonthly-Backfill")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


def backfill():
    bronze_path = get_s3_path("bronze", "transactions")
    bronze_df = (
        spark.read.format("delta").load(bronze_path)
        .filter(f.col("customer_id").isNotNull())
    )
    row_count = bronze_df.count()
    if row_count == 0:
        print("[silver.customer_activity_monthly backfill] Bronze empty, skipping.")
        return
    print(f"[silver.customer_activity_monthly backfill] Bronze rows: {row_count}")

    ### intra-batch dedup
    w_dedup = Window.partitionBy("transaction_id", "branch_id").orderBy(f.col("created_at").desc())
    bronze_df = (
        bronze_df.withColumn("_rn", f.row_number().over(w_dedup))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
    )

    result = (bronze_df
        .withColumn("ym", f.date_format(f.col("transaction_datetime"), "yyyyMM"))
        .groupBy("customer_id", "ym")
        .agg(f.min("transaction_datetime").alias("first_trans_datetime"),
            f.max("transaction_datetime").alias("last_trans_datetime"),
            f.countDistinct("transaction_id").cast("int").alias("trans_count"),
        )
        .withColumn("etl_datetime", f.current_timestamp())
        .select("customer_id", "ym", "first_trans_datetime", "last_trans_datetime",
                "trans_count", "etl_datetime")
    )
    result.cache()
    bf_count = result.count()
    print(f"[silver.customer_activity_monthly backfill] Result rows: {bf_count}")

    result.write.format("delta").mode("overwrite").partitionBy("ym").save(ACTIVITY_PATH)
    register_glue_table(spark, "silver", "customer_activity_monthly", ACTIVITY_PATH)
    print("[silver.customer_activity_monthly backfill] Complete.")


today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

ACTIVITY_PATH = get_s3_path("silver", "customer_activity_monthly")

backfill()
spark.stop()
