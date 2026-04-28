"""
Spark Batch: bronze.transactions -> silver.nou (Backfill — run once)
Reads ALL Bronze transactions, computes first-ever order per customer.
Overwrites silver.nou entirely. No date parameter needed.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, register_glue_table,
)

### spark session
spark = get_spark_session("Silver-NOU-Backfill")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def backfill():
    ### step 1: read ALL Bronze transactions
    bronze_path = get_s3_path("bronze", "transactions")
    bronze_df = spark.read.format("delta").load(bronze_path)
    row_count = bronze_df.count()
    if row_count == 0:
        print("[silver.nou backfill] Bronze empty, skipping.")
        return
    print(f"[silver.nou backfill] Bronze rows: {row_count}")

    ### step 2: first-ever order per customer across all history
    result = (
        bronze_df
        .withColumn("ymd", f.date_format(f.col("transaction_datetime"), "yyyyMMdd"))
        .groupBy("customer_id")
        .agg(f.min("ymd").alias("nou_ymd"))
        .withColumn("ym", f.substring("nou_ymd", 1, 6))
        .withColumn("etl_datetime", f.current_timestamp())
    )

    ### step 3: overwrite silver.nou
    result.write.format("delta").mode("overwrite").partitionBy("ym").save(NOU_PATH)
    register_glue_table(spark, "silver", "nou", NOU_PATH)
    print(f"[silver.nou backfill] Complete: {result.count()} customers.")


### Section 2: params
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

NOU_PATH = get_s3_path("silver", "nou")


### Section 3: run
backfill()

### Section 4: stop
spark.stop()
