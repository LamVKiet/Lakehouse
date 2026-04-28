"""
Spark Batch: bronze.transactions -> silver.nou (Daily ETL)
Registry of first-ever order per customer (1 row per customer).
Logic: find new customers from today's Bronze, then scan ALL Bronze
to compute their true first-ever order date (nou_ymd).
MERGE: only insert new customers (whenNotMatchedInsertAll).
Partition by ym (yyyyMM).
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from delta.tables import DeltaTable
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, register_glue_table, write_delta_merge,
)

### spark session
spark = get_spark_session("Silver-NOU")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def transform():
    bronze_path = get_s3_path("bronze", "transactions")

    ### step 1: read Bronze T-1 day to find today's customers
    today_df = (
        spark.read.format("delta").load(bronze_path)
        .filter(f.col("report_date") == run_date)
        .filter(f.col("customer_id").isNotNull())
    )
    if today_df.count() == 0:
        print(f"[silver.nou] No Bronze rows for {ymd}, skipping.")
        return
    today_customers = today_df.select("customer_id").distinct()

    ### step 2: find truly new customers (not yet in silver.nou)
    if DeltaTable.isDeltaTable(spark, NOU_PATH):
        existing = spark.read.format("delta").load(NOU_PATH).select("customer_id")
        new_customer_ids = today_customers.join(existing, on="customer_id", how="left_anti")
    else:
        new_customer_ids = today_customers

    new_count = new_customer_ids.count()
    if new_count == 0:
        print(f"[silver.nou] No new customers for {ymd}, skipping.")
        return
    print(f"[silver.nou] New customers: {new_count}")

    ### step 3: scan ALL Bronze to compute true first-ever order date
    all_bronze = spark.read.format("delta").load(bronze_path)
    new_customers = (
        all_bronze
        .join(f.broadcast(new_customer_ids), on="customer_id", how="inner")
        .groupBy("customer_id")
        .agg(f.min(f.date_format(f.col("transaction_datetime"), "yyyyMMdd")).alias("nou_ymd"))
        .withColumn("ym", f.substring("nou_ymd", 1, 6))
        .withColumn("etl_datetime", f.current_timestamp())
    )
    new_customers.show(5)

    ### step 4: Delta MERGE — insert new customers into silver.nou
    write_delta_merge(spark, new_customers, NOU_PATH, merge_keys=["customer_id"], partition_by="ym")
    register_glue_table(spark, "silver", "nou", NOU_PATH)
    print(f"[silver.nou] Inserted {new_customers.count()} customers for {ymd}.")


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
NOU_PATH = get_s3_path("silver", "nou")


### Section 3: run
transform()

### Section 4: stop
spark.stop()
