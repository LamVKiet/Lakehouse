"""
Spark Batch: bronze.transactions -> silver.nou (Daily ETL)
Registry of first-ever order per customer (1 row per customer).
Logic: find new customers from today's Bronze, then scan ALL Bronze
to compute their true first-ever order date (nou_ymd).
MERGE: only insert new customers (whenNotMatchedInsertAll).
Partition by ym (yyyyMM).

transform() is pure (df -> df, no I/O) so it is unit-testable; main() owns all I/O.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from delta.tables import DeltaTable
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_delta_partitions, register_glue_table, write_delta_merge,
)


### Section 1: transform (pure — unit-testable)
def transform(today_df: DataFrame, all_bronze_df: DataFrame, existing_df: DataFrame | None = None) -> DataFrame:
    today_customers = today_df.filter(f.col("customer_id").isNotNull()).select("customer_id").distinct()
    if existing_df is not None:
        new_ids = today_customers.join(existing_df.select("customer_id"), on="customer_id", how="left_anti")
    else:
        new_ids = today_customers
    return (
        all_bronze_df
        .join(f.broadcast(new_ids), on="customer_id", how="inner")
        .groupBy("customer_id")
        .agg(f.min(f.date_format(f.col("transaction_datetime"), "yyyyMMdd")).alias("nou_ymd"))
        .withColumn("ym", f.substring("nou_ymd", 1, 6))
        .withColumn("etl_datetime", f.current_timestamp())
    )


### Section 2: main (I/O — Spark session, read, write, register)
def main():
    spark = get_spark_session("Silver-NOU")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

    ymd = sys.argv[1]
    ym = ymd[0:7]
    today = datetime.now().strftime("%Y-%m-%d")
    print("PARAM >>>", ymd)
    print("PARAM >>>", ym)
    print("PARAM >>>", today)
    print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    run_date = ymd.replace("-", "")
    NOU_PATH = get_s3_path("silver", "nou")

    today_df = read_delta_partitions(spark, "bronze", "transactions", "in_ymd", [run_date])
    if today_df.count() == 0:
        print(f"[silver.nou] No Bronze rows for {ymd}, skipping.")
        spark.stop()
        return

    existing_df = None
    if DeltaTable.isDeltaTable(spark, NOU_PATH):
        existing_df = spark.read.format("delta").load(NOU_PATH).select("customer_id")
    all_bronze = spark.read.format("delta").load(get_s3_path("bronze", "transactions"))

    new_customers = transform(today_df, all_bronze, existing_df).cache()
    new_count = new_customers.count()
    if new_count == 0:
        print(f"[silver.nou] No new customers for {ymd}, skipping.")
        spark.stop()
        return
    print(f"[silver.nou] New customers: {new_count}")

    write_delta_merge(spark, new_customers, NOU_PATH, merge_keys=["customer_id"], partition_by="ym")
    register_glue_table(spark, "silver", "nou", NOU_PATH)
    print(f"[silver.nou] Inserted {new_count} customers for {ymd}.")
    spark.stop()


if __name__ == "__main__":
    main()
