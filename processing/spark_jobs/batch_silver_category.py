"""
Spark Batch: bronze.category -> silver.category (Daily ETL, SCD1).
Reads T-1 partition (filter by report_date column — Bronze has no physical partition).
Intra-batch dedup keeps the latest row per category_id.
Derives _cdc_operation from is_current (1->0/1, 0->2). Drops is_current. NO partition.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import Window
import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, register_glue_table, write_delta_merge,
    enable_cdf, enable_deletion_vectors,
)
from delta.tables import DeltaTable

### spark session
spark = get_spark_session("Silver-Category")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def transform():
    bronze_path = get_s3_path("bronze", "category")
    bronze_df = (
        spark.read.format("delta").load(bronze_path)
        .filter(f.col("report_date") == run_date)
    )
    if bronze_df.count() == 0:
        print(f"[silver.category] No Bronze rows for {ymd}, skipping.")
        return

    w_dedup = Window.partitionBy("category_id").orderBy(f.col("updated_at").desc())
    silver_df = (
        bronze_df.withColumn("_rn", f.row_number().over(w_dedup))
        .filter(f.col("_rn") == 1)
        .drop("_rn", "report_date", "report_month", "_loaded_at")
        .withColumn("_cdc_operation",
            f.when(f.col("is_current") == 0, f.lit(2))
             .when(f.date_format(f.col("created_at"), "yyyyMMdd") == f.lit(run_date), f.lit(0))
             .otherwise(f.lit(1))
        )
        .drop("is_current")
        .withColumn("_processed_at", f.current_timestamp())
        .select(
            "category_id", "category_name", "_cdc_operation",
            "created_at", "updated_at", "_processed_at",
        )
    )

    print(f"[silver.category] Rows to merge: {silver_df.count()}")

    is_first_run = not DeltaTable.isDeltaTable(spark, SILVER_PATH)
    write_delta_merge(
        spark, silver_df, SILVER_PATH,
        merge_keys=["category_id"],
        partition_by=None,
        update_on_match=True,
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true",
        } if is_first_run else None,
    )
    if not is_first_run:
        enable_cdf(spark, SILVER_PATH)
        enable_deletion_vectors(spark, SILVER_PATH)
    register_glue_table(spark, "silver", "category", SILVER_PATH)
    print(f"[silver.category] MERGE complete for {ymd}.")


### Section 2: params
ymd = sys.argv[1]
ym = ymd[0:7]
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", ymd)
print("PARAM >>>", ym)
print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

run_date = ymd.replace("-", "")
SILVER_PATH = get_s3_path("silver", "category")


### Section 3: run
transform()

### Section 4: stop
spark.stop()
