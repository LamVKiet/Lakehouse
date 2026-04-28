"""
Spark Batch: bronze.customers -> silver.customers (Daily ETL, SCD1).
Reads T-1 partition. Intra-batch dedup keeps the latest row per customer_id.
MERGE with update_on_match=True so dim snapshot stays current.
1 row per customer (no history) — query without is_current filter.
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
spark = get_spark_session("Silver-Customers")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def transform():
    bronze_path = get_s3_path("bronze", "customers")
    bronze_df = (
        spark.read.format("delta").load(bronze_path)
        .filter(f.col("report_date") == run_date)
    )
    if bronze_df.count() == 0:
        print(f"[silver.customers] No Bronze rows for {ymd}, skipping.")
        return

    ### intra-batch dedup: keep latest row per customer
    w_dedup = Window.partitionBy("customer_id").orderBy(f.col("updated_at").desc())
    silver_df = (
        bronze_df.withColumn("_rn", f.row_number().over(w_dedup))
        .filter(f.col("_rn") == 1)
        .drop("_rn", "report_date", "report_month", "_loaded_at")
    )

    ### enrich: full_name + phone mask + gender map + city extract + _cdc_operation
    silver_df = (
        silver_df
        .withColumn("full_name",
            f.trim(f.regexp_replace(
                f.concat_ws(" ", f.col("last_name"), f.col("first_name")),
                r"\s+", " ",
            ))
        )
        .withColumn("phone",
            f.when(f.col("phone").isNull(), f.lit(None).cast("string"))
             .otherwise(f.concat(
                 f.repeat(f.lit("x"), f.length("phone") - 3),
                 f.substring(f.col("phone"), -3, 3),
             ))
        )
        .withColumn("gender",
            f.when(f.col("gender") == 1, f.lit("M"))
             .when(f.col("gender") == 2, f.lit("F"))
             .otherwise(f.lit("O"))
        )
        .withColumn("city",
            f.when(f.col("address_line").isNull(), f.lit("unknown"))
             .otherwise(f.coalesce(
                 f.trim(f.element_at(f.split(f.col("address_line"), ","), -1)),
                 f.lit("unknown"),
             ))
        )
        .withColumn("_cdc_operation",
            f.when(f.col("is_deleted") == 1, f.lit(2))
             .when(f.date_format(f.col("created_at"), "yyyyMMdd") == f.lit(run_date), f.lit(0))
             .otherwise(f.lit(1))
        )
        .withColumn("_processed_at", f.current_timestamp())
        .select(
            "customer_id", "full_name", "phone", "dob", "age", "gender", "city",
            "_cdc_operation",
            "registered_datetime", "updated_at", "source", "_processed_at",
        )
    )

    print(f"[silver.customers] Rows to merge: {silver_df.count()}")

    is_first_run = not DeltaTable.isDeltaTable(spark, SILVER_PATH)
    write_delta_merge(
        spark, silver_df, SILVER_PATH,
        merge_keys=["customer_id"],
        partition_by="source",
        update_on_match=True,
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true",
        } if is_first_run else None,
    )
    if not is_first_run:
        enable_cdf(spark, SILVER_PATH)
        enable_deletion_vectors(spark, SILVER_PATH)
    register_glue_table(spark, "silver", "customers", SILVER_PATH)
    print(f"[silver.customers] MERGE complete for {ymd}.")


### Section 2: params
ymd = sys.argv[1]
ym = ymd[0:7]
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", ymd)
print("PARAM >>>", ym)
print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

run_date = ymd.replace("-", "")
SILVER_PATH = get_s3_path("silver", "customers")


### Section 3: run
transform()

### Section 4: stop
spark.stop()
