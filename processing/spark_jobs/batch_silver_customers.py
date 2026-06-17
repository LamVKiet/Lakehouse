"""
Spark Batch: bronze.customers -> silver.customers (Daily ETL, SCD1).
Reads T-1 partition. Intra-batch dedup keeps the latest row per customer_id.
MERGE with update_on_match=True so dim snapshot stays current.
1 row per customer (no history) — query without is_current filter.

transform() is pure (df -> df, no I/O) so it is unit-testable; main() owns all I/O.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_delta_partitions, register_glue_table, write_delta_merge,
    enable_cdf, enable_deletion_vectors,
)
from delta.tables import DeltaTable


### Section 1: transform (pure — unit-testable)
def transform(bronze_df: DataFrame, run_date: str) -> DataFrame:
    w_dedup = Window.partitionBy("customer_id").orderBy(f.col("updated_at").desc())
    silver_df = (
        bronze_df.withColumn("_rn", f.row_number().over(w_dedup))
        .filter(f.col("_rn") == 1)
        .drop("_rn", "report_date", "report_month", "_loaded_at")
    )
    return (silver_df
        .withColumn("full_name",
            f.trim(f.regexp_replace(
                f.concat_ws(" ", f.col("last_name"), f.col("first_name")),
                r"\s+", " ",
            ))
        )
        .withColumn("phone",
            f.when(f.col("phone").isNull(), f.lit(None).cast("string"))
             .otherwise(f.concat(
                 f.expr("repeat('x', length(phone) - 3)"),  # repeat() needs int n; use SQL for dynamic length
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


### Section 2: main (I/O — Spark session, read, write, register)
def main():
    spark = get_spark_session("Silver-Customers")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

    ymd = sys.argv[1]
    ym = ymd[0:7]
    today = datetime.now().strftime("%Y-%m-%d")
    print("PARAM >>>", ymd)
    print("PARAM >>>", ym)
    print("PARAM >>>", today)
    print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    run_date = ymd.replace("-", "")
    SILVER_PATH = get_s3_path("silver", "customers")

    bronze_df = read_delta_partitions(spark, "bronze", "customers", "in_ymd", [run_date])
    if bronze_df.count() == 0:
        print(f"[silver.customers] No Bronze rows for {ymd}, skipping.")
        spark.stop()
        return

    silver_df = transform(bronze_df, run_date)
    print(f"[silver.customers] Rows to merge: {silver_df.cache().count()}")

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
    spark.stop()


if __name__ == "__main__":
    main()
