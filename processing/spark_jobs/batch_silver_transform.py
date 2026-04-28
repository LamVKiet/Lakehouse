"""
Spark Batch: Bronze -> Silver events (T-1 day).
Triggered by Airflow daily at 02:30. Receives date as sys.argv[1].

Transforms:
  - Parse created_at string -> TimestampType (event_time)
  - Flatten raw_metadata JSON -> individual typed columns
  - Filter out rows with null event_uuid or event_ts = 0
  - Add is_transaction flag (event_id IN 12, 13: place_order / payment_callback)
  - Add source_topic from _kafka_topic

Dedup: Delta MERGE INTO by (event_uuid, log_date) — idempotent, safe to re-run.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from pyspark.sql import Window
from processing.schemas.event_schema import METADATA_SCHEMA
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, register_glue_table, write_delta_merge,
    ensure_constraint,
)

### spark session
spark = get_spark_session("Silver-Events")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def transform():
    bronze_path = get_s3_path("bronze", "behavior_events")
    try:
        bronze_df = (
            spark.read.format("delta").load(bronze_path)
            .filter(f.col("log_date") == ymd)
        )
    except Exception as e:
        print(f"[silver.events] Could not read Bronze: {e}")
        return

    row_count = bronze_df.count()
    if row_count == 0:
        print(f"[silver.events] No Bronze rows for {ymd}, skipping.")
        return
    print(f"[silver.events] Bronze rows: {row_count}")

    ### intra-batch dedup: Kafka may deliver duplicates on retry
    w_dedup = Window.partitionBy("event_uuid").orderBy(f.col("_ingested_at").desc())
    bronze_df = (
        bronze_df.withColumn("_rn", f.row_number().over(w_dedup))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
    )

    silver_df = (
        bronze_df
        .withColumn("meta", f.from_json(f.col("raw_metadata"), METADATA_SCHEMA))
        .withColumn("event_time", f.to_timestamp(f.col("created_at")))
        .withColumn("is_success", f.col("meta.is_success"))
        .withColumn("error_message", f.col("meta.error_message"))
        .withColumn("product_id", f.col("meta.product_id"))
        .withColumn("product_name", f.col("meta.product_name"))
        .withColumn("item_quantity",
            f.coalesce(
                f.col("meta.quantity"),
                f.col("meta.removed_quantity"),
                f.col("meta.new_quantity"),
                f.col("meta.total_items"),
                f.lit(0),
            )
        )
        .withColumn("is_transaction", f.col("event_id").isin(12, 13))
        .withColumn("source_topic", f.col("_kafka_topic"))
        .withColumn("_processed_at", f.current_timestamp())
        .filter(f.col("event_uuid").isNotNull() & (f.col("event_ts") > 0))
        .select(
            "event_uuid", "event_id", "event_type", "event_time", "log_date",
            "user_id", "device_os", "app_version",
            "is_success", "error_message", "product_id", "product_name", "item_quantity",
            "is_transaction", "source_topic", "_processed_at",
        )
    )

    out_count = silver_df.count()
    if out_count == 0:
        print(f"[silver.events] No valid rows after filtering, skipping.")
        return
    print(f"[silver.events] Silver rows to write: {out_count}")

    silver_path = get_s3_path("silver", "events")
    write_delta_merge(spark, silver_df, silver_path, merge_keys=["event_uuid", "log_date"], partition_by="log_date")
    ensure_constraint(spark, silver_path, "valid_event_id", "event_id BETWEEN 1 AND 13")
    register_glue_table(spark, "silver", "events", silver_path)
    print(f"[silver.events] MERGE complete for {ymd}.")


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
transform()

### Section 4: stop
spark.stop()
