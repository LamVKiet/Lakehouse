"""
Spark Batch: Bronze -> Silver events_discovery (T-1 day).
Filter event_id IN (1,2,3,4): home_screen_view, search, view_item, select_item_variant.
Serves PO Growth / Marketing — search performance, position CTR, view-to-variant rate.

Dedup: Delta MERGE INTO by (event_uuid, log_date) — idempotent.
"""

import os, sys
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
spark = get_spark_session("Silver-Events-Discovery")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

DISCOVERY_EVENT_IDS = [1, 2, 3, 4]


### Section 1: functions
def transform():
    bronze_path = get_s3_path("bronze", "behavior_events")
    try:
        bronze_df = (
            spark.read.format("delta").load(bronze_path)
            .filter(f.col("log_date") == ymd)
            .filter(f.col("event_id").isin(DISCOVERY_EVENT_IDS))
        )
    except Exception as e:
        print(f"[silver.events_discovery] Could not read Bronze: {e}")
        return

    row_count = bronze_df.count()
    if row_count == 0:
        print(f"[silver.events_discovery] No Bronze rows for {ymd}, skipping.")
        return
    print(f"[silver.events_discovery] Bronze rows: {row_count}")

    ### intra-batch dedup
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
        .withColumn("trans_event_id",
            f.concat_ws("_", f.date_format("event_time", "yyyyMMdd"), f.col("user_id"), f.col("event_id")))
        .withColumn("meta_app_version", f.col("meta.app_version"))
        .withColumn("source_screen", f.col("meta.source_screen"))
        .withColumn("source_element", f.col("meta.source_element"))
        .withColumn("position", f.col("meta.position"))
        .withColumn(
            "search_keyword",
            # lowercase + trim + collapse multiple whitespaces
            f.regexp_replace(f.trim(f.lower(f.col("meta.search_keyword"))), r"\s+", " "),
        )
        .withColumn("result_count", f.col("meta.result_count"))
        .withColumn("product_id", f.col("meta.product_id"))
        .withColumn("product_name", f.col("meta.product_name"))
        .withColumn("base_price", f.col("meta.base_price"))
        .withColumn("variant_type", f.col("meta.variant_type"))
        .withColumn("variant_value", f.col("meta.variant_value"))
        .withColumn("_processed_at", f.current_timestamp())
        .filter(f.col("event_uuid").isNotNull() & (f.col("event_ts") > 0))
        .select(
            "event_uuid", "trans_event_id", "event_id", "event_type", "event_time", "log_date",
            "session_id", "user_id", "device_os", "app_version", "meta_app_version",
            "source_screen", "source_element", "position",
            "search_keyword", "result_count",
            "product_id", "product_name", "base_price",
            "variant_type", "variant_value",
            "_processed_at",
        )
    )

    out_count = silver_df.count()
    if out_count == 0:
        print(f"[silver.events_discovery] No valid rows after filtering, skipping.")
        return
    print(f"[silver.events_discovery] Silver rows to write: {out_count}")

    silver_path = get_s3_path("silver", "events_discovery")
    write_delta_merge(spark, silver_df, silver_path, merge_keys=["event_uuid", "log_date"], partition_by="log_date")
    ensure_constraint(spark, silver_path, "valid_event_id_discovery", "event_id IN (1,2,3,4)")
    register_glue_table(spark, "silver", "events_discovery", silver_path)
    print(f"[silver.events_discovery] MERGE complete for {ymd}.")


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
