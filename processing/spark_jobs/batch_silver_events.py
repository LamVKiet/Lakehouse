"""
Spark Batch: bronze.behavior_events (raw clickstream payload) -> silver.events (T-1 day).
Unified job: ONE read + ONE parse + ONE dedup for all 14 event types (replaces the old
per-bucket events_discovery/cart/checkout jobs). Funnel bucketing is deferred to the marts.

Logic (pure functions, testable):
  1. read bronze WHERE report_date == run_date (ingestion-date partition).
  2. parse_events: from_json(payload, EVENT_SCHEMA) -> flatten top-level cols + keep
     `metadata` as a parsed struct (NOT exploded — marts flatten per bucket).
  3. transform: dropDuplicates(event_uuid) — events are immutable, any copy is identical so
     no order-by/latest-wins needed (cheaper than a Window+sort). Derive funnel_stage from
     event_id, ymd from log_date, drop invalid rows.
  4. MERGE INTO (insert-only) by (event_uuid, ymd); partition (ymd, funnel_stage).

Why dedup is still needed: producer is acks=1, no idempotence, + DLQ resend -> the same
event_uuid can be delivered to Kafka twice -> both land in Bronze. UUID is unique at
generation, not at delivery. MERGE gives cross-run idempotency; dropDuplicates handles
within-batch duplicates (MERGE requires a unique source key).
"""

import os, sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from processing.schemas.event_schema import EVENT_SCHEMA
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_delta_partitions, register_glue_table,
    write_delta_merge, ensure_constraint,
)

# discovery 1-4 / cart 5-8 / checkout 9-14 — conformed once here, marts filter on it.
# Wrapped in a fn (not a module-level Column) so the module imports without a SparkContext.
def funnel_stage():
    return (f.when(f.col("event_id") <= 4, f.lit("discovery"))
             .when(f.col("event_id") <= 8, f.lit("cart"))
             .otherwise(f.lit("checkout")))
SILVER_COLS = [
    "event_uuid", "event_id", "event_type", "funnel_stage",
    "event_time", "event_ts", "log_date",
    "session_id", "user_id", "device_os", "app_version",
    "metadata", "ymd", "_ingested_at", "_processed_at",
]


### Section 1: functions
def parse_events(raw):
    """Parse the whole-event JSON payload; keep metadata as a struct. Narrow projection
    before the dedup shuffle so it carries flat rows, not the giant payload string."""
    return (
        raw
        .withColumn("e", f.from_json(f.col("payload"), EVENT_SCHEMA))
        .select(
            f.col("e.event_uuid").alias("event_uuid"),
            f.col("e.event_id").alias("event_id"),
            f.col("e.event_type").alias("event_type"),
            f.col("e.timestamp").alias("event_ts"),
            f.col("e.log_date").alias("log_date"),
            f.col("e.created_at").alias("created_at"),
            f.col("e.session_id").alias("session_id"),
            f.col("e.user_id").alias("user_id"),
            f.col("e.device_os").alias("device_os"),
            f.col("e.app_version").alias("app_version"),
            f.col("e.metadata").alias("metadata"),
            f.col("_ingested_at"),
        )
    )


def transform(parsed):
    """Dedup (immutable events -> dropDuplicates, no sort) + derive funnel_stage/ymd + filter."""
    return (
        parsed
        .dropDuplicates(["event_uuid"])
        .withColumn("event_time", f.to_timestamp(f.col("created_at")))
        .withColumn("funnel_stage", funnel_stage())
        .withColumn("ymd", f.date_format(f.to_date(f.col("log_date")), "yyyyMMdd"))
        .withColumn("log_date", f.to_date(f.col("log_date")))
        .withColumn("_processed_at", f.current_timestamp())
        .filter(f.col("event_uuid").isNotNull() & (f.col("event_ts") > 0))
        .select(*SILVER_COLS)
    )


### Section 2 + 3 + 4: params / run / stop (guarded so the module is import-safe for tests)
if __name__ == "__main__":
    spark = get_spark_session("Silver-Events")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 64)   # AQE coalesces down; not hardcoded-low
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

    ymd = sys.argv[1]
    ym = ymd[0:7]
    today = datetime.now().strftime("%Y-%m-%d")

    print("PARAM >>>", ymd)
    print("PARAM >>>", ym)
    print("PARAM >>>", today)
    print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

    run_date = ymd.replace("-", "")
    run_month = ym.replace("-", "")
    SILVER_PATH = get_s3_path("silver", "events")

    raw = read_delta_partitions(spark, "bronze", "behavior_events", "in_ymd", [run_date])
    if raw.isEmpty():
        print(f"[silver.events] No Bronze rows ingested on {run_date}, skipping.")
    else:
        silver_df = transform(parse_events(raw))
        silver_df.cache()
        out_count = silver_df.count()
        if out_count == 0:
            print("[silver.events] No valid rows after parse/filter, skipping.")
        else:
            print(f"[silver.events] Rows to merge: {out_count}")
            write_delta_merge(spark, silver_df, SILVER_PATH,
                              merge_keys=["event_uuid", "ymd"], partition_by=["ymd", "funnel_stage"])
            ensure_constraint(spark, SILVER_PATH, "valid_event_id", "event_id BETWEEN 1 AND 14")
            ensure_constraint(spark, SILVER_PATH, "valid_funnel_stage",
                              "funnel_stage IN ('discovery','cart','checkout')")
            register_glue_table(spark, "silver", "events", SILVER_PATH)
            print(f"[silver.events] MERGE complete for {run_date} ({out_count} rows).")

    spark.stop()
