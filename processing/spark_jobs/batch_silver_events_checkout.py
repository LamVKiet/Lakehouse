"""
Spark Batch: Bronze -> Silver events_checkout (T-1 day).
Filter event_id IN (9,10,11,12,13,14): begin_checkout, add_shipping_info, add_coupon,
add_payment_info, place_order, payment_callback.
Serves PO Payment / Risk + Dev Backend — checkout drop-off, coupon validity, payment errors.

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
spark = get_spark_session("Silver-Events-Checkout")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

CHECKOUT_EVENT_IDS = [9, 10, 11, 12, 13, 14]


### Section 1: functions
def transform():
    bronze_path = get_s3_path("bronze", "behavior_events")
    try:
        bronze_df = (
            spark.read.format("delta").load(bronze_path)
            .filter(f.col("log_date") == ymd)
            .filter(f.col("event_id").isin(CHECKOUT_EVENT_IDS))
        )
    except Exception as e:
        print(f"[silver.events_checkout] Could not read Bronze: {e}")
        return

    row_count = bronze_df.count()
    if row_count == 0:
        print(f"[silver.events_checkout] No Bronze rows for {ymd}, skipping.")
        return
    print(f"[silver.events_checkout] Bronze rows: {row_count}")

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
        .withColumn("items_list", f.col("meta.items_list"))
        .withColumn("shipping_method", f.col("meta.shipping_method"))
        .withColumn("shipping_fee", f.col("meta.shipping_fee"))
        .withColumn("city_province", f.col("meta.city_province"))
        .withColumn("promotion_id", f.col("meta.promotion_id"))
        .withColumn("promotion_type", f.col("meta.promotion_type"))
        .withColumn("discount_amount", f.col("meta.discount_amount"))
        .withColumn("is_valid", f.col("meta.is_valid"))
        .withColumn("error_message", f.col("meta.error_message"))
        .withColumn("payment_method", f.col("meta.payment_method"))
        .withColumn("final_amount", f.col("meta.final_amount"))
        .withColumn("order_id", f.col("meta.order_id"))
        .withColumn("transaction_id", f.col("meta.transaction_id"))
        .withColumn("is_success", f.col("meta.is_success"))
        .withColumn("error_code", f.col("meta.error_code"))
        .withColumn("_processed_at", f.current_timestamp())
        .filter(f.col("event_uuid").isNotNull() & (f.col("event_ts") > 0))
        .select(
            "event_uuid", "trans_event_id", "event_id", "event_type", "event_time", "log_date",
            "session_id", "user_id", "device_os", "app_version",
            "items_list",
            "shipping_method", "shipping_fee", "city_province",
            "promotion_id", "promotion_type", "discount_amount", "is_valid", "error_message",
            "payment_method", "final_amount",
            "order_id", "transaction_id",
            "is_success", "error_code",
            "_processed_at",
        )
    )

    out_count = silver_df.count()
    if out_count == 0:
        print(f"[silver.events_checkout] No valid rows after filtering, skipping.")
        return
    print(f"[silver.events_checkout] Silver rows to write: {out_count}")

    silver_path = get_s3_path("silver", "events_checkout")
    write_delta_merge(spark, silver_df, silver_path, merge_keys=["event_uuid", "log_date"], partition_by="log_date")
    ensure_constraint(spark, silver_path, "valid_event_id_checkout", "event_id IN (9,10,11,12,13,14)")
    register_glue_table(spark, "silver", "events_checkout", silver_path)
    print(f"[silver.events_checkout] MERGE complete for {ymd}.")


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
