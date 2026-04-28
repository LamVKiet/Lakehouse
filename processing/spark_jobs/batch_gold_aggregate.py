"""
Spark Batch: Silver -> Gold (T-1 day).
Triggered by Airflow daily at 03:00. Receives date as sys.argv[1].

Produces 4 Gold tables from the Silver partition:
  1. funnel_hourly          — funnel drop-off analysis by event type
  2. movie_revenue_hourly   — revenue and tickets by movie per hour
  3. app_health_hourly      — error rates by app version and event type
  4. user_activity_daily    — per-user daily activity summary
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    TICKET_PRICE,
    get_spark_session, get_s3_path, register_glue_table, write_delta_replace_partition,
)

### spark session
spark = get_spark_session("Gold-Aggregate")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def build_funnel_hourly(silver_df):
    return (
        silver_df
        .withColumn("hour", f.date_trunc("hour", f.col("event_time")))
        .groupBy("hour", "event_type", "app_version", "device_os")
        .agg(
            f.count("*").alias("event_count"),
            f.sum(f.when(f.col("is_success") == 1, 1).otherwise(0)).alias("success_count"),
            f.countDistinct("user_id").alias("unique_users"),
        )
        .withColumn("year_month", f.date_format(f.col("hour"), "yyyyMM"))
    )


def build_movie_revenue_hourly(silver_df):
    booked = silver_df.filter(
        (f.col("event_type") == "ticket_booked") & (f.col("is_success") == 1)
    )
    return (
        booked
        .withColumn("hour", f.date_trunc("hour", f.col("event_time")))
        .groupBy("hour", "movie_id", "movie_name")
        .agg(
            f.sum("seat_count").alias("tickets_sold"),
            (f.sum("seat_count") * TICKET_PRICE).alias("total_revenue"),
            f.countDistinct("user_id").alias("unique_buyers"),
        )
        .withColumn("year_month", f.date_format(f.col("hour"), "yyyyMM"))
    )


def build_app_health_hourly(silver_df):
    return (
        silver_df
        .withColumn("hour", f.date_trunc("hour", f.col("event_time")))
        .groupBy("hour", "app_version", "event_type")
        .agg(
            f.count("*").alias("total_count"),
            f.sum(f.when(f.col("is_success") == 0, 1).otherwise(0)).alias("error_count"),
        )
        .withColumn("year_month", f.date_format(f.col("hour"), "yyyyMM"))
    )


def build_user_activity_daily(silver_df):
    return (
        silver_df
        .groupBy("log_date", "user_id")
        .agg(
            f.sum(f.when(f.col("event_type") == "app_open", 1).otherwise(0)).cast("int").alias("session_count"),
            f.count("*").cast("int").alias("total_events"),
            f.sum(f.when(f.col("event_type") == "ticket_booked", 1).otherwise(0)).cast("int").alias("booked_count"),
            (f.sum(f.when(f.col("event_type") == "ticket_booked", f.col("seat_count")).otherwise(0)) * TICKET_PRICE).alias("total_spent"),
            f.max("device_os").alias("preferred_device"),
            f.max("app_version").alias("last_app_version"),
        )
    )


def transform():
    silver_path = get_s3_path("silver", "events")
    try:
        silver_df = (
            spark.read.format("delta").load(silver_path)
            .filter(f.col("log_date") == ymd)
        )
    except Exception as e:
        print(f"[gold] Could not read Silver data: {e}")
        return

    row_count = silver_df.count()
    if row_count == 0:
        print(f"[gold] No Silver rows for {ymd}, skipping.")
        return
    print(f"[gold] Silver rows: {row_count}")
    silver_df.cache()

    funnel_path = get_s3_path("gold", "funnel_hourly")
    write_delta_replace_partition(spark, build_funnel_hourly(silver_df), funnel_path, partition_col="year_month", partition_value=run_month)
    register_glue_table(spark, "gold", "funnel_hourly", funnel_path)
    print("[gold] funnel_hourly written")

    revenue_path = get_s3_path("gold", "movie_revenue_hourly")
    write_delta_replace_partition(spark, build_movie_revenue_hourly(silver_df), revenue_path, partition_col="year_month", partition_value=run_month)
    register_glue_table(spark, "gold", "movie_revenue_hourly", revenue_path)
    print("[gold] movie_revenue_hourly written")

    health_path = get_s3_path("gold", "app_health_hourly")
    write_delta_replace_partition(spark, build_app_health_hourly(silver_df), health_path, partition_col="year_month", partition_value=run_month)
    register_glue_table(spark, "gold", "app_health_hourly", health_path)
    print("[gold] app_health_hourly written")

    activity_path = get_s3_path("gold", "user_activity_daily")
    write_delta_replace_partition(spark, build_user_activity_daily(silver_df), activity_path, partition_col="log_date", partition_value=ymd)
    register_glue_table(spark, "gold", "user_activity_daily", activity_path)
    print("[gold] user_activity_daily written")

    silver_df.unpersist()
    print(f"[gold] All tables complete for {ymd}.")


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
