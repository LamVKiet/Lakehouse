"""
Spark Batch: Silver -> Gold events_discovery (T-1 day).
Aggregates discovery events to (session, user, screen, element, position, search, product) grain.

Write: replaceWhere log_date — idempotent T-1 overwrite.
"""

import os, sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_delta_partitions, register_glue_table, write_delta_replace_partition,
)

spark = get_spark_session("Gold-Events-Discovery")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


def transform():
    try:
        silver_df = (
            read_delta_partitions(spark, "silver", "events", "in_ymd", [run_date])
            .filter(f.col("funnel_stage") == "discovery")
            .withColumn("source_screen",  f.col("metadata.source_screen"))
            .withColumn("source_element", f.col("metadata.source_element"))
            .withColumn("position",       f.col("metadata.position"))
            .withColumn("search_keyword", f.col("metadata.search_keyword"))
            .withColumn("product_id",     f.col("metadata.product_id"))
        )
    except Exception as e:
        print(f"[gold.events_discovery] Could not read Silver: {e}")
        return

    row_count = silver_df.count()
    if row_count == 0:
        print(f"[gold.events_discovery] No Silver rows for {ymd}, skipping.")
        return
    print(f"[gold.events_discovery] Silver rows: {row_count}")

    gold_df = (
        silver_df
        .groupBy("event_type", "log_date", "session_id", "user_id",
            "source_screen", "source_element", "position",
            "search_keyword", "product_id")
        .agg(f.count("event_uuid").alias("event_count"))
        .withColumn("ymd", f.date_format(f.col("log_date"), "yyyyMMdd"))
    )

    gold_path = get_s3_path("gold", "events_discovery")
    write_delta_replace_partition(spark, gold_df, gold_path, partition_col="ymd", partition_value=run_date)
    register_glue_table(spark, "gold", "events_discovery", gold_path)
    print(f"[gold.events_discovery] Written for {ymd}.")


ymd = sys.argv[1]
ym = ymd[0:7]
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", ymd)
print("PARAM >>>", ym)
print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

run_date = ymd.replace("-", "")
run_month = ym.replace("-", "")

transform()
spark.stop()
