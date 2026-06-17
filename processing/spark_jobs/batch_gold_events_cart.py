"""
Spark Batch: Silver -> Gold events_cart (T-1 day).
Aggregates cart events to (session, user, screen, element, product) grain.
qty_added counts only add_to_cart; qty_removed counts only remove_from_cart.

Write: replaceWhere log_date — idempotent T-1 overwrite.
"""

import os, sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_delta_partitions, register_glue_table, write_delta_replace_partition,
)

spark = get_spark_session("Gold-Events-Cart")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


def transform():
    try:
        silver_df = (
            read_delta_partitions(spark, "silver", "events", "in_ymd", [run_date])
            .filter(f.col("funnel_stage") == "cart")
            .withColumn("source_screen",    f.col("metadata.source_screen"))
            .withColumn("source_element",   f.col("metadata.source_element"))
            .withColumn("product_id",       f.col("metadata.product_id"))
            .withColumn("quantity",         f.col("metadata.quantity"))
            .withColumn("removed_quantity", f.col("metadata.removed_quantity"))
        )
    except Exception as e:
        print(f"[gold.events_cart] Could not read Silver: {e}")
        return

    row_count = silver_df.count()
    if row_count == 0:
        print(f"[gold.events_cart] No Silver rows for {ymd}, skipping.")
        return
    print(f"[gold.events_cart] Silver rows: {row_count}")

    gold_df = (
        silver_df
        .groupBy("event_type", "log_date", "session_id", "user_id",
            "source_screen", "source_element", "product_id")
        .agg(f.count("event_uuid").alias("event_count"),
            f.sum(f.when(f.col("event_type") == "add_to_cart", f.col("quantity")).otherwise(0)).alias("qty_added"),
            f.sum(f.when(f.col("event_type") == "remove_from_cart", f.col("removed_quantity")).otherwise(0)).alias("qty_removed"),)
        .withColumn("ymd", f.date_format(f.col("log_date"), "yyyyMMdd"))
    )

    gold_path = get_s3_path("gold", "events_cart")
    write_delta_replace_partition(spark, gold_df, gold_path, partition_col="ymd", partition_value=run_date)
    register_glue_table(spark, "gold", "events_cart", gold_path)
    print(f"[gold.events_cart] Written for {ymd}.")

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
