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
    get_spark_session, get_s3_path, register_glue_table, write_delta_replace_partition,
)

### spark session
spark = get_spark_session("Gold-Events-Discovery")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def transform():
    silver_path = get_s3_path("silver", "events_discovery")
    try:
        silver_df = spark.read.format("delta").load(silver_path).filter(f.col("log_date") == ymd)
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
        .groupBy(
            "event_type", "log_date", "session_id", "user_id",
            "source_screen", "source_element", "position",
            "search_keyword", "product_id",
        )
        .agg(f.count("event_uuid").alias("event_count"))
    )

    gold_path = get_s3_path("gold", "events_discovery")
    write_delta_replace_partition(spark, gold_df, gold_path, partition_col="log_date", partition_value=ymd)
    register_glue_table(spark, "gold", "events_discovery", gold_path)
    print(f"[gold.events_discovery] Written for {ymd}.")


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
