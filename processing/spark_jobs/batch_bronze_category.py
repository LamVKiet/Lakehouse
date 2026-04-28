"""
Spark Batch: MySQL category -> S3/Delta bronze.category
NO physical partition (low-cardinality dim, ~5 rows). Append-only (T-1 day).
report_date kept as data column for Silver T-1 filter.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_mysql_incremental,
    register_glue_table, write_delta_append,
)

### spark session
spark = get_spark_session("Bronze-Category")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def load_category():
    raw = read_mysql_incremental(spark, "category", ymd)
    row_count = raw.count()
    if row_count == 0:
        print(f"[category] No rows for {ymd}, skipping.")
        return
    print(f"[category] Source rows: {row_count}")
    df = (
        raw
        .withColumn("report_date",  f.date_format(f.coalesce(f.col("updated_at"), f.col("created_at")), "yyyyMMdd"))
        .withColumn("report_month", f.date_format(f.coalesce(f.col("updated_at"), f.col("created_at")), "yyyyMM"))
        .withColumn("_loaded_at", f.current_timestamp())
        .select(
            "report_date", "report_month",
            "category_id", "category_name", "is_current",
            "created_at", "updated_at", "_loaded_at",
        )
    )
    df.show(3)
    path = get_s3_path("bronze", "category")
    write_delta_append(df, path, partition_by=None)
    register_glue_table(spark, "bronze", "category", path)
    print(f"[category] Wrote {df.count()} rows for {ymd}.")


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
load_category()

### Section 4: stop
spark.stop()
