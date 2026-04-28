"""
Spark Batch: MySQL products -> S3/Delta bronze.products
Partition by report_date (yyyyMMdd). Append-only (T-1 day).
Bronze = raw landing zone, no MERGE/SCD2. Downstream Silver handles dedup.
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
spark = get_spark_session("Bronze-Products")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def load_products():
    raw = read_mysql_incremental(spark, "products", ymd)
    row_count = raw.count()
    if row_count == 0:
        print(f"[products] No rows for {ymd}, skipping.")
        return
    print(f"[products] Source rows: {row_count}")
    df = (
        raw
        .withColumn("report_date",  f.date_format(f.coalesce(f.col("updated_at"), f.col("created_at")), "yyyyMMdd"))
        .withColumn("report_month", f.date_format(f.coalesce(f.col("updated_at"), f.col("created_at")), "yyyyMM"))
        .withColumn("_loaded_at", f.current_timestamp())
        .select(
            "report_date", "report_month",
            "product_id", "product_name", "product_display_name",
            "category_id", "sales_unit", "color", "size", "unit_price",
            "created_at", "updated_at", "is_current", "_loaded_at",
        )
    )
    df.show(3)
    path = get_s3_path("bronze", "products")
    write_delta_append(df, path, partition_by="report_date")
    register_glue_table(spark, "bronze", "products", path)
    print(f"[products] Wrote {df.count()} rows for {ymd}.")


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
load_products()

### Section 4: stop
spark.stop()
