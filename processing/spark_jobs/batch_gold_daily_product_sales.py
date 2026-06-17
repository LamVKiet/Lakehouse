"""
Spark Batch: silver.transaction_details -> gold.daily_product_sales (T-1 day).
Triggered by Airflow daily at 03:00. Receives date as sys.argv[1] (YYYY-MM-DD).

Pre-aggregates line-grain sales to product x branch x day (the 70% efficiency rule).
Answers: best-seller, product revenue ranking, category mix, promo effectiveness.

Display labels (product_display_name, category_name) are NOT stored in Silver —
they are JOINed here from the SCD1 silver.products dim at build time, so Gold always
reflects the latest product names without re-merging the million-row Silver fact.

Write strategy: replaceWhere overwrite on ymd (idempotent T-1 overwrite).
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_delta_partitions, register_glue_table, write_delta_replace_partition,
)

spark = get_spark_session("Gold-DailyProductSales")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

GOLD_COLS = [
    "ymd", "ym", "branch_id", "product_id", "product_display_name", "category_name",
    "units_sold", "gross_revenue_vnd", "order_count", "promo_units", "regular_units",
    "avg_selling_price_vnd", "etl_datetime",
]


def transform():
    lines = read_delta_partitions(spark, "silver", "transaction_details", "in_ymd", [run_date])
    row_count = lines.count()
    if row_count == 0:
        print(f"[gold.daily_product_sales] No silver lines for ymd={run_date}, skipping.")
        return
    print(f"[gold.daily_product_sales] Silver lines in scope: {row_count}")

    agg = (lines.groupBy("ymd", "ym", "branch_id", "product_id")
        .agg(f.sum("trans_qty").cast("bigint").alias("units_sold"),
            f.sum("trans_line_amount").cast("decimal(15,2)").alias("gross_revenue_vnd"),
            f.countDistinct("transaction_id").cast("bigint").alias("order_count"),
            f.sum(f.when(f.col("is_promo") == 1, f.col("trans_qty")).otherwise(0)).cast("bigint").alias("promo_units"),
            f.sum(f.when(f.col("is_promo") == 0, f.col("trans_qty")).otherwise(0)).cast("bigint").alias("regular_units"),)
        .withColumn("avg_selling_price_vnd",
            f.when(f.col("units_sold") > 0,
                (f.col("gross_revenue_vnd") / f.col("units_sold")).cast("decimal(15,2)"))
             .otherwise(f.lit(None).cast("decimal(15,2)")))
    )

    prod_df = (spark.read.format("delta").load(get_s3_path("silver", "products"))
        .select("product_id", "product_display_name", "category_name")
    )
    result = (agg.join(f.broadcast(prod_df), "product_id", "left")
        .withColumn("etl_datetime", f.current_timestamp())
        .select(*GOLD_COLS))

    write_delta_replace_partition(spark, result, GOLD_PATH,
        partition_col="ymd", partition_value=run_date,
        partition_by=["branch_id", "ym", "ymd"],
    )
    register_glue_table(spark, "gold", "daily_product_sales", GOLD_PATH)
    print(f"[gold.daily_product_sales] Written for ymd={run_date}.")


ymd = sys.argv[1]
ym  = ymd[0:7]
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", ymd)
print("PARAM >>>", ym)
print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

run_date  = ymd.replace("-", "")
run_month = ym.replace("-", "")
GOLD_PATH = get_s3_path("gold", "daily_product_sales")


transform()
spark.stop()
