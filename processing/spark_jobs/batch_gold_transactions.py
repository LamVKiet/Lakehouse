"""
Spark Batch: silver.transactions -> 2 Gold tables (T-1 day).
Triggered by Airflow daily at 03:00. Receives date as sys.argv[1] (YYYY-MM-DD).

Output:
  1. gold.daily_customer_sales — Sales dashboard (revenue + orders by customer/branch/channel/payment)
  2. gold.daily_logistics_aging — Operations dashboard (revenue + orders by aging bucket/branch/status)

STOCK statuses (still open): W, O, D, P  -> diff_date = snapshot_date - trans_date
FLOW statuses (closed)    : I, B, C, R   -> diff_date = updated_at - trans_date
F (failed payment) is excluded from both tables.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, register_glue_table, write_delta_replace_partition,
)

### spark session
spark = get_spark_session("Gold-Transactions")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

STOCK_CODES = ["W", "O", "D", "P"]
FLOW_CODES  = ["I", "B", "C", "R"]


### Section 1: functions
def prepare_input(snapshot_date: str) -> DataFrame:
    """STOCK + FLOW union with diff_date + aging_category. F is excluded."""
    silver = (
        spark.read.format("delta").load(SILVER_TRANS_PATH)
        .filter(f.col("order_status") != "F")
    )

    stock = (
        silver.filter(f.col("order_status").isin(*STOCK_CODES))
        .withColumn("diff_date", f.datediff(f.lit(snapshot_date), f.col("trans_date")))
    )
    flow = (
        silver.filter(
            (f.col("order_status").isin(*FLOW_CODES))
            & (f.to_date(f.col("updated_at")) == f.lit(snapshot_date))
        )
        .withColumn("diff_date", f.datediff(f.to_date(f.col("updated_at")), f.col("trans_date")))
    )

    union = stock.unionByName(flow)
    return (
        union
        .withColumn("aging_category",
            f.when(f.col("diff_date") < 1,   "<1")
             .when(f.col("diff_date") <= 3,  "<=3")
             .when(f.col("diff_date") <= 7,  "<=7")
             .when(f.col("diff_date") <= 14, "<=14")
             .when(f.col("diff_date") <= 28, "<=28")
             .otherwise(">28")
        )
        .withColumn("snapshot_date", f.to_date(f.lit(snapshot_date)))
        .withColumn("ymd", f.lit(run_date))
        .withColumn("ym",  f.lit(run_month))
    )


def build_customer_sales(prepared: DataFrame) -> DataFrame:
    """GroupBy grain Bảng 1, pivot 4 status groups."""
    sum_when = lambda codes: f.sum(
        f.when(f.col("order_status").isin(*codes), f.col("order_total_amount")).otherwise(0)
    )
    cnt_when = lambda codes: f.countDistinct(
        f.when(f.col("order_status").isin(*codes), f.col("trans_id"))
    )
    return (
        prepared
        .groupBy("snapshot_date", "customer_id", "branch_id", "user_type", "channel", "payment_type", "ym", "ymd")
        .agg(
            sum_when(STOCK_CODES).alias("processing_rev"),
            cnt_when(STOCK_CODES).cast("int").alias("processing_orders"),
            sum_when(["I"]).alias("recognized_rev"),
            cnt_when(["I"]).cast("int").alias("recognized_orders"),
            sum_when(["B"]).alias("returned_rev"),
            cnt_when(["B"]).cast("int").alias("returned_orders"),
            sum_when(["C", "R"]).alias("cancelled_rev"),
            cnt_when(["C", "R"]).cast("int").alias("cancelled_orders"),
        )
        .withColumn("etl_datetime", f.current_timestamp())
    )


def build_logistics_aging(prepared: DataFrame) -> DataFrame:
    """GroupBy grain Bảng 2 — no customer_id, order_status as dim."""
    return (
        prepared
        .groupBy("snapshot_date", "aging_category", "branch_id", "order_status", "channel", "payment_type", "ym", "ymd")
        .agg(
            f.sum("order_total_amount").alias("total_rev"),
            f.countDistinct("trans_id").cast("int").alias("total_orders"),
        )
        .withColumn("etl_datetime", f.current_timestamp())
    )


def transform():
    prepared = prepare_input(snapshot_date)
    row_count = prepared.count()
    if row_count == 0:
        print(f"[gold.transactions] No silver rows in scope for snapshot_date={snapshot_date}, skipping.")
        return
    print(f"[gold.transactions] Prepared rows: {row_count}")
    prepared.cache()

    sales_path = get_s3_path("gold", "daily_customer_sales")
    sales_df = build_customer_sales(prepared)
    write_delta_replace_partition(
        spark, sales_df, sales_path,
        partition_col="ymd", partition_value=run_date,
        partition_by=["branch_id", "ym", "ymd"],
    )
    register_glue_table(spark, "gold", "daily_customer_sales", sales_path)
    print(f"[gold.transactions] daily_customer_sales written ({sales_df.count()} rows).")

    aging_path = get_s3_path("gold", "daily_logistics_aging")
    aging_df = build_logistics_aging(prepared)
    write_delta_replace_partition(
        spark, aging_df, aging_path,
        partition_col="ymd", partition_value=run_date,
        partition_by=["branch_id", "ym", "ymd"],
    )
    register_glue_table(spark, "gold", "daily_logistics_aging", aging_path)
    print(f"[gold.transactions] daily_logistics_aging written ({aging_df.count()} rows).")

    prepared.unpersist()


### Section 2: params
ymd = sys.argv[1]
ym  = ymd[0:7]
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", ymd)
print("PARAM >>>", ym)
print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

snapshot_date = ymd
run_date  = ymd.replace("-", "")
run_month = ym.replace("-", "")
SILVER_TRANS_PATH = get_s3_path("silver", "transactions")


### Section 3: run
transform()

### Section 4: stop
spark.stop()
