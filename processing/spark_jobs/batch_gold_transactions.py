"""
Spark Batch: silver.transactions -> 2 Gold tables (T-1 day).
Triggered by Airflow daily at 03:00. Receives date as sys.argv[1] (YYYY-MM-DD).

Output:
  1. gold.daily_customer_sales — Sales dashboard (revenue + orders by customer/branch/channel/payment)
                                 Retention computed on-the-fly via JOIN with silver.customer_activity_monthly.
  2. gold.daily_logistics_aging — Operations dashboard (revenue + orders by aging bucket/branch/status)

STOCK statuses (still open): W, O, D, P  -> diff_date = snapshot_date - trans_date
FLOW statuses (closed)    : I, B, C, R   -> diff_date = updated_at - trans_date
F (failed payment) is excluded from both tables.

Retention buckets (only on daily_customer_sales):
  retention_lag = 0  -> NOU         (first-ever active month)
  retention_lag = 1  -> Retention   (also active in t-1)
  retention_lag = 2..6 -> Resurrected at t-N
  retention_lag = 99 -> Resurrected (last active > 6 months ago)
user_type derived: 1=NOU, 2=Retention, 3=Resurrected (lag>=2 or 99).
"""

import os
import sys
from datetime import date, timedelta, datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_delta_partitions, register_glue_table, write_delta_replace_partition,
)

### spark session
spark = get_spark_session("Gold-Transactions")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

STOCK_CODES = ["W", "O", "D", "P"]
FLOW_CODES  = ["I", "B", "C", "R"]
RETENTION_WINDOW = 6   # months t-1 .. t-6


### Section 1: functions
def _lag_ym(yyyymm: str, n: int) -> str:
    """Return yyyyMM offset by n months back (pure Python, no Spark)."""
    d = date(int(yyyymm[:4]), int(yyyymm[4:]), 1)
    for _ in range(n):
        d = (d - timedelta(days=1)).replace(day=1)
    return d.strftime("%Y%m")


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


def enrich_retention(prepared: DataFrame) -> DataFrame:
    """JOIN silver.customer_activity_monthly to compute retention_lag + user_type on-the-fly."""
    recent_yms = [_lag_ym(run_month, n) for n in range(1, RETENTION_WINDOW + 1)]   # t-1 .. t-6
    print(f"[gold.transactions] Retention window yms: {recent_yms}")

    activity_df = (
        read_delta_partitions(spark, "silver", "customer_activity_monthly", "in_ym", recent_yms)
        .select("customer_id", "ym")
    )
    # for each customer → MIN(lag) across active months in t-1..t-6
    lag_pairs = []
    for idx, ym_val in enumerate(recent_yms):
        lag_pairs.extend([f.lit(ym_val), f.lit(idx + 1)])
    lag_map = f.create_map(*lag_pairs)
    activity_lag = (
        activity_df
        .withColumn("lag", lag_map[f.col("ym")])
        .groupBy("customer_id").agg(f.min("lag").alias("min_lag"))
    )

    # ever active before run_month → distinguishes NOU (never before) vs Resurrected > 6 months
    ever_active = (
        spark.read.format("delta").load(ACTIVITY_PATH)
        .filter(f.col("ym") < f.lit(run_month))
        .select("customer_id").distinct()
        .withColumn("ever_before", f.lit(True))
    )

    return (
        prepared
        .join(f.broadcast(activity_lag), on="customer_id", how="left")
        .join(f.broadcast(ever_active), on="customer_id", how="left")
        .withColumn("retention_lag",
            f.when(f.col("ever_before").isNull(), f.lit(0))             # NOU
            .when(f.col("min_lag").isNotNull(), f.col("min_lag"))       # active t-1..t-6
            .otherwise(f.lit(99))                                        # active before but > 6 months
        )
        .withColumn("user_type",
            f.when(f.col("retention_lag") == 0, f.lit(1))
            .when(f.col("retention_lag") == 1, f.lit(2))
            .otherwise(f.lit(3))
        )
        .drop("min_lag", "ever_before")
    )


def build_customer_sales(prepared: DataFrame) -> DataFrame:
    """GroupBy grain Bảng 1, pivot 4 status groups. retention_lag/user_type already enriched."""
    sum_when = lambda codes: f.sum(f.when(f.col("order_status").isin(*codes), f.col("order_total_amount")).otherwise(0))
    cnt_when = lambda codes: f.countDistinct(f.when(f.col("order_status").isin(*codes), f.col("trans_id")))
    return (prepared
        .groupBy("snapshot_date", "customer_id", "branch_id",
                 "user_type", "retention_lag",
                 "channel", "payment_type", "ym", "ymd")
        .agg(sum_when(STOCK_CODES).alias("processing_rev"),
            cnt_when(STOCK_CODES).cast("int").alias("processing_orders"),
            sum_when(["I"]).alias("recognized_rev"),
            cnt_when(["I"]).cast("int").alias("recognized_orders"),
            sum_when(["B"]).alias("returned_rev"),
            cnt_when(["B"]).cast("int").alias("returned_orders"),
            sum_when(["C", "R"]).alias("cancelled_rev"),
            cnt_when(["C", "R"]).cast("int").alias("cancelled_orders"),)
        .withColumn("etl_datetime", f.current_timestamp())
    )


def build_logistics_aging(prepared: DataFrame) -> DataFrame:
    """GroupBy grain Bảng 2 — no customer_id, order_status as dim. No retention."""
    return (prepared
        .groupBy("snapshot_date", "aging_category", "branch_id", "order_status",
                 "channel", "payment_type", "ym", "ymd")
        .agg(f.sum("order_total_amount").alias("total_rev"),
            f.countDistinct("trans_id").cast("int").alias("total_orders"),)
        .withColumn("etl_datetime", f.current_timestamp())
    )


def transform():
    prepared = prepare_input(snapshot_date).cache()
    row_count = prepared.count()
    if row_count == 0:
        print(f"[gold.transactions] No silver rows in scope for snapshot_date={snapshot_date}, skipping.")
        return
    print(f"[gold.transactions] Prepared rows: {row_count}")
    prepared_with_retention = enrich_retention(prepared)
    prepared_with_retention.cache()

    sales_path = get_s3_path("gold", "daily_customer_sales")
    sales_df = build_customer_sales(prepared_with_retention)
    write_delta_replace_partition(
        spark, sales_df, sales_path,
        partition_col="ymd", partition_value=run_date,
        partition_by=["branch_id", "ym", "ymd"],
    )
    register_glue_table(spark, "gold", "daily_customer_sales", sales_path)
    print(f"[gold.transactions] daily_customer_sales written.")

    aging_path = get_s3_path("gold", "daily_logistics_aging")
    aging_df = build_logistics_aging(prepared)
    write_delta_replace_partition(
        spark, aging_df, aging_path,
        partition_col="ymd", partition_value=run_date,
        partition_by=["branch_id", "ym", "ymd"],
    )
    register_glue_table(spark, "gold", "daily_logistics_aging", aging_path)
    print(f"[gold.transactions] daily_logistics_aging written.")

    prepared_with_retention.unpersist()
    prepared.unpersist()


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
ACTIVITY_PATH     = get_s3_path("silver", "customer_activity_monthly")

transform()
spark.stop()
