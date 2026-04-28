"""
Spark Batch: bronze.transactions -> silver.transactions (Daily ETL)
Enriches transaction headers with user_type (NOU/Retention/Resurrected),
NOU flag, nou_ymd. Reads silver.nou for customer first-order history.
Smart Key: trans_id = {ymd}_{customer_id}_{transaction_id}.
Partition by ym (yyyyMM). Delta MERGE (dedup by trans_id + ym).

User type classification (monthly granularity):
  1 = New Order User       — first-ever order month == current month
  2 = Retention Order User — customer also ordered in month t-1
  3 = Resurrected Order User — customer ordered before t-1 but NOT in t-1
"""

import os
import sys
from datetime import date, timedelta, datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import Window
import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, register_glue_table, write_delta_merge,
    ensure_constraint, enable_cdf, enable_deletion_vectors,
)

### spark session
spark = get_spark_session("Silver-Transactions")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def transform():
    ### step 1: read Bronze T-1 day
    bronze_path = get_s3_path("bronze", "transactions")
    bronze_df = spark.read.format("delta").load(bronze_path).filter(f.col("report_date") == run_date)
    row_count = bronze_df.count()
    if row_count == 0:
        print(f"[silver.transactions] No Bronze rows for {ymd}, skipping.")
        return
    print(f"[silver.transactions] Bronze rows: {row_count}")

    ### intra-batch dedup: bronze is append-only daily snapshots — keep the latest version
    ### (newest bronze.created_at == latest snapshot ingested)
    w_dedup = Window.partitionBy("transaction_id", "branch_id").orderBy(f.col("created_at").desc())
    bronze_df = (
        bronze_df.withColumn("_rn", f.row_number().over(w_dedup))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
    )

    ### step 2: parse time columns + Smart Key
    base_df = (
        bronze_df
        .withColumn("trans_month", f.date_format(f.col("transaction_datetime"), "yyyyMM"))
        .withColumn("trans_date",  f.to_date(f.col("transaction_datetime")))
        .withColumn("trans_time",  f.date_format(f.col("transaction_datetime"), "HH:mm:ss"))
        .withColumn("timestamp",   f.unix_timestamp(f.col("transaction_datetime")))
        .withColumn("ym",  f.date_format(f.col("transaction_datetime"), "yyyyMM"))
        .withColumn("ymd", f.date_format(f.col("transaction_datetime"), "yyyyMMdd"))
        .withColumn("trans_id", f.concat_ws("_",
            f.date_format(f.col("transaction_datetime"), "yyyyMMdd"),
            f.col("customer_id"),
            f.col("transaction_id"),
        ))
        .withColumn("order_total_amount", f.col("trans_total_amount"))
    )

    ### step 3: join silver.nou for customer first-order history
    nou_df = (
        spark.read.format("delta").load(NOU_PATH)
        .select(
            "customer_id",
            f.col("nou_ymd").alias("hist_nou_ymd"),
            f.col("ym").alias("first_order_ym"),
        )
    )

    ### step 4: prev_month customers from silver.transactions
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    prev_customers = (
        silver_df
        .filter(f.col("ym") == l1m_ym)
        .select("customer_id").distinct()
        .withColumn("in_prev_month", f.lit(True))
    )
    print(f"[silver.transactions] Prev month filter: ym == {l1m_ym}")

    ### step 5: enrich with user_type + nou_ymd
    enriched = (
        base_df
        .join(nou_df, on="customer_id", how="left")
        .join(prev_customers, on="customer_id", how="left")
        # ── user_type ──
        .withColumn("user_type",
            f.when(f.col("first_order_ym").isNull(), f.lit(1))
            .when(f.col("first_order_ym") == f.lit(run_month), f.lit(1))
            .when(f.col("in_prev_month") == True, f.lit(2))
            .when(
                (f.col("first_order_ym") < f.lit(run_month)) & f.col("in_prev_month").isNull(),
                f.lit(3),
            )
        )
        # ── nou_ymd ──
        .withColumn("nou_ymd",
            f.when(f.col("hist_nou_ymd").isNull(), f.col("ymd"))
            .otherwise(f.col("hist_nou_ymd"))
        )
        .withColumn("_is_new_customer", f.col("hist_nou_ymd").isNull())
        .drop("hist_nou_ymd", "first_order_ym", "in_prev_month")
    )

    ### step 6: NOU flag — 1 only for truly first-ever customer's earliest transaction
    w_cust = Window.partitionBy("customer_id").orderBy("timestamp")
    enriched = (
        enriched
        .withColumn("NOU",
            f.when(
                (f.col("_is_new_customer") == True) & (f.row_number().over(w_cust) == 1),
                f.lit(1),
            ).otherwise(f.lit(0))
        )
        .drop("_is_new_customer")
    )

    ### step 7: select final columns (incl. order_status + payment_type for status tracking)
    ### created_at: original order creation (preserved on MERGE update)
    ### updated_at: latest bronze snapshot's created_at (= latest version timestamp); refreshed on every MERGE update
    result = (
        enriched
        .withColumn("updated_at", f.col("created_at"))
        .withColumn("etl_datetime", f.current_timestamp())
        .select(
            "trans_month", "trans_date", "trans_time", "timestamp",
            "trans_id",
            "customer_id", "user_type", "NOU", "nou_ymd",
            "branch_id", "channel",
            "order_status", "payment_type",
            "order_total_amount",
            "created_at", "updated_at",
            "etl_datetime", "ym", "ymd",
        )
    )
    result.cache().count()
    result.show(5)
    print(f"[silver.transactions] User type distribution:")
    result.groupBy("user_type").count().show()

    ### step 8: Delta MERGE (dedup by trans_id + branch_id + ym + ymd, UPDATE on match for status mutation) + CDF + DV + constraints + Glue
    is_first_run = not _delta_exists(spark, SILVER_PATH)
    write_delta_merge(
        spark, result, SILVER_PATH,
        merge_keys=["trans_id", "branch_id", "ym", "ymd"],
        partition_by=["branch_id", "ym", "ymd"],
        update_on_match=True,
        update_exclude_cols=["created_at"],
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true",
        } if is_first_run else None,
    )
    if not is_first_run:
        enable_cdf(spark, SILVER_PATH)
        enable_deletion_vectors(spark, SILVER_PATH)
    ensure_constraint(spark, SILVER_PATH, "valid_order_status",
                      "order_status IN ('W','R','O','D','I','C','B','P','F')")
    ensure_constraint(spark, SILVER_PATH, "valid_channel", "channel IN ('offline','online')")
    ensure_constraint(spark, SILVER_PATH, "valid_user_type", "user_type IN (1,2,3)")
    register_glue_table(spark, "silver", "transactions", SILVER_PATH)
    print(f"[silver.transactions] MERGE complete for {ymd} ({result.count()} rows).")


def _delta_exists(spark, path: str) -> bool:
    from delta.tables import DeltaTable
    return DeltaTable.isDeltaTable(spark, path)


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
l1m_ym = (date(int(run_month[:4]), int(run_month[4:]), 1) - timedelta(days=1)).strftime("%Y%m")
SILVER_PATH = get_s3_path("silver", "transactions")
NOU_PATH    = get_s3_path("silver", "nou")


### Section 3: run
transform()

### Section 4: stop
spark.stop()
