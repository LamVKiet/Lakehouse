"""
Spark Batch: bronze.transactions -> silver.customer_activity_monthly (Daily ETL)
Registry of monthly active customers — 1 row per (customer_id, ym).
Idempotent partition overwrite on ym = run_month: scans bronze.transactions
WHERE report_month = run_month, recomputes per-customer monthly aggregate,
overwrites the current-month partition. Re-running the same day is safe.
Partition by ym (yyyyMM).

transform() is pure (df -> df, no I/O) so it is unit-testable; main() owns all I/O.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_delta_partitions, register_glue_table, write_delta_replace_partition,
)


### Section 1: transform (pure — unit-testable)
def transform(bronze_df: DataFrame, run_month: str) -> DataFrame:
    bronze_df = bronze_df.filter(f.col("customer_id").isNotNull())
    w_dedup = Window.partitionBy("transaction_id", "branch_id").orderBy(f.col("created_at").desc())
    bronze_df = (bronze_df.withColumn("_rn", f.row_number().over(w_dedup))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
    )
    return (bronze_df
        .groupBy("customer_id")
        .agg(f.min("transaction_datetime").alias("first_trans_datetime"),
            f.max("transaction_datetime").alias("last_trans_datetime"),
            f.countDistinct("transaction_id").cast("int").alias("trans_count"),)
        .withColumn("ym", f.lit(run_month))
        .withColumn("etl_datetime", f.current_timestamp())
        .select("customer_id", "ym", "first_trans_datetime", "last_trans_datetime",
                "trans_count", "etl_datetime")
    )


### Section 2: main (I/O — Spark session, read, write, register)
def main():
    spark = get_spark_session("Silver-CustomerActivityMonthly")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

    ymd = sys.argv[1]
    ym = ymd[0:7]
    today = datetime.now().strftime("%Y-%m-%d")
    print("PARAM >>>", ymd)
    print("PARAM >>>", ym)
    print("PARAM >>>", today)
    print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    run_month = ym.replace("-", "")
    ACTIVITY_PATH = get_s3_path("silver", "customer_activity_monthly")

    bronze_df = read_delta_partitions(spark, "bronze", "transactions", "in_ym", [run_month])
    row_count = bronze_df.count()
    if row_count == 0:
        print(f"[silver.customer_activity_monthly] No Bronze rows for month {run_month}, skipping.")
        spark.stop()
        return
    print(f"[silver.customer_activity_monthly] Bronze rows for {run_month}: {row_count}")

    result = transform(bronze_df, run_month)
    act_count = result.cache().count()
    print(f"[silver.customer_activity_monthly] Distinct customers in {run_month}: {act_count}")

    write_delta_replace_partition(spark, result, ACTIVITY_PATH,
        partition_col="ym", partition_value=run_month, partition_by="ym")
    register_glue_table(spark, "silver", "customer_activity_monthly", ACTIVITY_PATH)
    print(f"[silver.customer_activity_monthly] Overwrite complete for ym={run_month}.")
    spark.stop()


if __name__ == "__main__":
    main()
