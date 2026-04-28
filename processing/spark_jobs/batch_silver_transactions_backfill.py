"""
Spark Batch: bronze.transactions -> silver.transactions (Backfill — run once)
Reads ALL Bronze transactions, joins silver.nou for customer history,
computes user_type/NOU/nou_ymd using self-join for prev_month detection.
Requires: silver.nou backfill must run first.
No date parameter needed.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import Window
import pyspark.sql.functions as f
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, register_glue_table,
)

### spark session
spark = get_spark_session("Silver-Transactions-Backfill")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.caseSensitive", "false")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


### Section 1: functions
def backfill():
    ### step 1: read ALL Bronze transactions
    bronze_path = get_s3_path("bronze", "transactions")
    bronze_df = spark.read.format("delta").load(bronze_path)
    row_count = bronze_df.count()
    if row_count == 0:
        print("[silver.transactions backfill] Bronze empty, skipping.")
        return
    print(f"[silver.transactions backfill] Bronze rows: {row_count}")

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

    ### step 3: join silver.nou for first-order history
    nou_df = (
        spark.read.format("delta").load(NOU_PATH)
        .select(
            "customer_id",
            f.col("nou_ymd").alias("hist_nou_ymd"),
            f.col("ym").alias("first_order_ym"),
        )
    )
    enriched = base_df.join(nou_df, on="customer_id", how="left")

    ### step 4: build prev_month flags via self-join
    # For each (customer, ym), create a flag saying "this customer ordered in the prev month of next_ym"
    customer_months = base_df.select("customer_id", "ym").distinct()
    prev_month_flags = (
        customer_months
        .withColumn("next_ym",
            f.date_format(
                f.add_months(f.to_date(f.concat(f.col("ym"), f.lit("01")), "yyyyMMdd"), 1),
                "yyyyMM",
            )
        )
        .select(
            f.col("customer_id"),
            f.col("next_ym").alias("ym"),
            f.lit(True).alias("in_prev_month"),
        )
    )
    enriched = enriched.join(prev_month_flags, on=["customer_id", "ym"], how="left")

    ### step 5: user_type + NOU flag + nou_ymd
    w_cust = Window.partitionBy("customer_id").orderBy("timestamp")
    enriched = (
        enriched
        .withColumn("user_type",
            f.when(f.col("first_order_ym") == f.col("ym"), f.lit(1))
            .when(f.col("in_prev_month") == True, f.lit(2))
            .when(
                (f.col("first_order_ym") < f.col("ym")) & f.col("in_prev_month").isNull(),
                f.lit(3),
            )
        )
        .withColumn("nou_ymd", f.col("hist_nou_ymd"))
        .withColumn("NOU",
            f.when(
                (f.col("first_order_ym") == f.col("ym")) & (f.row_number().over(w_cust) == 1),
                f.lit(1),
            ).otherwise(f.lit(0))
        )
        .drop("hist_nou_ymd", "first_order_ym", "in_prev_month")
    )

    ### step 6: select final columns
    result = (
        enriched
        .withColumn("etl_datetime", f.current_timestamp())
        .select(
            "trans_month", "trans_date", "trans_time", "timestamp",
            "trans_id",
            "customer_id", "user_type", "NOU", "nou_ymd",
            "branch_id", "channel",
            "order_total_amount",
            "etl_datetime", "ym", "ymd",
        )
    )
    result.cache().count()
    result.show(5)
    print("[silver.transactions backfill] User type distribution:")
    result.groupBy("user_type").count().show()

    ### step 7: overwrite Silver
    result.write.format("delta").mode("overwrite").partitionBy("ym").save(SILVER_PATH)
    register_glue_table(spark, "silver", "transactions", SILVER_PATH)
    print(f"[silver.transactions backfill] Complete: {result.count()} rows.")


### Section 2: params
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

SILVER_PATH = get_s3_path("silver", "transactions")
NOU_PATH    = get_s3_path("silver", "nou")


### Section 3: run
backfill()

### Section 4: stop
spark.stop()
