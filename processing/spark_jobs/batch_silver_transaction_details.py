"""
Spark Batch (CDC source): bronze.pos_transaction_details + bronze.online_transaction_details
(Debezium raw envelope) -> silver.transaction_details. Daily incremental ETL.

Atomic LINE grain: 1 row per transaction_detail_id (NO pre-aggregation — collapse to
order/product totals happens at Gold). This is the fact-at-lowest-grain table.

Logic (CDC-correct, mirrors batch_silver_transactions_cdc.py):
  1. Read bronze CDC detail tables WHERE report_date == run_date (INGESTION-date partition).
  2. Parse Debezium envelope with an explicit schema (no infer).
  3. Drop deletes defensively: keep op IN ('c','u','r').
  4. Union pos (offline) + online; add `channel`.
  5. Intra-batch dedup: keep latest per transaction_detail_id by (source.ts_ms desc, _kafka_offset desc).
  6. Denormalize ONLY keys + numeric from silver.products (category_id, unit_price) — display
     labels (product_display_name, category_name) are deferred to Gold (cheaper + always-fresh
     against the SCD1 dim). DAG: silver_products MUST run before this job.
  7. Smart Key line_id = {ymd}_{transaction_id}_{transaction_detail_id}, ymd from created_at
     (line creation ~ order date; details carry no transaction_datetime).
  8. MERGE with "newer-wins" guard (s.source_ts_ms > t.source_ts_ms) + dynamic ymd pruning.

Connector assumptions (see bronze-layer.md): decimal.handling.mode=string,
time.precision.mode=connect -> DATETIME = epoch millis (int64), decimals = string.
is_promo is TINYINT(1) -> Debezium may emit boolean OR 0/1; parsed as string then normalized.

parse_cdc() + transform() are pure (df -> df, no I/O) so they are unit-testable; main() owns all I/O.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from delta.tables import DeltaTable
from processing.spark_jobs.delta_utils import (
    get_spark_session, get_s3_path, read_delta_partitions, register_glue_table,
    ensure_constraint, enable_cdf, enable_deletion_vectors,
)

AFTER_SCHEMA = StructType([
    StructField("transaction_detail_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("branch_id", StringType()),
    StructField("product_id", StringType()),
    StructField("is_promo", StringType()),                # TINYINT(1): boolean or 0/1
    StructField("product_uom_code", StringType()),
    StructField("trans_qty", IntegerType()),
    StructField("trans_line_amount", StringType()),       # decimal.handling.mode=string
    StructField("variant_size", StringType()),
    StructField("variant_color", StringType()),
    StructField("created_at", LongType()),                # epoch millis
])
ENVELOPE_SCHEMA = StructType([
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("after", AFTER_SCHEMA),
    StructField("source", StructType([StructField("ts_ms", LongType())])),
])

SILVER_COLS = [
    "line_id", "transaction_detail_id", "transaction_id", "branch_id", "channel",
    "product_id", "category_id",
    "variant_size", "variant_color", "is_promo", "product_uom_code",
    "trans_qty", "trans_line_amount", "unit_price",
    "created_at", "source_ts_ms", "etl_datetime", "ym", "ymd",
]


### Section 1: transform (pure — unit-testable)
def parse_cdc(raw: DataFrame, channel: str) -> DataFrame:
    """Parse a CDC Bronze detail table's Debezium envelope (no I/O — operates on a read df)."""
    return (
        raw
        .withColumn("env", f.from_json(f.col("payload"), ENVELOPE_SCHEMA))
        .filter(f.col("env.op").isin("c", "u", "r"))      # defensive: drop deletes
        .filter(f.col("env.after.transaction_detail_id").isNotNull())
        .select(
            f.col("env.after.transaction_detail_id").alias("transaction_detail_id"),
            f.col("env.after.transaction_id").alias("transaction_id"),
            f.col("env.after.branch_id").alias("branch_id"),
            f.col("env.after.product_id").alias("product_id"),
            f.col("env.after.is_promo").alias("is_promo"),
            f.col("env.after.product_uom_code").alias("product_uom_code"),
            f.col("env.after.trans_qty").alias("trans_qty"),
            f.col("env.after.trans_line_amount").alias("trans_line_amount"),
            f.col("env.after.variant_size").alias("variant_size"),
            f.col("env.after.variant_color").alias("variant_color"),
            f.col("env.after.created_at").alias("created_at"),
            f.col("env.source.ts_ms").alias("source_ts_ms"),
            f.col("_kafka_offset"),
        )
        .withColumn("channel", f.lit(channel))
    )


def transform(pos: DataFrame, onl: DataFrame, prod_df: DataFrame) -> DataFrame:
    unioned = pos.unionByName(onl)
    w = Window.partitionBy("transaction_detail_id").orderBy(
        f.col("source_ts_ms").desc(), f.col("_kafka_offset").desc())
    deduped = (
        unioned.withColumn("_rn", f.row_number().over(w))
        .filter(f.col("_rn") == 1)
        .drop("_rn", "_kafka_offset")
    )
    typed = (
        deduped
        .withColumn("created_at", f.expr("timestamp_millis(created_at)"))
        .withColumn("trans_line_amount", f.col("trans_line_amount").cast("decimal(15,2)"))
        .withColumn("is_promo",
            f.when(f.lower(f.col("is_promo")).isin("1", "true", "t"), f.lit(1)).otherwise(f.lit(0)))
        .withColumn("ym", f.date_format("created_at", "yyyyMM"))
        .withColumn("ymd", f.date_format("created_at", "yyyyMMdd"))
        .withColumn("line_id", f.concat_ws("_",
            f.date_format("created_at", "yyyyMMdd"),
            f.col("transaction_id"), f.col("transaction_detail_id")))
    )
    return (
        typed.join(f.broadcast(prod_df), "product_id", "left")
        .withColumn("etl_datetime", f.current_timestamp())
        .select(*SILVER_COLS)
    )


### Section 2: main (I/O — Spark session, read, write, register)
def main():
    spark = get_spark_session("Silver-TransactionDetails-CDC")
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
    run_date = ymd.replace("-", "")
    SILVER_PATH = get_s3_path("silver", "transaction_details")

    pos = parse_cdc(
        read_delta_partitions(spark, "bronze", "pos_transaction_details", "in_ymd", [run_date]), "offline")
    onl = parse_cdc(
        read_delta_partitions(spark, "bronze", "online_transaction_details", "in_ymd", [run_date]), "online")
    prod_df = (
        spark.read.format("delta").load(get_s3_path("silver", "products"))
        .select("product_id", "category_id", "unit_price")
    )
    result = transform(pos, onl, prod_df).cache()
    out_count = result.count()
    if out_count == 0:
        print(f"[silver.transaction_details] No CDC rows ingested on {ymd}, skipping.")
        spark.stop()
        return
    print(f"[silver.transaction_details] Rows to merge (deduped lines): {out_count}")

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        (result.write.format("delta").mode("overwrite")
            .partitionBy("branch_id", "ym", "ymd")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.enableDeletionVectors", "true")
            .save(SILVER_PATH))
    else:
        ymds = [r["ymd"] for r in result.select("ymd").distinct().collect()]
        ymd_pred = ",".join(f"'{x}'" for x in ymds)
        cond = (
            "t.line_id = s.line_id AND t.branch_id = s.branch_id "
            f"AND t.ym = s.ym AND t.ymd = s.ymd AND t.ymd IN ({ymd_pred})"
        )
        update_set = {c: f"s.{c}" for c in SILVER_COLS if c != "created_at"}
        (DeltaTable.forPath(spark, SILVER_PATH).alias("t")
            .merge(result.alias("s"), cond)
            .whenMatchedUpdate(condition="s.source_ts_ms > t.source_ts_ms", set=update_set)
            .whenNotMatchedInsertAll()
            .execute())
        enable_cdf(spark, SILVER_PATH)
        enable_deletion_vectors(spark, SILVER_PATH)

    ensure_constraint(spark, SILVER_PATH, "valid_is_promo", "is_promo IN (0,1)")
    ensure_constraint(spark, SILVER_PATH, "valid_channel", "channel IN ('offline','online')")
    ensure_constraint(spark, SILVER_PATH, "valid_trans_qty", "trans_qty >= 0")
    register_glue_table(spark, "silver", "transaction_details", SILVER_PATH)
    print(f"[silver.transaction_details] MERGE complete for ingestion window {ymd} ({out_count} lines).")
    spark.stop()


if __name__ == "__main__":
    main()
