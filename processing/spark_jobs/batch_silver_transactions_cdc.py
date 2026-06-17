"""
Spark Batch (CDC source): bronze.pos_transactions + bronze.online_transactions
(Debezium raw envelope) -> silver.transactions. Daily incremental ETL.

Canonical silver.transactions job (CDC). Wired in dag_bronze_to_silver as the daily
task; mirrors batch_silver_transaction_details.py (same CDC source path).

Logic (CDC-correct):
  1. Read bronze CDC tables WHERE report_date == run_date (INGESTION-date partition —
     this is the Airflow incremental window, NOT the business date).
  2. Parse Debezium envelope with an explicit schema (no infer).
  3. Drop deletes defensively: keep op IN ('c','u','r'). Cancellations are status
     changes, never hard deletes — op='d' would be a dev error.
  4. Union pos (offline) + online; add `channel`.
  5. Intra-batch dedup: one window can carry several events for the same order
     (W->O->I). Keep the latest by (source.ts_ms desc, _kafka_offset desc).
  6. Partition / Smart Key by transaction_datetime (IMMUTABLE business date) so an
     order never changes partition. `updated_at` = payload updated_at (business
     "last changed"); `source_ts_ms` = binlog commit time (version comparator).
  7. MERGE with "newer-wins" guard (s.source_ts_ms > t.source_ts_ms) + dynamic ymd
     pruning so re-runs / out-of-order events can't regress a row, and the MERGE
     prunes target partitions. created_at preserved on update.

Connector assumptions (see bronze-layer.md): decimal.handling.mode=string,
time.precision.mode=connect -> DATETIME = epoch millis (int64), decimals = string.
If you change time.precision.mode, adjust the timestamp conversions below.

parse_cdc() + transform() are pure (df -> df, no I/O) so they are unit-testable; main() owns all I/O.
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType
from processing.spark_jobs.delta_utils import (
    read_delta_partitions, register_glue_table, get_s3_path,
    ensure_constraint, enable_cdf, enable_deletion_vectors,
)
from processing.dq.config import Paths, build_spark, shutdown_spark
from processing.dq.wap import WAPGate, quarantine_failed_rows
from processing.dq.exceptions import DataQualityError
from processing.dq.status import summarize_failures, write_summary
from processing.dq.checks import (
    silver_transactions_check, silver_transactions_soft_check,
    TRANSACTION_ROW_RULES, TRANSACTION_GRAIN,
)
from processing.dq.reconcile import header_detail_amount_mismatches, referential_orphans
from processing.dq.anomaly import add_anomaly_checks

PIPELINE = "silver_transactions"
PATHS = Paths(table="transactions", layer="silver")
MERGE_KEYS = ["trans_id", "branch_id", "ym", "ymd"]
EXIT_DQ_FAIL = 42   # DATA fail (Deequ gate) -> Airflow map AirflowFailException (no retry)

AFTER_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("branch_id", StringType()),
    StructField("transaction_datetime", LongType()),     # connect Timestamp = epoch millis
    StructField("payment_type", StringType()),
    StructField("trans_total_amount", StringType()),      # decimal.handling.mode=string
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("created_at", LongType()),                # epoch millis
    StructField("updated_at", LongType()),                # epoch millis
])
ENVELOPE_SCHEMA = StructType([
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("after", AFTER_SCHEMA),
    StructField("source", StructType([StructField("ts_ms", LongType())])),
])

SILVER_COLS = [
    "trans_month", "trans_date", "trans_time", "timestamp",
    "trans_id", "customer_id", "branch_id", "channel",
    "order_status", "payment_type", "order_total_amount",
    "created_at", "updated_at", "source_ts_ms",
    "etl_datetime", "ym", "ymd",
]


### Section 1: transform (pure — unit-testable)
def parse_cdc(raw: DataFrame, channel: str) -> DataFrame:
    """Parse a CDC Bronze transactions table's Debezium envelope (no I/O — operates on a read df)."""
    return (
        raw
        .withColumn("env", f.from_json(f.col("payload"), ENVELOPE_SCHEMA))
        .filter(f.col("env.op").isin("c", "u", "r"))      # defensive: drop deletes (dev error)
        .filter(f.col("env.after.transaction_id").isNotNull())
        .select(
            f.col("env.after.transaction_id").alias("transaction_id"),
            f.col("env.after.branch_id").alias("branch_id"),
            f.col("env.after.transaction_datetime").alias("transaction_datetime"),
            f.col("env.after.payment_type").alias("payment_type"),
            f.col("env.after.trans_total_amount").alias("trans_total_amount"),
            f.col("env.after.customer_id").alias("customer_id"),
            f.col("env.after.order_status").alias("order_status"),
            f.col("env.after.created_at").alias("created_at"),
            f.col("env.after.updated_at").alias("updated_at"),
            f.col("env.source.ts_ms").alias("source_ts_ms"),
            f.col("_kafka_offset"),
        )
        .withColumn("channel", f.lit(channel))
    )


def transform(pos: DataFrame, onl: DataFrame) -> DataFrame:
    unioned = pos.unionByName(onl)
    w = Window.partitionBy("transaction_id", "branch_id").orderBy(
        f.col("source_ts_ms").desc(), f.col("_kafka_offset").desc())
    deduped = (
        unioned.withColumn("_rn", f.row_number().over(w))
        .filter(f.col("_rn") == 1)
        .drop("_rn", "_kafka_offset")
    )
    return (
        deduped
        .withColumn("transaction_datetime", f.expr("timestamp_millis(transaction_datetime)"))
        .withColumn("created_at", f.expr("timestamp_millis(created_at)"))
        .withColumn("updated_at", f.expr("timestamp_millis(updated_at)"))
        .withColumn("order_total_amount", f.col("trans_total_amount").cast("decimal(15,2)"))
        .withColumn("trans_month", f.date_format("transaction_datetime", "yyyyMM"))
        .withColumn("trans_date", f.to_date("transaction_datetime"))
        .withColumn("trans_time", f.date_format("transaction_datetime", "HH:mm:ss"))
        .withColumn("timestamp", f.unix_timestamp("transaction_datetime"))
        .withColumn("ym", f.date_format("transaction_datetime", "yyyyMM"))
        .withColumn("ymd", f.date_format("transaction_datetime", "yyyyMMdd"))
        .withColumn("trans_id", f.concat_ws("_",
            f.date_format("transaction_datetime", "yyyyMMdd"),
            f.col("customer_id"), f.col("transaction_id")))
        .withColumn("etl_datetime", f.current_timestamp())
        .select(*SILVER_COLS)
    )


### Section 2: WAP phases (I/O) — Write -> Audit -> Publish, one process/task per phase
def _new_spark():
    spark = build_spark("Silver-Transactions-CDC")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    return spark


def phase_write(spark, gate: WAPGate, run_date: str, ymd: str) -> int:
    """W: read Bronze CDC -> transform/dedup -> write staging (isolated by run_date)."""
    pos = parse_cdc(
        read_delta_partitions(spark, "bronze", "pos_transactions", "in_ymd", [run_date]), "offline")
    onl = parse_cdc(
        read_delta_partitions(spark, "bronze", "online_transactions", "in_ymd", [run_date]), "online")
    result = transform(pos, onl)
    gate.write_staging(result)   # write even when empty -> creates the partition, audit/publish no-op
    n = gate.read_staging().count()
    print(f"[silver.transactions][write] Staged {n} deduped orders for ingestion window {ymd}.")
    return n


def phase_audit(spark, gate: WAPGate) -> None:
    """A: Deequ suite on staging (hard + soft + anomaly). Error -> quarantine + exit(EXIT_DQ_FAIL)
    (no retry). Anomaly = Warning (metric vs history in the repository) -> publish still runs,
    only log + alert; on cold start without enough baseline it passes naturally."""
    if gate.read_staging().rdd.isEmpty():
        print("[silver.transactions][audit] Empty staging batch, skipping checks.")
        return
    try:
        outcome = gate.audit(checks=[silver_transactions_check(spark),
                                     silver_transactions_soft_check(spark)],
                             with_anomaly=lambda s: add_anomaly_checks(
                                 s, mean_col="order_total_amount", sum_col="order_total_amount"),
                             row_rules=TRANSACTION_ROW_RULES, grain=TRANSACTION_GRAIN)
        print(f"[silver.transactions][audit] status={outcome.status} failed={outcome.failed}")
        if outcome.status == "Warning":
            # soft check / anomaly trip -> does not block publish, but surface it for oncall to inspect history
            print(f"[silver.transactions][audit] WARN signals (anomaly/soft) -> {outcome.failed}")
    except DataQualityError as e:
        # Summary sentinel: recompute bad rows (failure path, rare) -> compact JSON for the Airflow mail.
        staged = gate.read_staging()
        bad = quarantine_failed_rows(staged, TRANSACTION_ROW_RULES, TRANSACTION_GRAIN)
        summary = summarize_failures(staged, bad, phase="audit", run_date=gate.run_date,
                                     failed=e.failed, grain_col="trans_id")
        write_summary(spark, f"{gate.paths.status}{gate.run_date}.json", summary)
        # Stable marker for the DAG: spark-submit may normalize the exit code to 1, so the
        # audit task keys the no-retry decision on this line OR the exit code.
        print("DQ_GATE_FAILED")
        print(f"[silver.transactions][audit] DQ GATE FAILED -> quarantined. summary={summary['reason_counts']} {e}")
        shutdown_spark(spark)
        sys.exit(EXIT_DQ_FAIL)


def phase_reconcile(spark, gate: WAPGate) -> None:
    """R: cross-table Accuracy on the STAGED batch (after audit, before publish). Deequ checks one
    table at a time, so the two cross-table money rules live here: header<->detail control total +
    referential integrity (customer_id/branch_id). Any offender -> quarantine_reconcile + exit(EXIT_DQ_FAIL)."""
    from delta.tables import DeltaTable
    staged = gate.read_staging()
    if staged.rdd.isEmpty():
        print("[silver.transactions][reconcile] Empty staging batch, skipping reconcile.")
        return
    # silver.transactions keeps only the Smart Key trans_id -> re-extract the raw transaction_id (the ID has no '_').
    header = staged.withColumn("transaction_id", f.element_at(f.split(f.col("trans_id"), "_"), -1))
    ymds = [r["ymd"] for r in header.select("ymd").distinct().collect()]
    offenders = []
    det_path = get_s3_path("silver", "transaction_details")
    if DeltaTable.isDeltaTable(spark, det_path):
        details = (spark.read.format("delta").load(det_path).where(f.col("ymd").isin(ymds))
                   .select("transaction_id", "branch_id", "trans_line_amount"))
        offenders.append(header_detail_amount_mismatches(header, details))
    else:
        print("[silver.transactions][reconcile] silver.transaction_details absent -> skip control-total check.")
    cust_path = get_s3_path("silver", "customers")
    if DeltaTable.isDeltaTable(spark, cust_path):
        dim = spark.read.format("delta").load(cust_path).select("customer_id")
        offenders.append(referential_orphans(header, dim, key="customer_id", reason="customer_id_orphan"))
    else:
        print("[silver.transactions][reconcile] silver.customers absent -> skip customer FK check.")
    br_path = get_s3_path("silver", "branches")
    if DeltaTable.isDeltaTable(spark, br_path):
        dim = spark.read.format("delta").load(br_path).select("branch_id")
        offenders.append(referential_orphans(header, dim, key="branch_id", reason="branch_id_orphan"))
    else:
        print("[silver.transactions][reconcile] silver.branches absent -> skip branch FK check.")
    if not offenders:
        print("[silver.transactions][reconcile] No dim/detail tables available, nothing to reconcile.")
        return
    proj = ["trans_id", "transaction_id", "customer_id", "branch_id",
            "order_total_amount", "ym", "ymd", "failure_reason"]
    bad = offenders[0].select(*proj)
    for o in offenders[1:]:
        bad = bad.unionByName(o.select(*proj))
    n_bad = bad.count()
    if n_bad == 0:
        print("[silver.transactions][reconcile] PASS — header/detail balanced, all FKs resolved.")
        return
    bad = bad.persist()
    (bad.withColumn("run_date", f.lit(gate.run_date))
        .write.format("delta").mode("overwrite")
        .option("replaceWhere", f"run_date = '{gate.run_date}'")
        .partitionBy("run_date").save(PATHS.quarantine_reconcile))
    # reason_counts already carries the full breakdown (reconcile failure_reason is single-reason, not CSV)
    summary = summarize_failures(staged, bad, phase="reconcile", run_date=gate.run_date,
                                 failed=None, grain_col="trans_id")
    write_summary(spark, f"{gate.paths.status}{gate.run_date}.json", summary)
    print("DQ_GATE_FAILED")
    print(f"[silver.transactions][reconcile] RECONCILE FAILED -> {n_bad} offenders quarantined. "
          f"summary={summary['reason_counts']}")
    shutdown_spark(spark)
    sys.exit(EXIT_DQ_FAIL)


def phase_publish(spark, gate: WAPGate) -> None:
    """P: MERGE staging -> prod (newer-wins guard + preserve created_at), then constraint/CDF/DV/Glue."""
    if gate.read_staging().rdd.isEmpty():
        print("[silver.transactions][publish] Empty staging batch, nothing to publish.")
        return
    gate.publish_merge(
        MERGE_KEYS,
        match_condition="s.source_ts_ms > t.source_ts_ms",
        update_exclude_cols=["created_at"],
        prune_col="ymd",
        partition_by=["branch_id", "ym", "ymd"],
    )
    prod = gate.paths.prod
    enable_cdf(spark, prod)
    enable_deletion_vectors(spark, prod)
    ensure_constraint(spark, prod, "valid_order_status",
                      "order_status IN ('W','R','O','D','I','C','B','P','F')")
    ensure_constraint(spark, prod, "valid_channel", "channel IN ('offline','online')")
    register_glue_table(spark, "silver", "transactions", prod)
    print("[silver.transactions][publish] MERGE complete -> prod.")


### Section 3: dispatch
def main():
    ymd = sys.argv[1]
    phase = sys.argv[2] if len(sys.argv) > 2 else "all"
    run_date = ymd.replace("-", "")
    print("PARAM >>>", ymd, "| phase:", phase)
    print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

    spark = _new_spark()
    gate = WAPGate(spark, PATHS, run_date, pipeline=PIPELINE)
    if phase in ("write", "all"):
        phase_write(spark, gate, run_date, ymd)
    if phase in ("audit", "all"):
        phase_audit(spark, gate)   # may sys.exit(EXIT_DQ_FAIL)
    if phase in ("reconcile", "all"):
        phase_reconcile(spark, gate)   # may sys.exit(EXIT_DQ_FAIL)
    if phase in ("publish", "all"):
        phase_publish(spark, gate)
    shutdown_spark(spark)


if __name__ == "__main__":
    main()
