"""
Phase 6 — WAP + Delta integration (wiring the engine into the pipeline).

Write   -> write the transformed batch to staging, isolated by run_date (ingestion window).
Audit   -> VerificationSuite over the staged partition; status=Error => block + quarantine.
Publish -> only on pass: Delta MERGE (newer-wins) or replaceWhere => atomically visible.

The three Airflow tasks (write / audit / publish) are separate processes -> they cannot share
an in-memory DataFrame. Staging is therefore partitioned + filtered by `run_date`: each task
re-reads exactly that day's batch, idempotent on re-run (overwrite of the same run_date partition).

Two layers of defense:
  - Delta CHECK / NOT NULL = PREVENTIVE, fails at write time, cheap but only simple rules.
  - Deequ suite            = DETECTIVE, every dimension + ratios + evidence + history.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from pydeequ.verification import VerificationResult, VerificationSuite

from processing.spark_jobs.delta_utils import ensure_constraint
from .config import Paths, make_tags
from .exceptions import DataQualityError
from .repository import get_repository, make_key


# =============================================================================
# Preventive layer — Delta CHECK constraint (set once, idempotent)
# =============================================================================
def apply_delta_constraints(spark: SparkSession, table_path: str, constraints: list[tuple[str, str]]) -> None:
    """Block coarse garbage right at the write commit -> saves one audit round. Delegates to
    ensure_constraint (ADD CONSTRAINT IF NOT EXISTS). `constraints` = [(name, expression), ...]."""
    for name, expr in constraints:
        ensure_constraint(spark, table_path, name, expr)


def quarantine_failed_rows(df: DataFrame, row_rules: list[tuple], grain: list[str] | None = None) -> DataFrame:
    """Deequ only returns the violation RATIO, not which rows. This reuses the exact rule
    predicates to tag each row + filter out the bad ones for quarantine forensics.
      - row_rules = [(name, valid_predicate, _min_ratio), ...] (ROW-level): violation = NOT valid.
      - grain (optional): rows duplicated on grain -> reason 'duplicate_grain' (batch-level rule).
    Returns only rows violating >=1 rule, keeping the original columns + a `failure_reason` column
    (CSV of rule names). Note: batch-level rules like hasSize can't be pinned to a single row -> not here."""
    from pyspark.sql import Window
    reasons = [f.when(~f.expr(pred), f.lit(name)) for name, pred, _ in row_rules]
    if grain:
        reasons.append(f.when(f.count("*").over(Window.partitionBy(*grain)) > 1, f.lit("duplicate_grain")))
    return (df.withColumn("failure_reason", f.concat_ws(",", *reasons))
              .where("failure_reason <> ''"))


# =============================================================================
# WAP engine
# =============================================================================
@dataclass
class AuditOutcome:
    status: str          # "Success" | "Warning" | "Error"
    publish_ok: bool
    failed: list[str]
    evidence_path: str


class WAPGate:
    """One table = one WAPGate, used inside each Airflow task (Phase 7).
    Staging is isolated by run_date so the three separate tasks re-read the same batch."""

    def __init__(self, spark: SparkSession, paths: Paths, run_date: str, *, pipeline: str):
        self.spark = spark
        self.paths = paths
        self.run_date = run_date
        self.pipeline = pipeline

    # --- WRITE ---------------------------------------------------------------
    def write_staging(self, df: DataFrame) -> None:
        """Write the transformed batch to staging. Tag run_date (ingestion) + overwrite exactly
        that partition -> idempotent, never touching another day's batch."""
        (
            df.withColumn("run_date", f.lit(self.run_date))
            .write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"run_date = '{self.run_date}'")
            .partitionBy("run_date")
            .save(self.paths.staging)
        )

    def read_staging(self) -> DataFrame:
        # Incremental: only this batch's partition (partition pruning), NOT a full scan.
        return (
            self.spark.read.format("delta")
            .load(self.paths.staging)
            .where(f"run_date = '{self.run_date}'")
        )

    # --- AUDIT ---------------------------------------------------------------
    def audit(self, checks: list, *, with_anomaly=None, row_rules=None, grain=None) -> AuditOutcome:
        """Run the suite on the staged data. Persist evidence + metric history BEFORE deciding.
        Error => quarantine + raise DataQualityError (NO retry). Warning/Success => publish.
        row_rules/grain (optional): if given, quarantine writes only the BAD ROWS + a failure_reason
        column (via quarantine_failed_rows); otherwise it falls back to writing the whole batch."""
        df_staged = self.read_staging()
        df_staged.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            repo = get_repository(self.spark, self.paths.metrics)
            tags = make_tags(self.paths, self.run_date, pipeline=self.pipeline)
            key = make_key(self.spark, tags)

            suite = VerificationSuite(self.spark).onData(df_staged).useRepository(repo)
            for chk in checks:
                suite = suite.addCheck(chk)
            if with_anomaly is not None:
                suite = with_anomaly(suite)
            result = suite.saveOrAppendResult(key).run()

            # 1) EVIDENCE first — always persisted, pass or fail (proof for the auditor)
            detail = VerificationResult.checkResultsAsDataFrame(self.spark, result)
            (
                detail.withColumn("_run_date", f.lit(self.run_date))
                .write.format("delta").mode("append").save(self.paths.evidence)
            )

            failed = [
                r["constraint"]
                for r in detail.where("constraint_status = 'Failure'").collect()
            ]
            status = result.status
            outcome = AuditOutcome(
                status=status,
                publish_ok=(status != "Error"),
                failed=failed,
                evidence_path=self.paths.evidence,
            )

            # 2) Error => quarantine (isolate, do NOT delete staging) then raise no-retry.
            #    With row_rules -> write only bad rows + failure_reason; otherwise -> whole batch.
            if status == "Error":
                bad = (quarantine_failed_rows(df_staged, row_rules, grain)
                       if row_rules else df_staged)
                (
                    bad.write.format("delta").mode("overwrite")
                    .option("replaceWhere", f"run_date = '{self.run_date}'")
                    .partitionBy("run_date")
                    .save(self.paths.quarantine)
                )
                raise DataQualityError(status, failed, self.paths.evidence)

            return outcome  # Success or Warning -> allow publish
        finally:
            df_staged.unpersist()

    # --- PUBLISH -------------------------------------------------------------
    def publish_merge(
        self,
        merge_keys: list[str],
        *,
        match_condition: str | None = None,
        update_exclude_cols: list[str] | None = None,
        prune_col: str | None = None,
        partition_by: list[str] | None = None,
    ) -> None:
        """MERGE staging -> prod. Parameters mirror the CDC semantics of silver.transactions:
          - merge_keys: the MERGE key set (the correct grain).
          - match_condition: newer-wins guard, e.g. 's.source_ts_ms > t.source_ts_ms'.
          - update_exclude_cols: columns preserved on UPDATE (e.g. created_at).
          - prune_col: partition column to add 't.<col> IN (...)' -> Delta prunes the target.
          - partition_by: used on the first table creation (overwrite).
        Drops the staging run_date column before MERGE (prod has no such column)."""
        from delta.tables import DeltaTable

        src = self.read_staging().drop("run_date")
        if not DeltaTable.isDeltaTable(self.spark, self.paths.prod):
            writer = src.write.format("delta").mode("overwrite")
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(self.paths.prod)
            return

        cond = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
        if prune_col:
            vals = [r[prune_col] for r in src.select(prune_col).distinct().collect()]
            pred = ",".join(f"'{v}'" for v in vals)
            cond += f" AND t.{prune_col} IN ({pred})"

        builder = DeltaTable.forPath(self.spark, self.paths.prod).alias("t").merge(src.alias("s"), cond)
        if match_condition is not None or update_exclude_cols is not None:
            update_set = {c: f"s.{c}" for c in src.columns if c not in (update_exclude_cols or [])}
            builder = builder.whenMatchedUpdate(condition=match_condition, set=update_set)
        else:
            builder = builder.whenMatchedUpdateAll()
        builder.whenNotMatchedInsertAll().execute()

    def publish_replace_where(self, partition_col: str, partition_by: list[str] | None = None) -> None:
        """Batch = a whole partition -> replaceWhere (atomic). Used for the Gold-style path."""
        src = self.read_staging().drop("run_date")
        writer = src.write.format("delta").mode("overwrite")
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(self.spark, self.paths.prod):
            vals = [r[partition_col] for r in src.select(partition_col).distinct().collect()]
            pred = ",".join(f"'{v}'" for v in vals)
            writer = writer.option("replaceWhere", f"{partition_col} IN ({pred})")
        elif partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(self.paths.prod)
