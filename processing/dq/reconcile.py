"""
Phase 6b — Cross-table reconciliation (Accuracy dimension) for silver.transactions.

Deequ checks one table at a time -> it CANNOT express cross-table rules. The two real Accuracy
rules of a money table live here:
  - header<->detail control total: SUM(silver.transaction_details.trans_line_amount) per order
    == silver.transactions.order_total_amount (a total mismatch = cast/unit/missing line).
  - referential integrity: every staged customer_id / branch_id must exist in its dim.

Runs in a SEPARATE `reconcile` phase, AFTER the Deequ audit and BEFORE publish, on the STAGED
batch (validate before it reaches prod). Offending rows are tagged with `failure_reason` ->
quarantine; any hit -> exit 42 (no retry), like the audit gate.

Pure (df -> df) so it is unit-testable; the job owns all I/O + the DeltaTable.isDeltaTable guard.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
import pyspark.sql.functions as f


def header_detail_amount_mismatches(header: DataFrame, details: DataFrame, *, tolerance: str = "0") -> DataFrame:
    """Orders whose SUM(detail line amount) != header order_total_amount (beyond `tolerance`).
      - header: needs transaction_id, branch_id, order_total_amount (+ identifying cols to keep).
      - details: needs transaction_id, branch_id, trans_line_amount.
    INNER join -> orders with NO detail yet (detail pipeline lagging) are NOT flagged here (timing-tolerant).
    Returns the offending header rows + detail_total + failure_reason."""
    detail_tot = (
        details.groupBy("transaction_id", "branch_id")
        .agg(f.sum("trans_line_amount").cast("decimal(15,2)").alias("detail_total"))
    )
    tol = f.lit(tolerance).cast("decimal(15,2)")
    return (
        header.join(detail_tot, ["transaction_id", "branch_id"], "inner")
        .where(f.abs(f.col("order_total_amount") - f.col("detail_total")) > tol)
        .withColumn("failure_reason", f.lit("header_detail_amount_mismatch"))
    )


def referential_orphans(header: DataFrame, dim: DataFrame, *, key: str, reason: str) -> DataFrame:
    """Staged rows whose `key` is non-null but has no match in the dim (left-anti join).
    Nulls are spared (e.g. guest-checkout customer_id null). `dim` must contain a column named `key`."""
    non_null = header.where(f.col(key).isNotNull())
    dim_keys = dim.select(f.col(key)).distinct()
    return (
        non_null.join(dim_keys, [key], "left_anti")
        .withColumn("failure_reason", f.lit(reason))
    )
