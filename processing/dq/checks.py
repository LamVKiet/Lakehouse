"""
Phase 2 — Check / Constraint / VerificationSuite (the VERDICT layer + the gate).

Check = Analyzer (measure) + Assertion (threshold). The suite does NOT re-scan — internally
it is still the single-pass of Phase 1. CheckLevel.Error = hard (blocks publish); Warning = soft
(publish + alert).

One suite per layer: bronze (schema/size/freshness/contract), silver (6 dimensions),
gold (.satisfies business rule + reconciliation). Tune the builders below to your real schema.
"""
from __future__ import annotations

from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pyspark.sql import SparkSession

# =============================================================================
# BRONZE — strict on third-party data: block garbage at the gate
# =============================================================================
def bronze_check(spark: SparkSession, *, fresh_predicate: str) -> Check:
    c = Check(spark, CheckLevel.Error, "bronze_contract_gate")
    return (
        c.hasSize(lambda n: n >= 1)                       # volume: not empty
        .hasDataType("account_no", ConstrainableDataTypes.Integral)  # type inferred from CONTENT
        # Freshness: Deequ has no Freshness() -> build it from Compliance + a fixed UTC reference.
        # WARNING: pass a precomputed reference into the predicate; don't run current_timestamp() in the scan.
        .satisfies(fresh_predicate, "is_fresh", lambda f: f >= 1.0)
    )


# =============================================================================
# SILVER — silver.transactions (6 dimensions, the project's real schema)
# =============================================================================
ORDER_STATUS = ["W", "R", "O", "D", "I", "C", "B", "P", "F"]
CHANNELS = ["offline", "online"]
PAYMENT_TYPES = ["cash", "card", "momo", "zalopay", "vnpay"]


def _in_list(col: str, vals: list[str]) -> str:
    return f"{col} IN (" + ", ".join(f"'{v}'" for v in vals) + ")"


# SINGLE SOURCE OF TRUTH for ROW-level rules: (name, valid_predicate, min_ratio).
# valid_predicate = TRUE when the row is VALID. Used by BOTH:
#   - the Deequ hard check (produces the ratio -> gate decision), and
#   - quarantine_failed_rows (tags the row-level failure_reason).
# Validity/consistency are null-tolerant (null is NOT invalid) to match Deequ semantics;
# completeness is a separate null-check rule above.
TRANSACTION_ROW_RULES = [
    # completeness — null-check the 6 grain columns
    ("trans_id_complete",               "trans_id IS NOT NULL", 1.0),
    ("customer_id_complete",            "customer_id IS NOT NULL", 1.0),
    ("branch_id_complete",              "branch_id IS NOT NULL", 1.0),
    ("channel_complete",                "channel IS NOT NULL", 1.0),
    ("order_status_complete",           "order_status IS NOT NULL", 1.0),
    ("order_total_amount_complete",     "order_total_amount IS NOT NULL", 1.0),
    # validity — domain + sign; empty-string guard (completeness only blocks NULL, not '')
    ("order_status_valid",              f"order_status IS NULL OR {_in_list('order_status', ORDER_STATUS)}", 1.0),
    ("channel_valid",                   f"channel IS NULL OR {_in_list('channel', CHANNELS)}", 1.0),
    ("order_total_amount_non_negative", "order_total_amount IS NULL OR order_total_amount >= 0", 1.0),
    ("trans_id_non_empty",              "trans_id IS NULL OR length(trim(trans_id)) > 0", 1.0),
    ("customer_id_non_empty",           "customer_id IS NULL OR length(trim(customer_id)) > 0", 1.0),
    ("branch_id_non_empty",             "branch_id IS NULL OR length(trim(branch_id)) > 0", 1.0),
    # consistency — cross-field: updated after created; derived partition key must match (ym = first 6 chars of ymd)
    ("updated_at_after_created_at",     "created_at IS NULL OR updated_at IS NULL OR updated_at >= created_at", 0.999),
    ("ym_matches_ymd",                  "ym IS NULL OR ymd IS NULL OR ym = substr(ymd, 1, 6)", 1.0),
    # timeliness — the transaction date must not be in the future (clock/timezone error)
    ("ymd_not_future",                  "ymd IS NULL OR to_date(ymd, 'yyyyMMdd') <= current_date()", 1.0),
]

# MERGE grain — BATCH-level UNIQUENESS rule (cross-row, can't be pinned to one row; handled separately).
TRANSACTION_GRAIN = ["trans_id", "branch_id", "ym", "ymd"]


def silver_transactions_check(spark: SparkSession) -> Check:
    """Hard gate (Error) — a breach blocks publish + quarantines. Row rules are built from
    TRANSACTION_ROW_RULES (same source as quarantine); volume + uniqueness are batch-level extras."""
    c = Check(spark, CheckLevel.Error, "silver_transactions_gate")
    c = c.hasSize(lambda n: n >= 1)                                  # volume (batch-level)
    for name, pred, thr in TRANSACTION_ROW_RULES:                   # 6-dimension row rules
        c = c.satisfies(pred, name, lambda f, t=thr: f >= t)
    c = c.hasUniqueness(TRANSACTION_GRAIN, lambda u: u == 1.0)      # grain (batch-level)
    return c


def silver_transactions_soft_check(spark: SparkSession) -> Check:
    """Soft gate (Warning) — publish + alert. Optional columns / suspicious thresholds, no block."""
    c = Check(spark, CheckLevel.Warning, "silver_transactions_soft")
    return (
        # payment_type may be empty while the order is still pending -> soft threshold
        c.hasCompleteness("payment_type", lambda x: x >= 0.95)
        .isContainedIn("payment_type", PAYMENT_TYPES)
        # per-order sanity ceiling (~500M VND) — exceeding it suggests a wrong money-unit entry
        .hasMax("order_total_amount", lambda m: m <= 500_000_000)
    )


# =============================================================================
# GOLD — fewer technical rules, more analyst-authored business rules + reconciliation
# =============================================================================
def gold_hard_check(spark: SparkSession) -> Check:
    c = Check(spark, CheckLevel.Error, "gold_hard_gate")
    return (
        c.hasSize(lambda n: n >= 1)
        .isComplete("transaction_id")
        .isUnique("transaction_id")
        .isComplete("amount")
        .satisfies(  # business rule authored by the analyst
            "amount = debit_total - credit_total",
            "ledger_balances",
            lambda f: f >= 0.999,
        )
    )


def gold_soft_check(spark: SparkSession) -> Check:
    """Warning: publish but alert. Anomaly (Phase 5) also lands at this level."""
    c = Check(spark, CheckLevel.Warning, "gold_soft_signals")
    return (
        c.hasCompleteness("memo", lambda x: x >= 0.30)
        .hasMin("amount", lambda m: m >= 0)
    )
