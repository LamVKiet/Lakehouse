"""Deequ gate tests for silver.transactions checks (Phase 2 of processing.dq).

Runs a real VerificationSuite, so it needs the Deequ jar on the classpath. Set
DEEQU_LOCAL_JAR to deequ-2.0.7-spark-3.5.jar before running locally; on CI (no jar)
the whole module skips. The shared `spark` fixture loads the jar when the env is set.
"""

import os
from datetime import datetime
from decimal import Decimal

import pytest

# PyDeequ resolves its coordinate from SPARK_VERSION at import time — set before importing.
os.environ.setdefault("SPARK_VERSION", "3.5")

pytestmark = pytest.mark.skipif(
    not os.environ.get("DEEQU_LOCAL_JAR", "").strip(),
    reason="DEEQU_LOCAL_JAR not set — Deequ jar unavailable, skipping DQ suite tests.",
)

from pydeequ.verification import VerificationSuite  # noqa: E402

from processing.dq.checks import silver_transactions_check  # noqa: E402

SCHEMA = (
    "trans_id string, customer_id string, branch_id string, channel string, "
    "order_status string, payment_type string, order_total_amount decimal(15,2), "
    "created_at timestamp, updated_at timestamp, ym string, ymd string"
)
COLS = [
    "trans_id",
    "customer_id",
    "branch_id",
    "channel",
    "order_status",
    "payment_type",
    "order_total_amount",
    "created_at",
    "updated_at",
    "ym",
    "ymd",
]
T0 = datetime(2026, 5, 1, 8, 30, 0)


def _row(**over):
    base = dict(
        trans_id="20260501_C1_T1",
        customer_id="C1",
        branch_id="B1",
        channel="offline",
        order_status="W",
        payment_type="cash",
        order_total_amount=Decimal("500000.00"),
        created_at=T0,
        updated_at=T0,
        ym="202605",
        ymd="20260501",
    )
    base.update(over)
    return tuple(base[c] for c in COLS)


def _status(spark, rows):
    df = spark.createDataFrame(rows, SCHEMA)
    result = VerificationSuite(spark).onData(df).addCheck(silver_transactions_check(spark)).run()
    return result.status


def test_hard_gate_passes_on_clean_batch(spark):
    assert _status(spark, [_row(), _row(trans_id="20260501_C2_T2", customer_id="C2")]) == "Success"


def test_hard_gate_fails_on_invalid_order_status(spark):
    assert _status(spark, [_row(order_status="Z")]) == "Error"


def test_hard_gate_fails_on_negative_amount(spark):
    assert _status(spark, [_row(order_total_amount=Decimal("-1.00"))]) == "Error"


def test_hard_gate_fails_on_duplicate_grain(spark):
    # Same (trans_id, branch_id, ym, ymd) twice -> uniqueness != 1.0.
    assert _status(spark, [_row(), _row()]) == "Error"


def test_hard_gate_fails_when_updated_before_created(spark):
    assert _status(spark, [_row(updated_at=datetime(2026, 4, 30, 0, 0, 0))]) == "Error"


def test_hard_gate_fails_on_future_ymd(spark):
    assert _status(spark, [_row(ymd="20990101", ym="209901")]) == "Error"


def test_hard_gate_fails_on_ym_ymd_mismatch(spark):
    assert _status(spark, [_row(ym="202601")]) == "Error"


def test_hard_gate_fails_on_empty_customer_id(spark):
    assert _status(spark, [_row(customer_id="")]) == "Error"
