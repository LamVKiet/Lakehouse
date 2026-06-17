"""Tests for quarantine_failed_rows — row-level failure tagging (no Deequ jar needed,
it's pure pyspark). Skips only if pydeequ isn't importable (wap.py imports it at load)."""

import os
from datetime import datetime
from decimal import Decimal

import pytest

os.environ.setdefault("SPARK_VERSION", "3.5")  # wap -> pydeequ reads it at import
pytest.importorskip("pydeequ")

from processing.dq.checks import TRANSACTION_GRAIN, TRANSACTION_ROW_RULES  # noqa: E402
from processing.dq.wap import quarantine_failed_rows  # noqa: E402

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


def _reasons(spark, rows):
    df = spark.createDataFrame(rows, SCHEMA)
    bad = quarantine_failed_rows(df, TRANSACTION_ROW_RULES, TRANSACTION_GRAIN)
    return {r["trans_id"]: r["failure_reason"] for r in bad.collect()}


def test_clean_rows_are_not_quarantined(spark):
    out = _reasons(spark, [_row(), _row(trans_id="20260501_C2_T2", customer_id="C2")])
    assert out == {}  # no row violates anything


def test_invalid_status_and_negative_amount_tagged(spark):
    out = _reasons(spark, [_row(trans_id="BAD1", order_status="Z", order_total_amount=Decimal("-1.00"))])
    assert "order_status_valid" in out["BAD1"]
    assert "order_total_amount_non_negative" in out["BAD1"]


def test_null_key_tagged_as_complete_not_valid(spark):
    # order_status NULL -> only caught by completeness, NOT validity (null-tolerant)
    out = _reasons(spark, [_row(trans_id="BAD2", order_status=None)])
    assert "order_status_complete" in out["BAD2"]
    assert "order_status_valid" not in out["BAD2"]


def test_updated_before_created_tagged(spark):
    out = _reasons(spark, [_row(trans_id="BAD3", updated_at=datetime(2026, 4, 30, 0, 0, 0))])
    assert "updated_at_after_created_at" in out["BAD3"]


def test_duplicate_grain_tagged(spark):
    out = _reasons(spark, [_row(), _row()])  # same grain twice
    assert all("duplicate_grain" in v for v in out.values())
    assert len(out) == 2


def test_future_ymd_tagged(spark):
    out = _reasons(spark, [_row(trans_id="BAD4", ymd="20990101", ym="209901")])
    assert "ymd_not_future" in out["BAD4"]


def test_ym_ymd_mismatch_tagged(spark):
    out = _reasons(spark, [_row(trans_id="BAD5", ym="202601")])
    assert "ym_matches_ymd" in out["BAD5"]


def test_empty_customer_id_tagged(spark):
    out = _reasons(spark, [_row(trans_id="BAD6", customer_id="")])
    assert "customer_id_non_empty" in out["BAD6"]
