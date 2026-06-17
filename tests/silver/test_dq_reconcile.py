"""Tests for cross-table reconcile functions (pure pyspark — no pydeequ jar needed)."""

from decimal import Decimal

from processing.dq.reconcile import header_detail_amount_mismatches, referential_orphans

HEADER_SCHEMA = "transaction_id string, branch_id string, customer_id string, order_total_amount decimal(15,2)"
DETAIL_SCHEMA = "transaction_id string, branch_id string, trans_line_amount decimal(15,2)"


def _header(spark, rows):
    return spark.createDataFrame(rows, HEADER_SCHEMA)


def _details(spark, rows):
    return spark.createDataFrame(rows, DETAIL_SCHEMA)


def test_balanced_order_not_flagged(spark):
    header = _header(spark, [("T1", "B1", "C1", Decimal("300.00"))])
    details = _details(spark, [("T1", "B1", Decimal("100.00")), ("T1", "B1", Decimal("200.00"))])
    assert header_detail_amount_mismatches(header, details).count() == 0


def test_control_total_mismatch_flagged(spark):
    header = _header(spark, [("T1", "B1", "C1", Decimal("300.00"))])
    details = _details(spark, [("T1", "B1", Decimal("100.00")), ("T1", "B1", Decimal("150.00"))])
    out = header_detail_amount_mismatches(header, details).collect()
    assert len(out) == 1
    assert out[0]["failure_reason"] == "header_detail_amount_mismatch"
    assert out[0]["detail_total"] == Decimal("250.00")


def test_order_without_details_not_flagged(spark):
    # INNER join -> detail pipeline lagging, an order with no line yet is NOT flagged (timing-tolerant)
    header = _header(spark, [("T1", "B1", "C1", Decimal("300.00"))])
    details = _details(spark, [("T9", "B1", Decimal("300.00"))])
    assert header_detail_amount_mismatches(header, details).count() == 0


def test_tolerance_absorbs_small_diff(spark):
    header = _header(spark, [("T1", "B1", "C1", Decimal("300.00"))])
    details = _details(spark, [("T1", "B1", Decimal("299.50"))])
    assert header_detail_amount_mismatches(header, details, tolerance="1").count() == 0
    assert header_detail_amount_mismatches(header, details, tolerance="0").count() == 1


def test_referential_orphan_flagged(spark):
    header = _header(spark, [("T1", "B1", "C1", Decimal("1.00")), ("T2", "B1", "C2", Decimal("1.00"))])
    dim = spark.createDataFrame([("C1",)], "customer_id string")
    out = {
        r["transaction_id"]: r["failure_reason"]
        for r in referential_orphans(header, dim, key="customer_id", reason="customer_id_orphan").collect()
    }
    assert out == {"T2": "customer_id_orphan"}


def test_null_key_is_tolerated(spark):
    # guest checkout: customer_id null -> not an orphan
    header = _header(spark, [("T1", "B1", None, Decimal("1.00"))])
    dim = spark.createDataFrame([("C1",)], "customer_id string")
    assert referential_orphans(header, dim, key="customer_id", reason="customer_id_orphan").count() == 0
