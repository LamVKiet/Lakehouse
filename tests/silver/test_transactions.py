"""Unit tests for batch_silver_transactions_cdc — parse_cdc (Debezium envelope) + transform.

Two pure functions: parse_cdc(raw, channel) parses the envelope; transform(pos, onl)
unions + dedups (newer source_ts_ms wins) + types + builds the Smart Key.
"""

import json
from decimal import Decimal

from chispa.dataframe_comparer import assert_df_equality

from processing.spark_jobs.batch_silver_transactions_cdc import parse_cdc, transform

# 2024-05-01 08:30:00 +07 = 1714552200000 ms (session tz Asia/Ho_Chi_Minh).
TS_MS = 1714552200000

RAW_SCHEMA = "payload string, _kafka_offset long"
PARSED_SCHEMA = (
    "transaction_id string, branch_id string, transaction_datetime bigint, payment_type string, "
    "trans_total_amount string, customer_id string, order_status string, created_at bigint, "
    "updated_at bigint, source_ts_ms bigint, _kafka_offset bigint, channel string"
)
PARSED_COLS = [c.split()[0] for c in PARSED_SCHEMA.split(",")]


def _envelope(op="c", **after_over):
    after = dict(
        transaction_id="T1",
        branch_id="B1",
        transaction_datetime=TS_MS,
        payment_type="cash",
        trans_total_amount="500000",
        customer_id="C1",
        order_status="W",
        created_at=TS_MS,
        updated_at=TS_MS,
    )
    after.update(after_over)
    return json.dumps({"op": op, "ts_ms": TS_MS, "after": after, "source": {"ts_ms": TS_MS}})


def _raw(spark, rows):
    return spark.createDataFrame(rows, RAW_SCHEMA)


def _parsed(spark, **over):
    base = dict(
        transaction_id="T1",
        branch_id="B1",
        transaction_datetime=TS_MS,
        payment_type="cash",
        trans_total_amount="500000",
        customer_id="C1",
        order_status="W",
        created_at=TS_MS,
        updated_at=TS_MS,
        source_ts_ms=TS_MS,
        _kafka_offset=10,
        channel="offline",
    )
    base.update(over)
    return spark.createDataFrame([tuple(base[c] for c in PARSED_COLS)], PARSED_SCHEMA)


def _empty_parsed(spark):
    return spark.createDataFrame([], PARSED_SCHEMA)


def test_parse_cdc_extracts_fields_and_channel(spark):
    raw = _raw(spark, [(_envelope(), 10)])
    out = parse_cdc(raw, "offline").collect()[0]
    assert out["transaction_id"] == "T1"
    assert out["channel"] == "offline"
    assert out["order_status"] == "W"
    assert out["source_ts_ms"] == TS_MS


def test_parse_cdc_drops_deletes(spark):
    raw = _raw(spark, [(_envelope(op="d"), 10), (_envelope(op="c"), 11)])
    out = parse_cdc(raw, "offline")
    assert out.count() == 1  # delete op filtered, only the create remains


def test_transform_smart_key_and_derived(spark):
    out = transform(_parsed(spark), _empty_parsed(spark)).collect()[0]
    assert out["trans_id"] == "20240501_C1_T1"  # {ymd}_{customer_id}_{transaction_id}
    assert out["ym"] == "202405"
    assert out["order_total_amount"] == Decimal("500000.00")  # "500000" -> decimal(15,2)
    # transaction_datetime (timestamp) is tz-fragile when collected to Python; ymd (derived via
    # the session tz inside Spark) is the deterministic contract for the date component.
    assert out["ymd"] == "20240501"


# Newer source_ts_ms wins on intra-batch dedup; order_status flips W -> B → multi-row contract.
def test_transform_dedup_newer_wins(spark):
    old = _parsed(spark, order_status="W", source_ts_ms=TS_MS, _kafka_offset=1)
    new = _parsed(spark, order_status="B", source_ts_ms=TS_MS + 1000, _kafka_offset=2)
    actual = transform(old.unionByName(new), _empty_parsed(spark)).select("trans_id", "order_status")
    expected = spark.createDataFrame([("20240501_C1_T1", "B")], "trans_id string, order_status string")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
