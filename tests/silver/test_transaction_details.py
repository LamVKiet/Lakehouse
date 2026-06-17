"""Unit tests for batch_silver_transaction_details — parse_cdc (Debezium envelope) + transform.

Two pure functions: parse_cdc(raw, channel) parses the envelope; transform(pos, onl, prod_df)
unions + dedups + types + denormalizes from silver.products.
"""

import json
from decimal import Decimal

from chispa.dataframe_comparer import assert_df_equality

from processing.spark_jobs.batch_silver_transaction_details import parse_cdc, transform

# 2024-05-01 08:30:00 +07 = 1714552200000 ms (session tz Asia/Ho_Chi_Minh).
TS_MS = 1714552200000

RAW_SCHEMA = "payload string, _kafka_offset long"
PARSED_SCHEMA = (
    "transaction_detail_id string, transaction_id string, branch_id string, product_id string, "
    "is_promo string, product_uom_code string, trans_qty int, trans_line_amount string, "
    "variant_size string, variant_color string, created_at bigint, source_ts_ms bigint, "
    "_kafka_offset bigint, channel string"
)
PARSED_COLS = [c.split()[0] for c in PARSED_SCHEMA.split(",")]
PROD_SCHEMA = "product_id string, category_id string, unit_price decimal(15,2)"


def _envelope(op="c", **after_over):
    after = dict(
        transaction_detail_id="D1",
        transaction_id="T1",
        branch_id="B1",
        product_id="P1",
        is_promo="1",
        product_uom_code="PCS",
        trans_qty=2,
        trans_line_amount="398000",
        variant_size="M",
        variant_color="red",
        created_at=TS_MS,
    )
    after.update(after_over)
    return json.dumps({"op": op, "ts_ms": TS_MS, "after": after, "source": {"ts_ms": TS_MS}})


def _raw(spark, rows):
    return spark.createDataFrame(rows, RAW_SCHEMA)


def _parsed(spark, **over):
    base = dict(
        transaction_detail_id="D1",
        transaction_id="T1",
        branch_id="B1",
        product_id="P1",
        is_promo="1",
        product_uom_code="PCS",
        trans_qty=2,
        trans_line_amount="398000",
        variant_size="M",
        variant_color="red",
        created_at=TS_MS,
        source_ts_ms=TS_MS,
        _kafka_offset=10,
        channel="offline",
    )
    base.update(over)
    return spark.createDataFrame([tuple(base[c] for c in PARSED_COLS)], PARSED_SCHEMA)


def _empty_parsed(spark):
    return spark.createDataFrame([], PARSED_SCHEMA)


def _prod(spark, rows=(("P1", "CAT1", Decimal("199000")),)):
    return spark.createDataFrame(list(rows), PROD_SCHEMA)


def test_parse_cdc_extracts_fields_and_channel(spark):
    raw = _raw(spark, [(_envelope(), 10)])
    out = parse_cdc(raw, "offline").collect()[0]
    assert out["transaction_detail_id"] == "D1"
    assert out["channel"] == "offline"
    assert out["is_promo"] == "1"
    assert out["source_ts_ms"] == TS_MS


def test_parse_cdc_drops_deletes(spark):
    raw = _raw(spark, [(_envelope(op="d"), 10), (_envelope(op="c"), 11)])
    out = parse_cdc(raw, "offline")
    assert out.count() == 1  # delete op filtered, only the create remains


def test_transform_smart_key_and_denorm(spark):
    out = transform(_parsed(spark), _empty_parsed(spark), _prod(spark)).collect()[0]
    assert out["line_id"] == "20240501_T1_D1"  # {ymd}_{transaction_id}_{transaction_detail_id}
    assert out["category_id"] == "CAT1"
    assert out["unit_price"] == Decimal("199000.00")
    assert out["is_promo"] == 1  # "1" -> 1
    assert out["trans_line_amount"] == Decimal("398000.00")
    # created_at (timestamp) is tz-fragile when collected to Python; ymd (derived via the
    # session tz inside Spark) is the deterministic contract for the date component.
    assert out["ymd"] == "20240501"


def test_transform_is_promo_normalization(spark):
    out = transform(_parsed(spark, is_promo="false"), _empty_parsed(spark), _prod(spark)).collect()[0]
    assert out["is_promo"] == 0


# Newer source_ts_ms wins on intra-batch dedup → multi-row contract.
def test_transform_dedup_newer_wins(spark):
    old = _parsed(spark, source_ts_ms=TS_MS, trans_qty=2, _kafka_offset=1)
    new = _parsed(spark, source_ts_ms=TS_MS + 1000, trans_qty=9, _kafka_offset=2)
    actual = transform(old.unionByName(new), _empty_parsed(spark), _prod(spark)).select("line_id", "trans_qty")
    expected = spark.createDataFrame([("20240501_T1_D1", 9)], "line_id string, trans_qty int")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
