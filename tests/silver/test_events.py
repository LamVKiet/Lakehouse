"""Unit tests for batch_silver_events — unified clickstream Silver (parse + dedup + funnel_stage).

Bronze now keeps the whole event in `payload` (JSON string), so these build the raw shape
(`payload`, `_ingested_at`) and exercise the two pure functions: parse_events + transform.
"""

import json
from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality

from processing.spark_jobs.batch_silver_events import parse_events, transform

RAW_SCHEMA = "payload string, _ingested_at timestamp"


def _payload(**over):
    """One full event JSON (matches data-simulator); metadata is a nested object."""
    meta = {"search_keyword": "red shirt", "result_count": 5}
    base = dict(
        event_uuid="E1",
        event_id=2,
        event_type="search",
        timestamp=1714552200,
        log_date="2024-05-01",
        created_at="2024-05-01T08:30:00",
        session_id="S1",
        user_id="user_1205",
        device_os="iOS",
        app_version="2.0.0",
        metadata=over.pop("metadata", meta),
    )
    base.update(over)
    return json.dumps(base)


def _raw(spark, *payloads):
    ts = datetime(2024, 5, 1, 8, 30)
    return spark.createDataFrame([(p, ts) for p in payloads], RAW_SCHEMA)


# parse_events flattens top-level cols and keeps metadata as a parsed struct (not exploded).
def test_parse_events_fields_and_metadata_struct(spark):
    parsed = parse_events(_raw(spark, _payload())).collect()[0]
    assert parsed["event_uuid"] == "E1"
    assert parsed["event_id"] == 2
    assert parsed["event_ts"] == 1714552200
    assert parsed["metadata"]["search_keyword"] == "red shirt"
    assert parsed["metadata"]["result_count"] == 5


# funnel_stage derived from event_id: 1-4 discovery, 5-8 cart, 9-14 checkout.
def test_funnel_stage_mapping(spark):
    raw = _raw(
        spark,
        _payload(event_uuid="D", event_id=2),
        _payload(event_uuid="C", event_id=5),
        _payload(event_uuid="K", event_id=13),
    )
    actual = transform(parse_events(raw)).select("event_uuid", "funnel_stage")
    expected = spark.createDataFrame(
        [("D", "discovery"), ("C", "cart"), ("K", "checkout")],
        "event_uuid string, funnel_stage string",
    )
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


# Producer-side duplicate: same event_uuid delivered twice -> dropDuplicates keeps 1 row.
def test_dedup_same_uuid(spark):
    raw = _raw(spark, _payload(), _payload())
    assert transform(parse_events(raw)).count() == 1


# Invalid-row filter: null event_uuid and event_ts <= 0 are dropped.
def test_filter_invalid_rows(spark):
    raw = _raw(spark, _payload(event_uuid="OK"), _payload(event_uuid="ZERO_TS", timestamp=0), _payload(event_uuid=None))
    actual = transform(parse_events(raw)).select("event_uuid")
    expected = spark.createDataFrame([("OK",)], "event_uuid string")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


# ymd derived from log_date (yyyy-MM-dd -> yyyyMMdd) + log_date cast to date.
def test_ymd_and_log_date_derivation(spark):
    out = transform(parse_events(_raw(spark, _payload(log_date="2024-05-01")))).collect()[0]
    assert out["ymd"] == "20240501"
    assert str(out["log_date"]) == "2024-05-01"
    assert out["event_time"] == datetime(2024, 5, 1, 8, 30, 0)
