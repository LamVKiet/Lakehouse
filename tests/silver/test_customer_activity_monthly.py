"""Unit tests for batch_silver_customer_activity_monthly.transform — monthly registry (groupBy)."""

from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality

from processing.spark_jobs.batch_silver_customer_activity_monthly import transform

CAM_SCHEMA = (
    "customer_id string, transaction_id string, branch_id string, "
    "transaction_datetime timestamp, created_at timestamp"
)
CAM_COLS = [c.split()[0] for c in CAM_SCHEMA.split(",")]


def _cam_row(spark, rows):
    return spark.createDataFrame([tuple(r[c] for c in CAM_COLS) for r in rows], CAM_SCHEMA)


def _r(**over):
    base = dict(
        customer_id="C1",
        transaction_id="T1",
        branch_id="B1",
        transaction_datetime=datetime(2024, 5, 2, 9, 0),
        created_at=datetime(2024, 5, 2, 9, 0),
    )
    base.update(over)
    return base


def test_cam_aggregates_per_customer(spark):
    df = _cam_row(
        spark,
        [
            _r(transaction_id="T1", transaction_datetime=datetime(2024, 5, 2, 9, 0)),
            _r(transaction_id="T2", transaction_datetime=datetime(2024, 5, 20, 18, 0)),
        ],
    )
    out = transform(df, "202405").collect()[0]
    assert out["customer_id"] == "C1"
    assert out["ym"] == "202405"
    assert out["trans_count"] == 2
    assert out["first_trans_datetime"] == datetime(2024, 5, 2, 9, 0)
    assert out["last_trans_datetime"] == datetime(2024, 5, 20, 18, 0)


def test_cam_filters_null_customer(spark):
    df = _cam_row(spark, [_r(customer_id=None), _r(customer_id="C1")])
    actual = transform(df, "202405").select("customer_id")
    expected = spark.createDataFrame([("C1",)], "customer_id string")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


# Duplicate snapshot of the same (transaction_id, branch_id) is deduped → counts once.
def test_cam_dedup_snapshot_counts_once(spark):
    df = _cam_row(
        spark,
        [
            _r(transaction_id="T1", created_at=datetime(2024, 5, 2, 9, 0)),
            _r(transaction_id="T1", created_at=datetime(2024, 5, 2, 19, 0)),  # later snapshot, same key
        ],
    )
    out = transform(df, "202405").collect()[0]
    assert out["trans_count"] == 1
