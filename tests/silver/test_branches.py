"""Unit tests for batch_silver_branches.transform — SCD1 dim (_cdc_operation + dedup)."""

from datetime import date, datetime

from chispa.dataframe_comparer import assert_df_equality

from processing.spark_jobs.batch_silver_branches import transform

BR_SCHEMA = (
    "report_date string, report_month string, branch_id string, branch_name string, "
    "branch_type string, region string, city_province string, ward string, status string, "
    "open_date date, created_at timestamp, updated_at timestamp, is_current boolean, _loaded_at timestamp"
)
BR_COLS = [c.split()[0] for c in BR_SCHEMA.split(",")]


def _br_row(spark, **over):
    base = dict(
        report_date="20240501",
        report_month="202405",
        branch_id="B1",
        branch_name="Flagship HCMC",
        branch_type="flagship",
        region="South",
        city_province="HCMC",
        ward=None,
        status="active",
        open_date=date(2020, 1, 1),
        created_at=datetime(2024, 1, 1),
        updated_at=datetime(2024, 5, 1),
        is_current=True,
        _loaded_at=datetime(2024, 5, 2),
    )
    base.update(over)
    return spark.createDataFrame([tuple(base[c] for c in BR_COLS)], BR_SCHEMA)


def test_branches_cdc_operation(spark):
    d = transform(_br_row(spark, is_current=False), "20240501").collect()[0]
    assert d["_cdc_operation"] == 2  # closed
    i = transform(_br_row(spark, created_at=datetime(2024, 5, 1)), "20240501").collect()[0]
    assert i["_cdc_operation"] == 0  # insert
    u = transform(_br_row(spark, created_at=datetime(2024, 1, 1)), "20240501").collect()[0]
    assert u["_cdc_operation"] == 1  # update


def test_branches_drops_is_current(spark):
    assert "is_current" not in transform(_br_row(spark), "20240501").columns


# Intra-batch dedup keeps the latest updated_at per branch_id → multi-row contract.
def test_branches_dedup_keeps_latest(spark):
    older = _br_row(spark, updated_at=datetime(2024, 5, 1), status="active")
    newer = _br_row(spark, updated_at=datetime(2024, 5, 9), status="closed")
    actual = transform(older.unionByName(newer), "20240501").select("branch_id", "status")
    expected = spark.createDataFrame([("B1", "closed")], "branch_id string, status string")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
