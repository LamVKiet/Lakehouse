"""Unit tests for batch_silver_category.transform — SCD1 dim (_cdc_operation + dedup)."""

from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality

from processing.spark_jobs.batch_silver_category import transform

CAT_SCHEMA = (
    "report_date string, report_month string, category_id string, category_name string, "
    "is_current boolean, created_at timestamp, updated_at timestamp, _loaded_at timestamp"
)
CAT_COLS = [c.split()[0] for c in CAT_SCHEMA.split(",")]


def _cat_row(spark, **over):
    base = dict(
        report_date="20240501",
        report_month="202405",
        category_id="CAT1",
        category_name="Shirts",
        is_current=True,
        created_at=datetime(2024, 1, 1),
        updated_at=datetime(2024, 5, 1),
        _loaded_at=datetime(2024, 5, 2),
    )
    base.update(over)
    return spark.createDataFrame([tuple(base[c] for c in CAT_COLS)], CAT_SCHEMA)


def test_category_cdc_operation(spark):
    # is_current=0 -> 2 (discontinued)
    d = transform(_cat_row(spark, is_current=False), "20240501").collect()[0]
    assert d["_cdc_operation"] == 2
    # created on run_date -> 0 (insert)
    i = transform(_cat_row(spark, created_at=datetime(2024, 5, 1)), "20240501").collect()[0]
    assert i["_cdc_operation"] == 0
    # created earlier, still current -> 1 (update)
    u = transform(_cat_row(spark, created_at=datetime(2024, 1, 1)), "20240501").collect()[0]
    assert u["_cdc_operation"] == 1


def test_category_drops_bronze_meta(spark):
    out_cols = set(transform(_cat_row(spark), "20240501").columns)
    assert {"report_date", "report_month", "_loaded_at", "is_current"}.isdisjoint(out_cols)
    assert out_cols == {"category_id", "category_name", "_cdc_operation", "created_at", "updated_at", "_processed_at"}


# Intra-batch dedup keeps the latest updated_at per category_id → multi-row contract.
def test_category_dedup_keeps_latest(spark):
    older = _cat_row(spark, updated_at=datetime(2024, 5, 1), category_name="Old")
    newer = _cat_row(spark, updated_at=datetime(2024, 5, 9), category_name="New")
    actual = transform(older.unionByName(newer), "20240501").select("category_id", "category_name")
    expected = spark.createDataFrame([("CAT1", "New")], "category_id string, category_name string")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
