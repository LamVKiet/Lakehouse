"""Unit tests for batch_silver_products.transform — SCD1 dim + category denormalization (JOIN)."""

import re
from datetime import datetime
from decimal import Decimal

from chispa.dataframe_comparer import assert_df_equality

from processing.spark_jobs.batch_silver_products import transform

PROD_SCHEMA = (
    "report_date string, report_month string, product_id string, product_name string, "
    "product_display_name string, category_id string, sales_unit string, color string, "
    "size string, unit_price decimal(15,2), created_at timestamp, updated_at timestamp, "
    "is_current boolean, _loaded_at timestamp"
)
# split on top-level commas only — decimal(15,2) carries an inner comma.
PROD_COLS = [c.split()[0] for c in re.split(r",(?![^()]*\))", PROD_SCHEMA)]
CAT_DF_SCHEMA = "category_id string, category_name string"


def _prod_row(spark, **over):
    base = dict(
        report_date="20240501",
        report_month="202405",
        product_id="P1",
        product_name="BASIC_TEE",
        product_display_name="Basic Cotton T-Shirt",
        category_id="CAT1",
        sales_unit="PCS",
        color="white",
        size="M",
        unit_price=Decimal("199000"),
        created_at=datetime(2024, 1, 1),
        updated_at=datetime(2024, 5, 1),
        is_current=True,
        _loaded_at=datetime(2024, 5, 2),
    )
    base.update(over)
    return spark.createDataFrame([tuple(base[c] for c in PROD_COLS)], PROD_SCHEMA)


def _cat_df(spark, rows=(("CAT1", "Shirts"),)):
    return spark.createDataFrame(list(rows), CAT_DF_SCHEMA)


def test_products_denormalizes_category_name(spark):
    out = transform(_prod_row(spark), _cat_df(spark), "20240501").collect()[0]
    assert out["category_name"] == "Shirts"
    assert out["_cdc_operation"] == 1  # created earlier, still current


def test_products_unmatched_category_is_null(spark):
    # left join — product with a category not in the dim keeps category_name null
    out = transform(_prod_row(spark, category_id="CAT_X"), _cat_df(spark), "20240501").collect()[0]
    assert out["category_name"] is None


# Dedup keeps latest updated_at + denormalized name → multi-row contract.
def test_products_dedup_keeps_latest(spark):
    older = _prod_row(spark, updated_at=datetime(2024, 5, 1), unit_price=Decimal("100000"))
    newer = _prod_row(spark, updated_at=datetime(2024, 5, 9), unit_price=Decimal("150000"))
    actual = transform(older.unionByName(newer), _cat_df(spark), "20240501").select(
        "product_id", "category_name", "unit_price"
    )
    expected = spark.createDataFrame(
        [("P1", "Shirts", Decimal("150000"))],
        "product_id string, category_name string, unit_price decimal(15,2)",
    )
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
