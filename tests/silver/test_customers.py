"""Unit tests for batch_silver_customers.transform — builder + per-field asserts (flat, edge-heavy).

SCD1 dedup is the one multi-row contract → assert_df_equality.
transform stamps a non-deterministic _processed_at, so the equality test .select()s only
the columns under contract.
"""

from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality

from processing.spark_jobs.batch_silver_customers import transform

# Explicit schema — `dob` is all-None in fixtures, so type inference can't run.
CUST_SCHEMA = (
    "customer_id string, first_name string, last_name string, phone string, "
    "dob date, age int, gender int, address_line string, is_deleted int, "
    "registered_datetime timestamp, created_at timestamp, updated_at timestamp, "
    "source string, report_date string, report_month string, _loaded_at timestamp"
)
CUST_COLS = [c.split()[0] for c in CUST_SCHEMA.split(",")]


def _cust_row(spark, **over):
    base = dict(
        customer_id="C1",
        first_name="An",
        last_name="Nguyen",
        phone="0901234567",
        dob=None,
        age=30,
        gender=1,
        address_line="12 Le Loi, District 1, HCMC",
        is_deleted=0,
        registered_datetime=datetime(2023, 1, 1),
        created_at=datetime(2024, 5, 1),
        updated_at=datetime(2024, 5, 1),
        source="online_web",
        report_date="20240501",
        report_month="202405",
        _loaded_at=datetime(2024, 5, 2),
    )
    base.update(over)
    return spark.createDataFrame([tuple(base[c] for c in CUST_COLS)], CUST_SCHEMA)


def test_customers_clean_fields(spark):
    out = transform(_cust_row(spark), "20240501").collect()[0]
    assert out["full_name"] == "Nguyen An"
    assert out["phone"] == "xxxxxxx567"  # mask all but last 3
    assert out["gender"] == "M"
    assert out["city"] == "HCMC"  # last segment after comma


def test_customers_gender_and_city_fallback(spark):
    df = _cust_row(spark, gender=2, address_line=None, phone=None)
    out = transform(df, "20240501").collect()[0]
    assert out["gender"] == "F"
    assert out["city"] == "unknown"
    assert out["phone"] is None


def test_customers_cdc_operation(spark):
    # deleted -> 2
    d = transform(_cust_row(spark, is_deleted=1), "20240501").collect()[0]
    assert d["_cdc_operation"] == 2
    # created on run_date -> 0 (insert)
    i = transform(_cust_row(spark, created_at=datetime(2024, 5, 1)), "20240501").collect()[0]
    assert i["_cdc_operation"] == 0
    # created earlier -> 1 (update)
    u = transform(_cust_row(spark, created_at=datetime(2024, 1, 1)), "20240501").collect()[0]
    assert u["_cdc_operation"] == 1


# SCD1 dedup is a multi-row contract → assert_df_equality.
def test_customers_dedup_keeps_latest_per_id(spark):
    older = _cust_row(spark, updated_at=datetime(2024, 5, 1), source="offline", phone="0900000001")
    newer = _cust_row(spark, updated_at=datetime(2024, 5, 9), source="app_store", phone="0900000009")
    actual = transform(older.unionByName(newer), "20240501").select("customer_id", "source", "phone")
    expected = spark.createDataFrame(
        [("C1", "app_store", "xxxxxxx009")],
        "customer_id string, source string, phone string",
    )
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
