"""Unit tests for batch_silver_nou.transform — first-ever order registry (anti-join + min date)."""

from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality

from processing.spark_jobs.batch_silver_nou import transform

TODAY_SCHEMA = "customer_id string"
BRONZE_SCHEMA = "customer_id string, transaction_datetime timestamp"
EXISTING_SCHEMA = "customer_id string"


def _today(spark, ids):
    return spark.createDataFrame([(i,) for i in ids], TODAY_SCHEMA)


def _bronze(spark, rows):
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


# nou_ymd = earliest order across ALL history, not just today's partition.
def test_nou_first_ever_date_from_all_history(spark):
    today = _today(spark, ["C1"])
    all_bronze = _bronze(
        spark,
        [
            ("C1", datetime(2024, 5, 1, 10, 0)),  # today
            ("C1", datetime(2024, 1, 9, 8, 0)),  # earlier history → this is nou
        ],
    )
    actual = transform(today, all_bronze).select("customer_id", "nou_ymd", "ym")
    expected = spark.createDataFrame([("C1", "20240109", "202401")], "customer_id string, nou_ymd string, ym string")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


# existing customers are excluded via left-anti join (insert-only registry).
def test_nou_excludes_existing_customers(spark):
    today = _today(spark, ["C1", "C2"])
    all_bronze = _bronze(
        spark,
        [
            ("C1", datetime(2024, 5, 1, 10, 0)),
            ("C2", datetime(2024, 5, 1, 11, 0)),
        ],
    )
    existing = spark.createDataFrame([("C1",)], EXISTING_SCHEMA)
    actual = transform(today, all_bronze, existing).select("customer_id")
    expected = spark.createDataFrame([("C2",)], "customer_id string")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


# null customer_id in today's batch is filtered before the registry build.
def test_nou_filters_null_customer(spark):
    today = _today(spark, ["C1", None])
    all_bronze = _bronze(spark, [("C1", datetime(2024, 5, 1, 10, 0))])
    actual = transform(today, all_bronze).select("customer_id")
    expected = spark.createDataFrame([("C1",)], "customer_id string")
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
