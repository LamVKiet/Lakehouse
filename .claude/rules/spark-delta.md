---
description: "Rules for writing Spark jobs with Delta Lake, S3, AWS Glue Catalog, and Trino queries"
paths:
  - "processing/**"
  - "notebooks/**"
  - "orchestration/**"
  - "analytics/**"
---

# Spark + Delta Lake Rules

## Spark Session — Always Use delta_utils.py

All Spark jobs must use the factory in `processing/spark_jobs/delta_utils.py`:

```python
from delta_utils import get_spark_session

spark = get_spark_session("job-name")
```

The factory sets the following configs required for Delta + S3:

```python
SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
    .getOrCreate()
```

## S3 Path Convention

```python
S3_BUCKET = os.getenv("S3_BUCKET")

def get_s3_path(layer: str, table: str) -> str:
    return f"s3a://{S3_BUCKET}/{layer}/{table}/"

# Examples:
# get_s3_path("bronze", "behavior_events")    -> s3a://bucket/bronze/behavior_events/
# get_s3_path("silver", "events")             -> s3a://bucket/silver/events/
# get_s3_path("gold", "funnel_hourly")        -> s3a://bucket/gold/funnel_hourly/
```

## streaming_to_bronze.py Rules

- Mode: `foreachBatch`, trigger every `30 seconds`.
- Read only `behavior_events` topic (transaction data now sourced from MySQL via batch_bronze_sql.py).
- Add Kafka metadata columns: `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_ingested_at`.
- Store `metadata` field as a raw JSON string in `raw_metadata` column — do NOT flatten.
- Write mode: `append` only. Bronze is immutable.
- Checkpoint path: `/checkpoints/bronze/` (named Docker volume — must persist across restarts).

```python
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/bronze/behavior_events")
    .trigger(processingTime="30 seconds")
    .foreachBatch(write_bronze_batch)
    .start(get_s3_path("bronze", "behavior_events"))
)
```

## Batch Bronze Rules (MySQL -> S3/Delta)

- **Append-only**: Bronze is immutable. Never use MERGE or UPDATE on Bronze tables.
- **Incremental ingestion**: Use `read_mysql_incremental()` with `DATE(COALESCE(updated_at, created_at)) = ymd`.
- **Metadata columns**: Add `report_date` (yyyyMMdd), `report_month` (yyyyMM), `_loaded_at` (current_timestamp).
- **Partition**: All Bronze MySQL tables partition by `report_date`.
- **Schema preservation**: Keep all source columns exactly as-is (including `is_current` for SCD2 sources). Downstream Silver handles dedup/SCD2.

```python
from processing.spark_jobs.delta_utils import read_mysql_incremental, write_delta_append

raw = read_mysql_incremental(spark, "table_name", ymd)
# For tables without updated_at:
raw = read_mysql_incremental(spark, "table_name", ymd, date_col="created_at")

write_delta_append(df, path, partition_by="report_date")
```

## batch_silver_transform.py Rules

- Receives date as `sys.argv[1]` from Airflow (T-1 day).
- Reads Bronze partition: `WHERE log_date = '{date}'`.
- Transforms:
  - Parse `created_at` string -> `TimestampType` -> `event_time`.
  - Flatten `raw_metadata` JSON string -> individual columns using `from_json()`.
  - Filter out rows where `event_uuid IS NULL` or `event_ts = 0`.
  - Add `is_transaction = (event_id >= 5)`.
  - Add `source_topic` from `_kafka_topic`.
- Write: **Delta MERGE INTO** (idempotent, no duplicates):

```python
from delta.tables import DeltaTable

delta_table.alias("t").merge(
    new_df.alias("s"),
    "t.event_uuid = s.event_uuid AND t.log_date = s.log_date"
).whenNotMatchedInsertAll().execute()
```

## Silver Layer Optimization Rules (Delta 3.2+)

### Intra-batch deduplication (mandatory)
Every silver job that reads from a Bronze partition must apply a Window dedup to remove
late/duplicate rows arriving within a single batch:

```python
from pyspark.sql import Window
import pyspark.sql.functions as f

w = Window.partitionBy(<dedup_keys>).orderBy(f.col("updated_at").desc())
df = df.withColumn("_rn", f.row_number().over(w)).filter(f.col("_rn") == 1).drop("_rn")
```

| Silver table | partitionBy keys | orderBy |
|--------------|------------------|---------|
| events | event_uuid | _ingested_at desc |
| transactions | (transaction_id, branch_id) | created_at desc (bronze snapshot version) |
| customers / products / branches | <id_col> | updated_at desc |
| nou | natural via groupBy + min(transaction_datetime) | — |

### MERGE INTO + update_on_match
Use `write_delta_merge(update_on_match=True)` for any Silver table where source rows mutate over time:
- `silver.transactions` — `order_status` flips W → I → B.
- `silver.customers` / `products` / `branches` — SCD1 dim, latest snapshot wins.

Insert-only tables keep `update_on_match=False` (default): `silver.events`, `silver.nou`.

### Change Data Feed (CDF) + Deletion Vectors
Enable both on tables that experience UPDATE traffic. Helpers in `delta_utils.py`:

```python
write_delta_merge(spark, df, path, merge_keys, partition_by, update_on_match=True,
                  table_properties={"delta.enableChangeDataFeed": "true",
                                    "delta.enableDeletionVectors": "true"})  # first run only

# subsequent runs (table already exists)
enable_cdf(spark, path)
enable_deletion_vectors(spark, path)
```

Skip CDF/DV for `silver.events` and `silver.nou` (insert-only — CDF would be empty noise).

### Delta CHECK CONSTRAINT
Apply via `ensure_constraint()` (idempotent — `ALTER TABLE ADD CONSTRAINT IF NOT EXISTS`):

```python
ensure_constraint(spark, path, "valid_order_status",
                  "order_status IN ('W','R','O','D','I','C','B','P','F')")
ensure_constraint(spark, path, "valid_channel", "channel IN ('offline','online')")
ensure_constraint(spark, path, "valid_user_type", "user_type IN (1,2,3)")
ensure_constraint(spark, path, "valid_event_id", "event_id BETWEEN 1 AND 13")
```

### Weekly OPTIMIZE / VACUUM
Run via `processing/spark_jobs/maintain_silver.py` scheduled in `dag_silver_maintenance.py` (Sun 04:00):

| Table | OPTIMIZE … ZORDER BY |
|-------|----------------------|
| silver.events | (user_id, event_id) |
| silver.transactions | (customer_id, branch_id) |
| silver.customers | customer_id |
| silver.products | product_id |
| silver.branches | branch_id |
| silver.nou | (no ZORDER, OPTIMIZE only) |

VACUUM RETAIN 168 HOURS (7 days). Increase to 720 HOURS only if Time Travel audit beyond a week is required.

## batch_gold_aggregate.py Rules

- Receives `--date YYYY-MM-DD` from Airflow. Reads Silver partition for that date.
- Produces 4 DataFrames: `funnel_hourly`, `movie_revenue_hourly`, `app_health_hourly`, `user_activity_daily`.
- Write each Gold table with `replaceWhere` overwrite (idempotent, only T-1 partition replaced):

```python
gold_df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"log_date = '{date}'") \
    .save(get_s3_path("gold", "funnel_hourly"))
```

## batch_gold_transactions.py Rules

Builds 2 Gold tables in one pass over `silver.transactions` (shares STOCK + FLOW prep stage):
- `gold.daily_customer_sales` — Sales dashboard (revenue by customer/branch/channel/payment).
- `gold.daily_logistics_aging` — Operations dashboard (aging buckets by branch/status/channel/payment).

### Status grouping (apparel retail order_status — 9 codes; F excluded from Gold)

| Group | Codes | Type | diff_date logic |
|-------|-------|------|-----------------|
| Processing | W, O, D, P | STOCK (still open) | `DATEDIFF(snapshot_date, trans_date)` |
| Recognized | I | FLOW (success) | `DATEDIFF(DATE(updated_at), trans_date)` |
| Returned | B | FLOW (return) | `DATEDIFF(DATE(updated_at), trans_date)` |
| Cancelled | C, R | FLOW (cancel) | `DATEDIFF(DATE(updated_at), trans_date)` |
| (excluded) | F | — | filter `order_status != 'F'` |

### Input scope rules
- **STOCK part**: scan **entire** silver.transactions WHERE `order_status IN ('W','O','D','P')`. Lơ lửng orders may have been created days/weeks/months ago.
- **FLOW part**: scan WHERE `DATE(updated_at) = snapshot_date AND order_status IN ('I','B','C','R')`. Only today's settled events.
- **F filter**: applied at root before STOCK/FLOW split.

### Aging buckets (Bảng 2 — mutually exclusive)

```python
f.when(f.col("diff_date") < 1,   "<1")
 .when(f.col("diff_date") <= 3,  "<=3")
 .when(f.col("diff_date") <= 7,  "<=7")
 .when(f.col("diff_date") <= 14, "<=14")
 .when(f.col("diff_date") <= 28, "<=28")
 .otherwise(">28")
```

### Multi-level partition + write
Both tables partition by `(branch_id, ym, ymd)`. Write mode is `replaceWhere` on `ymd`:

```python
write_delta_replace_partition(
    spark, df, get_s3_path("gold", "daily_customer_sales"),
    partition_col="ymd", partition_value=run_date,
    partition_by=["branch_id", "ym", "ymd"],
)
```

`silver.transactions` also partitions by `(branch_id, ym, ymd)` to align grain. MERGE keys: `(trans_id, branch_id, ym, ymd)`.

### `silver.transactions.created_at` + `updated_at` semantics
Both sourced from `bronze.created_at` (NOT `bronze.updated_at`). Bronze is append-only daily snapshots — each ingestion creates a new row with a new `created_at` (the snapshot version timestamp). SCD1 to silver picks the latest version via `Window.orderBy(created_at desc)`.

- **`created_at`**: original order creation timestamp. Written on INSERT, **preserved on UPDATE** via `update_exclude_cols=["created_at"]` in `write_delta_merge`.
- **`updated_at`**: latest bronze.created_at of that order. Refreshed on every MERGE UPDATE so it reflects the newest snapshot version. Gold uses this to compute `diff_date` for FLOW statuses.

```python
write_delta_merge(
    spark, result, SILVER_PATH,
    merge_keys=["trans_id", "branch_id", "ym", "ymd"],
    partition_by=["branch_id", "ym", "ymd"],
    update_on_match=True,
    update_exclude_cols=["created_at"],   # preserve original creation timestamp
)
```

## AWS Glue Catalog Registration

After creating a new Delta table for the first time, register it in Glue Catalog:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.gold.funnel_hourly
    USING delta
    LOCATION '{get_s3_path("gold", "funnel_hourly")}'
""")
```

## Trino SQL (DA Queries)

Trino connects to Glue Catalog via `lakehouse` catalog. Sample queries:

```sql
-- Funnel drop-off analysis (yesterday)
SELECT event_type,
       SUM(event_count) AS total_events,
       SUM(success_count) * 1.0 / SUM(event_count) AS success_rate
FROM lakehouse.gold.funnel_hourly
WHERE date_trunc('day', hour) = current_date - INTERVAL '1' DAY
GROUP BY event_type ORDER BY total_events DESC;

-- Revenue ranking (past 7 days)
SELECT movie_name,
       SUM(tickets_sold) AS tickets,
       SUM(total_revenue) AS revenue_vnd
FROM lakehouse.gold.movie_revenue_hourly
WHERE hour >= current_date - INTERVAL '7' DAY
GROUP BY movie_name ORDER BY revenue_vnd DESC;

-- Error rate by app version
SELECT app_version,
       SUM(error_count) * 1.0 / SUM(total_count) AS error_rate
FROM lakehouse.gold.app_health_hourly
WHERE event_type = 'payment_failed'
GROUP BY app_version ORDER BY error_rate DESC;
```

## Airflow DAG Rules (3 separate DAG files)

Each DAG is a separate file in `orchestration/dags/`:

| DAG | Schedule (UTC+7) | Tasks |
|-----|-------------------|-------|
| `dag_bronze_sql.py` | 02:00 | 5 MySQL->Bronze batch jobs |
| `dag_bronze_to_silver.py` | 02:30 | 1 Silver transform |
| `dag_silver_to_gold.py` | 03:00 | 2 parallel: gold events aggregate + gold transactions (sales + aging) |

- Use `LOCAL_DS` template with +7h offset for Vietnam timezone.
- Each Spark job receives the date as `sys.argv[1]` (not argparse).
- Use `create_spark_task(dag, python_file)` helper for consistent spark-submit.
- `DummyOperator` as START/END markers.

## Schema

Always import from the shared schema modules — never redefine inline:

```python
from processing.schemas.event_schema import EVENT_SCHEMA  # Kafka events
from processing.schemas.sql_schema import CUSTOMERS_SCHEMA  # MySQL tables
```
