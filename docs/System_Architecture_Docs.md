# System Architecture — Omnichannel Apparel Retail Data Lakehouse

## 1. System Concept

This system builds a **Real-time Event-driven Data Lakehouse** for an omnichannel fashion retail chain. Data flows from two primary sources:

- **Frontend Clickstream (Streaming)**: User behavior events on Web/App are captured and streamed through Apache Kafka in real-time.
- **Backend ERP/Sales (Batch)**: Transactional and master data (customers, products, branches, orders) are stored in MySQL and ingested daily via Spark batch ETL.

The architecture follows the **Medallion pattern** (Bronze / Silver / Gold) layered on top of Delta Lake on AWS S3:

- **Streaming layer** (always-on): Kafka → Spark Structured Streaming → Bronze Delta tables on S3.
- **Batch layer** (daily at 02:00): Airflow triggers Spark Batch jobs: MySQL → Bronze, then Bronze → Silver → Gold.
- **Query layer**: Trino + AWS Glue Data Catalog. Data Analysts query Gold/Silver tables directly with SQL.

---

## 2. Business Value

### 2.1. Cart Abandonment & Conversion Analysis
By capturing the full 13-step user journey (search → view → add_to_cart → checkout → place_order → payment_callback), the system enables precise funnel analysis — identifying where users drop off and which steps have the highest friction.

### 2.2. Omnichannel Performance
Combining POS (in-store) and Online transaction data in a unified Bronze/Silver layer allows direct comparison of channel performance: revenue, order volume, customer acquisition, and return rates.

### 2.3. Customer Lifecycle Segmentation
Silver layer classifies customers into NOU (New), Retention, and Resurrected segments based on order history — powering targeted marketing and retention campaigns.

### 2.4. Executive Dashboard (T-1 Reporting)
Gold tables aggregated by Airflow each morning at 02:00 give management a complete view of yesterday's performance — ready before business hours start.

### 2.5. Analyst Self-service
Data Analysts query Gold/Silver tables directly through Trino using standard SQL — no Spark knowledge required. AWS Glue Catalog provides a single schema registry across all layers.

---

## 3. Architecture Diagram

```
[FastAPI data-simulator :8000]          [MySQL — backend ERP/Sales data]
        |                                       |
        | HTTP GET /events/single               | (customers, products, branches,
        v                                       |  pos/online transactions & details)
[producer.py]  (ingestion/producer/)            |
        |                                       |
        |-- 100% events --> [behavior_events]   |
        v  (10 partitions, replication=2)       |
[Kafka Cluster — 2 nodes KRaft]                 |
        |                                       |
        v                                       v  [Airflow DAG — Task 1]
[streaming_to_bronze.py]            [batch_bronze_*.py (5 jobs)]
  always-on, trigger 30s              T-1 batch (02:00 daily)
        |                                       |
        v                                       v
AWS S3 / Delta Lake — BRONZE
  s3://bucket/bronze/behavior_events/           (partitioned by log_date)
  s3://bucket/bronze/customers/                 (partitioned by report_date)
  s3://bucket/bronze/products/                  (partitioned by report_date)
  s3://bucket/bronze/branches/                  (partitioned by report_date)
  s3://bucket/bronze/transactions/              (partitioned by report_date)
  s3://bucket/bronze/transaction_details/       (partitioned by report_date)
        |
        v  [Airflow DAG — Task 2]
[batch_silver_*.py]  -- Bronze → Silver (dedup, enrich, classify)
        |
        v
AWS S3 / Delta Lake — SILVER
  s3://bucket/silver/events/                    (partitioned by log_date)
  s3://bucket/silver/nou/                       (partitioned by ym)
  s3://bucket/silver/transactions/              (partitioned by ym)
        |
        v  [Airflow DAG — Task 3]
[batch_gold_aggregate.py]  -- Silver → Gold (pre-aggregation)
        |
        v
AWS S3 / Delta Lake — GOLD
  s3://bucket/gold/funnel_hourly/               (partitioned by year_month)
  s3://bucket/gold/movie_revenue_hourly/        (partitioned by year_month)
  s3://bucket/gold/app_health_hourly/           (partitioned by year_month)
  s3://bucket/gold/user_activity_daily/         (partitioned by log_date)
        |
        v
[Trino :8090] <-- [AWS Glue Data Catalog]
        |
        v
DA -> Jupyter (:8888) / SQL client
```

---

## 4. Clickstream Event Types

All 13 event types are published to a single `behavior_events` Kafka topic. Transaction data is sourced from MySQL, not Kafka.

| event_id | event_type | Funnel | Description |
|----------|-----------|--------|-------------|
| 1 | search | Upper | User searches by keyword |
| 2 | view_item | Upper | User views product detail |
| 3 | select_item_variant | Upper | User selects color/size |
| 4 | add_to_cart | Mid | User adds item to cart |
| 5 | view_cart | Mid | User opens cart screen |
| 6 | remove_from_cart | Mid | User removes item (negative signal) |
| 7 | update_cart_item | Mid | User changes qty/variant in cart |
| 8 | begin_checkout | Lower | User starts checkout |
| 9 | add_shipping_info | Lower | User fills shipping info |
| 10 | add_coupon | Lower | User applies promo code |
| 11 | add_payment_info | Lower | User selects payment method |
| 12 | place_order | Lower | User submits order |
| 13 | payment_callback | Lower | Payment gateway result |

Strict ordering per user is guaranteed by setting `message key = user_id` — Kafka always routes the same user to the same partition.

---

## 5. Medallion Layer Details

### Bronze — Raw Landing Zone
- **Clickstream**: Written by Spark Structured Streaming (30s trigger, foreachBatch, append-only).
  - `raw_metadata` stores the original `metadata` JSON as a string — never flattened.
  - `_kafka_offset` enables targeted reprocessing.
- **MySQL batch**: Written by Spark Batch (Airflow, 02:00 daily, T-1 day).
  - 5 tables: customers, products, branches, transactions, transaction_details.
  - Append-only, incremental ingestion via `DATE(COALESCE(updated_at, created_at)) = ymd`.

### Silver — Cleaned & Deduplicated
- Written by Spark Batch (Airflow, 02:30 daily, T-1 partition).
- Deduplication via **Delta MERGE INTO** on merge key + partition key.
- Customer lifecycle classification: NOU (1), Retention (2), Resurrected (3).
- Smart Key pattern for generated IDs: `{ymd}_{entity_id}_{sequence}`.

### Gold — Pre-aggregated for DA
- Written by Spark Batch (Airflow, 03:00 daily, after Silver completes).
- Write strategy: `replaceWhere` overwrite on T-1 partition — safe, idempotent.
- Target: dashboard queries complete in under 3 seconds with zero runtime joins.

---

## 6. MySQL Backend Tables

| Table | Type | Key fields |
|-------|------|-----------|
| customers | Dimension | customer_id, customer_name, registered_datetime, source |
| products | Dimension (SCD2) | product_id, product_name, product_display_name, sales_unit |
| branches | Dimension (SCD2) | branch_id, branch_name, region, city_province, status |
| pos_transactions | Fact | transaction_id, branch_id, customer_id, order_status, delivery_date |
| pos_transaction_details | Fact | transaction_detail_id, product_id, variant_size, variant_color, trans_qty |
| online_transactions | Fact | transaction_id, branch_id, customer_id, order_status, delivery_date |
| online_transaction_details | Fact | transaction_detail_id, product_id, variant_size, variant_color, trans_qty |

---

## 7. Service Roles

| Service | Role |
|---------|------|
| `data-simulator` (FastAPI) | Generates realistic fake clickstream events for apparel retail via HTTP API |
| `python-producer` | Calls simulator API, publishes events to Kafka `behavior_events` topic |
| `kafka-1/2` | 2-node Kafka cluster in KRaft mode (no Zookeeper) |
| `kafka-ui` | Web UI for topic and consumer group monitoring |
| `spark-master` + `spark-worker-1` | Spark cluster for all processing jobs |
| `spark-bronze-job` | Runs `streaming_to_bronze.py` — always-on streaming to S3 Bronze |
| `mysql` | Backend operational database (ERP/Sales data) |
| `airflow` | Schedules and monitors daily batch pipeline (Bronze/Silver/Gold) |
| `trino` | Distributed SQL query engine for DA — queries Delta tables via Glue |
| `jupyter-lab` | Interactive exploration with PySpark + Trino SQL |

---

## 8. Strengths & Weaknesses

### Strengths
1. **Dual-source Architecture**: Combines real-time clickstream with batch ERP data for complete business view.
2. **Scalability**: 2-node KRaft Kafka cluster handles tens of thousands of messages/second without data loss.
3. **Strict Ordering**: `user_id` as Kafka message key guarantees all events for a user land in the same partition in order.
4. **Idempotent Batch**: Both Silver (MERGE INTO) and Gold (replaceWhere) batch jobs are safe to re-run — no duplicates, no data loss.
5. **Separation of Concerns**: Streaming (Bronze, always-on) and Batch (Silver/Gold, Airflow-scheduled) have independent failure domains.
6. **Analyst Self-service**: DA queries Gold/Silver with standard SQL via Trino — no Spark required.

### Weaknesses
1. **Operational Complexity**: Requires understanding of Kafka partitions, Delta Lake checkpointing, Airflow DAG dependencies, and AWS Glue schema evolution.
2. **T-1 Latency on Gold**: DA can only query fully processed data from yesterday. Real-time Gold queries are not supported in this design.
3. **At-least-once Delivery**: Kafka may re-deliver messages on network lag. Bronze may have duplicates — Silver's MERGE INTO deduplication is the safety net.
4. **AWS Cost**: S3 object storage + Glue API calls + Spark compute all incur AWS costs in production.

---

## 9. Code File Guide

| File | Role |
|------|------|
| `ingestion/simulator/main.py` | FastAPI app — exposes `/events/single`, `/events/batch`, `/events/scenario`, `/health` |
| `ingestion/simulator/event_generator.py` | Fake clickstream event generation (13 apparel retail event types) |
| `ingestion/producer/movie_events_producer.py` | Calls simulator API, publishes to Kafka `behavior_events` |
| `ingestion/consumers/consumer_behavior.py` | Multi-threaded consumer for `behavior_events` — not deployed as Docker service |
| `common/kafka_config.py` | Shared Kafka connection config (bootstrap servers, producer/consumer configs) |
| `processing/schemas/event_schema.py` | Shared Spark StructType for clickstream events |
| `processing/schemas/sql_schema.py` | Shared Spark StructType for MySQL tables |
| `processing/spark_jobs/delta_utils.py` | Spark session factory with Delta + S3 + Glue + MySQL JDBC utils |
| `processing/spark_jobs/streaming_to_bronze.py` | Spark Structured Streaming — Kafka → S3 Bronze (30s trigger) |
| `processing/spark_jobs/batch_bronze_*.py` | Spark Batch — MySQL → S3 Bronze (5 tables, T-1) |
| `processing/spark_jobs/batch_silver_*.py` | Spark Batch — Bronze → Silver (MERGE, dedup, enrich) |
| `processing/spark_jobs/batch_gold_aggregate.py` | Spark Batch — Silver → Gold (4 aggregations, replaceWhere) |
| `orchestration/dags/` | Airflow DAGs — 3 separate DAGs for Bronze/Silver/Gold |
| `infra/mysql/init.sql` | DDL for 7 MySQL tables (apparel retail) |
| `infra/mysql/seed_data.py` | Seed fake data into MySQL |
| `infra/trino/catalog/lakehouse.properties` | Trino Delta connector → Glue → S3 |
| `scripts/setup_topics.py` | Creates Kafka topics (run once) |
| `analytics/queries/` | Sample Trino SQL queries for DA |
