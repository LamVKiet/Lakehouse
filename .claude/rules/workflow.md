---
description: "Startup steps, operation commands, and debug guide for the Medallion stack"
---

# Development Workflow

## Directory Structure

```
kafka-crash-course/
├── common/
│   └── kafka_config.py                # shared Kafka connection config (bootstrap servers, producer/consumer configs)
│
├── ingestion/
│   ├── producer/
│   │   └── movie_events_producer.py   # calls data-simulator API, sends to Kafka
│   ├── consumers/                     # optional real-time consumers (not deployed as Docker services)
│   │   ├── consumer_behavior.py
│   │   └── consumer_transaction.py
│   └── simulator/
│       ├── main.py                    # FastAPI app (:8000)
│       └── event_generator.py         # fake event logic
│
├── processing/
│   ├── spark_jobs/
│   │   ├── streaming_to_bronze.py              # [STREAMING] Kafka -> Bronze (behavior_events only)
│   │   ├── batch_bronze_customers.py           # [BATCH T-1] MySQL -> bronze.customers
│   │   ├── batch_bronze_category.py            # [BATCH T-1] MySQL -> bronze.category (no partition)
│   │   ├── batch_bronze_products.py            # [BATCH T-1] MySQL -> bronze.products (SCD2)
│   │   ├── batch_bronze_branches.py            # [BATCH T-1] MySQL -> bronze.branches (SCD2)
│   │   ├── batch_bronze_transactions.py        # [BATCH T-1] MySQL -> bronze.transactions
│   │   ├── batch_bronze_transaction_details.py # [BATCH T-1] MySQL -> bronze.transaction_details
│   │   ├── batch_silver_customers.py            # [BATCH T-1] Bronze -> silver.customers (SCD1)
│   │   ├── batch_silver_category.py             # [BATCH T-1] Bronze -> silver.category (SCD1, no partition)
│   │   ├── batch_silver_products.py             # [BATCH T-1] Bronze + silver.category -> silver.products (SCD1)
│   │   ├── batch_silver_branches.py             # [BATCH T-1] Bronze -> silver.branches (SCD1)
│   │   ├── batch_silver_nou.py                  # [BATCH T-1] Bronze -> silver.nou (MERGE new customers)
│   │   ├── batch_silver_nou_backfill.py         # [BACKFILL]  Bronze -> silver.nou (all history, run once)
│   │   ├── batch_silver_transactions.py         # [BATCH T-1] Bronze + silver.nou -> silver.transactions
│   │   ├── batch_silver_transactions_backfill.py # [BACKFILL] Bronze + silver.nou -> silver.transactions (run once)
│   │   ├── batch_silver_events_discovery.py    # [BATCH T-1] Bronze -> silver.events_discovery (event_id 1-4)
│   │   ├── batch_silver_events_cart.py         # [BATCH T-1] Bronze -> silver.events_cart (event_id 5-8)
│   │   ├── batch_silver_events_checkout.py     # [BATCH T-1] Bronze -> silver.events_checkout (event_id 9-14)
│   │   ├── batch_gold_events_discovery.py      # [BATCH T-1] Silver events_discovery -> Gold
│   │   ├── batch_gold_events_cart.py           # [BATCH T-1] Silver events_cart -> Gold
│   │   ├── batch_gold_events_checkout.py       # [BATCH T-1] Silver events_checkout -> Gold
│   │   └── delta_utils.py                      # Spark + S3/Delta + MySQL JDBC shared utils
│   └── schemas/
│       ├── event_schema.py            # Kafka event StructType
│       └── sql_schema.py             # MySQL tables StructType
│
├── orchestration/
│   └── dags/
│       ├── dag_bronze_sql.py          # DAG: MySQL -> Bronze (02:00, 5 tasks)
│       ├── dag_bronze_to_silver.py    # DAG: Bronze -> Silver (02:30, nou >> transactions)
│       ├── dag_backfill_silver.py     # DAG: Backfill Silver (manual trigger, run once)
│       └── dag_silver_to_gold.py      # DAG: Silver -> Gold (03:00, 4 tasks: events_discovery, events_cart, events_checkout, transactions)
│
├── infra/
│   ├── mysql/
│   │   ├── init.sql                   # DDL for 7 MySQL tables
│   │   └── seed_data.py              # seed fake data (run once)
│   ├── trino/
│   │   ├── config.properties
│   │   ├── node.properties
│   │   └── catalog/lakehouse.properties  # Delta connector -> Glue -> S3
│   └── airflow/
│       └── requirements.txt           # apache-airflow-providers-apache-spark
│
├── analytics/
│   └── queries/
│       ├── funnel_analysis.sql
│       ├── revenue_ranking.sql
│       └── app_health.sql
│
├── scripts/
│   └── setup_topics.py                # create Kafka topics (run once)
│
├── notebooks/
│   └── 01_lakehouse_exploration.ipynb
│
├── docker-compose.yaml
├── Dockerfile                         # Python 3.10 — producer, simulator, consumers
├── Dockerfile.spark                   # Python 3.12 + Java 17 + Spark + Delta jars
├── .env
└── .env.example
```

## Environment Setup

```bash
# Copy .env.example and fill in AWS credentials
cp .env.example .env

# Required .env values:
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET
# AIRFLOW__CORE__FERNET_KEY (generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
```

## Startup

```bash
# 1. Start all infrastructure
docker compose up --build

# 2. Create Kafka topics (run once on first launch)
docker exec python-producer python scripts/setup_topics.py

# 3. Verify data-simulator is running
curl http://localhost:8000/health
curl http://localhost:8000/events/single

# 4. Stop
docker compose down

# 5. Stop and wipe all volumes (full reset)
docker compose down -v
```

## Service URLs

| Service | URL | Notes |
|---------|-----|-------|
| Kafka UI | localhost:8080 | Topic/consumer group monitoring |
| Spark Master UI | localhost:8081 | Job status, workers |
| Spark Worker UI | localhost:8082 | Worker resources |
| Spark App UI | localhost:4040 | Active job DAG |
| Jupyter Lab | localhost:8888 | token: admin123 |
| Airflow UI | localhost:8085 | DAG scheduling |
| Trino UI | localhost:8090 | Query history |
| data-simulator | localhost:8000 | /docs for Swagger |

## Debug

```bash
# View logs of a specific service
docker compose logs -f python-producer
docker compose logs -f spark-bronze-job
docker compose logs -f airflow

# Shell into containers
docker exec -it kafka-1 bash
docker exec -it spark-master bash

# List Kafka topics
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:19092

# Describe a topic
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:19092 --describe --topic behavior_events

# Consume from beginning (manual verification)
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:19092 \
  --topic behavior_events \
  --from-beginning \
  --max-messages 5

# Check S3 Bronze landing
aws s3 ls s3://${S3_BUCKET}/bronze/behavior_events/ --recursive | head -20

# Trigger Airflow DAG manually (Silver + Gold)
docker exec airflow airflow dags trigger daily_silver_gold_pipeline \
  --conf '{"date":"2026-04-12"}'

# Trino SQL — verify Silver dedup
docker exec trino trino --catalog lakehouse --schema silver \
  --execute "SELECT COUNT(*) FROM events WHERE log_date = DATE '2026-04-12';"

# Trino SQL — check duplicate events
docker exec trino trino --catalog lakehouse --schema silver \
  --execute "SELECT event_uuid, COUNT(*) AS cnt FROM events GROUP BY event_uuid HAVING cnt > 1;"
```

## Batch Pipeline Re-run

If the Airflow DAG fails, re-run it safely — both Silver (Delta MERGE INTO) and Gold (replaceWhere overwrite) are idempotent:

```bash
# Re-trigger for a specific date
docker exec airflow airflow dags trigger daily_silver_gold_pipeline \
  --conf '{"date":"2026-04-12"}'
```
