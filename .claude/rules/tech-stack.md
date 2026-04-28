---
description: "Full tech stack and versions for the Medallion Architecture pipeline"
---

# Tech Stack

## Core Services

| Layer | Tool | Version | Purpose |
|-------|------|---------|---------|
| Message Broker | Apache Kafka (KRaft) | 7.8.3 (Confluent) | 2-node cluster, event streaming backbone |
| Stream Processing | Apache Spark | 3.4.1 | Structured Streaming (Bronze) + Batch (Silver/Gold) |
| Lakehouse Storage | Delta Lake | 3.2.0 | ACID transactions, time travel, CDF, Deletion Vectors on S3 |
| Object Storage | AWS S3 | — | Stores all Delta tables (Bronze/Silver/Gold) |
| Metadata Catalog | AWS Glue Data Catalog | — | Managed Hive Metastore — DA queries via Trino |
| Query Engine | Trino | 435 | Distributed SQL over Delta tables via Glue Catalog |
| Orchestration | Apache Airflow | 2.9 (slim) | Schedules batch pipeline (Silver + Gold) at 02:00 daily |
| Event Simulator | FastAPI | latest | Generates fake movie ticketing events via HTTP API |
| Language | Python | 3.10 (Kafka apps, simulator), 3.12 (Spark) | Producer, consumers, Spark jobs |
| Kafka Client | confluent-kafka | latest | Python Kafka producer/consumer |
| Notebook | JupyterLab | latest | Interactive PySpark + Trino SQL exploration |
| Monitoring | Kafka UI | latest | Web UI for Kafka cluster management |
| Containerization | Docker Compose | latest | Orchestrates all local services |

## Spark Jars Required (in Dockerfile.spark)

| Jar | Version | Purpose |
|-----|---------|---------|
| spark-sql-kafka-0-10_2.12 | 3.4.1 | Kafka source for Structured Streaming |
| delta-spark_2.12 | 3.2.0 | Delta Lake table format (renamed from delta-core in 3.0+) |
| delta-storage | 3.2.0 | Delta Lake storage layer |
| hadoop-aws | 3.3.4 | S3A filesystem connector |
| aws-java-sdk-bundle | 1.12.262 | AWS SDK for S3 access |

## Python Packages

| Package | Environment | Purpose |
|---------|-------------|---------|
| confluent-kafka | Python 3.10 (Dockerfile) | Kafka producer/consumer |
| fastapi + uvicorn | Python 3.10 (Dockerfile) | data-simulator HTTP API |
| delta-spark==3.2.0 | Python 3.12 (Dockerfile.spark) | Delta Lake Python API |
| apache-airflow-providers-apache-spark | Airflow image | SparkSubmitOperator |

## Removed from Stack
- ~~ClickHouse~~ — replaced by Delta Lake + Trino query layer
- ~~MySQL~~ — no transactional DB needed in new architecture
- ~~clickhouse-jdbc / clickhouse-connect~~ — no longer required
