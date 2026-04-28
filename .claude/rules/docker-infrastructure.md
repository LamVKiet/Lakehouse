---
description: "Docker services, ports, Dockerfile strategy for Medallion Architecture stack"
paths:
  - "docker-compose.yaml"
  - "Dockerfile"
  - "Dockerfile.*"
  - ".env"
---

# Docker Infrastructure

## Dockerfile Strategy

Two Dockerfiles, differentiated by dependency set:

- `Dockerfile` — Kafka apps (producer, consumers) + FastAPI simulator. Lightweight (~150MB): Python 3.10 + confluent-kafka + fastapi + uvicorn. No Java or Spark needed.
- `Dockerfile.spark` — Spark Master, Worker, Jupyter, Airflow, Spark jobs. Heavy (~1.5GB): Python 3.12 + Java 17 + Spark 3.4.1 + Delta Lake jars + hadoop-aws + aws-java-sdk-bundle. Role is determined by the `command` field in docker-compose.

Do NOT merge the two Dockerfiles. Kafka apps do not need Java/Spark. Building them together wastes image size and build time.

## Services & Ports

| Service | Container | Port | URL |
|---------|-----------|------|-----|
| Kafka Node 1 | kafka-1 | 9092 (ext), 19092 (int) | — |
| Kafka Node 2 | kafka-2 | 9094 (ext), 19094 (int) | — |
| Kafka UI | kafka-ui | 8080 | localhost:8080 |
| Spark Master | spark-master | 8081 (UI), 7077 (RPC) | localhost:8081 |
| Spark Bronze Job | spark-bronze-job | 4040 (App UI) | localhost:4040 |
| Spark Worker | spark-worker-1 | 8082 | localhost:8082 |
| Jupyter Lab | jupyter-lab | 8888 | localhost:8888 (token: admin123) |
| **data-simulator** | data-simulator | **8000** | localhost:8000 (FastAPI /docs) |
| **Trino** | trino | **8090** | localhost:8090 |
| **Airflow** | airflow | **8085** | localhost:8085 |
| **MySQL** | mysql | **3306** | localhost:3306 (backend raw SQL layer) |

## Services Removed from Previous Stack

| Service | Reason |
|---------|--------|
| ~~clickhouse~~ | Replaced by Delta Lake + Trino query layer |

## Services Added / Renamed

| Old Name | New Name | Change |
|----------|----------|--------|
| spark-streaming-job | spark-bronze-job | Renamed — only writes Bronze layer now |
| — | data-simulator | New — FastAPI fake event generator |
| — | trino | New — distributed SQL query engine |
| — | airflow | New — batch pipeline orchestration |
| — | mysql | New — backend operational data (raw SQL layer) |

## Kafka Internal vs External Listeners

- Services inside Docker network use INTERNAL listener (e.g., `kafka-1:19092`).
- Host machine uses EXTERNAL listener (e.g., `localhost:9092`).
- `KAFKA_BOOTSTRAP_SERVERS` env var in docker-compose always points to INTERNAL ports.

## Named Volumes

```yaml
volumes:
  kafka_1_data:
  kafka_2_data:
  spark_checkpoints:    # Bronze streaming checkpoint — must be persistent across restarts
  airflow_logs:
  mysql_data:           # MySQL backend data — persists across restarts
```

`spark_checkpoints` is critical — losing it forces Spark Streaming to re-read Kafka from the beginning, causing duplicate Bronze writes.

## Key Environment Variables (.env)

```env
# Kafka
KAFKA_VERSION=7.8.3
CLUSTER_ID=1L6g7nGhU-eAKfL--X25wo

# AWS (required for S3 + Glue)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-southeast-1
S3_BUCKET=movie-ticketing-lakehouse

# Jupyter
JUPYTER_TOKEN=admin123

# Simulator
SIMULATOR_PORT=8000

# MySQL (backend raw SQL layer)
MYSQL_DB=movie_ticketing
MYSQL_USER=app_user
MYSQL_PASSWORD=app_pass
```
