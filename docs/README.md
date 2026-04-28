# kafka-crash-course

Real-time Event-driven Data Pipeline for a Movie Ticketing App.

**Stack**: Kafka → Spark Streaming (Bronze) → Spark Batch T-1 (Silver/Gold) → Delta Lake on AWS S3
**Query**: Trino + AWS Glue Data Catalog
**Orchestration**: Apache Airflow

See `docs/System_Architecture_Docs.md` for full architecture documentation.

---

## Quick Start

```bash
# Copy environment file and fill in AWS credentials
cp .env.example .env

# Start all services
docker compose up --build

# Create Kafka topics (first run only)
docker exec python-producer python scripts/setup_topics.py

# Verify simulator is running
curl http://localhost:8000/health
```

## Useful Kafka Commands

```bash
# List topics
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:19092

# Describe a topic and its partitions
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:19092 \
  --describe --topic behavior_events

# Consume events from beginning (manual check)
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:19092 \
  --topic behavior_events \
  --from-beginning \
  --max-messages 10
```

## Service URLs

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Spark Master | http://localhost:8081 |
| Spark App (Bronze) | http://localhost:4040 |
| Airflow | http://localhost:8085 |
| Jupyter Lab | http://localhost:8888 (token: admin123) |
| Trino | http://localhost:8090 |
| data-simulator | http://localhost:8000/docs |
