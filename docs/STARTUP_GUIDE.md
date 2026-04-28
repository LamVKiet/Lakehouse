# Startup Guide — Observe the Full Pipeline

This guide walks you through starting the system step by step so you can observe
each tool (Kafka, Spark Streaming, Airflow, Trino) in action.

---

## Prerequisites

Fill in your AWS credentials in `.env` before starting:

```env
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-southeast-2
S3_BUCKET=projectde2026
```

---

## Step 0 — Build and Start All Services

```bash
docker compose up -d --build
```

Wait ~30 seconds for all services to initialize, then check:

```bash
docker compose ps
```

All services should show `Up` or `Up (healthy)`. If `spark-bronze-job` shows `Restarting`,
check logs: `docker compose logs spark-bronze-job --tail 30`

---

## Step 1 — Observe Kafka Cluster (3 Nodes)

**URL**: http://localhost:8080

Open Kafka UI. You should see:
- 2 brokers: `kafka-1`, `kafka-2`
- No topics yet (topics are created in Step 2)

Verify via CLI:
```bash
docker exec kafka-1 kafka-broker-api-versions \
  --bootstrap-server kafka-1:19092 | grep -i "kafka-"
```

---

## Step 2 — Create Kafka Topics (first run only)

```bash
docker exec python-producer python scripts/setup_topics.py
```

Expected output:
```
Topic 'behavior_events' created: 10 partitions, replication=2
Topic 'transaction_events' created: 3 partitions, replication=2
```

Go back to **Kafka UI → Topics**. You will now see both topics.
Click `behavior_events` → **Partitions** tab to see the 10 partitions distributed across 3 brokers.

---

## Step 3 — Observe the Data Simulator (FastAPI)

**URL**: http://localhost:8000/docs (Swagger UI)

Test the API manually:

```bash
# Health check
curl http://localhost:8000/health

# Generate a single event
curl http://localhost:8000/events/single

# Generate a batch of 5 events
curl http://localhost:8000/events/batch?count=5
```

You will see JSON events like:
```json
{
  "event_uuid": "a1b2c3...",
  "event_id": 3,
  "event_type": "view_movie_details",
  "user_id": "user_4821",
  "device_os": "iOS",
  "app_version": "1.1.5",
  "metadata": { "movie_id": "mv_042", "movie_name": "Dune Part 3" }
}
```

---

## Step 4 — Observe the Kafka Producer

The producer calls the simulator and dual-writes events to Kafka.

```bash
docker compose logs -f python-producer
```

You will see lines like:
```
Sent to behavior_events:    user_3921 | search_movie        | partition=4
Sent to behavior_events:    user_1205 | ticket_booked       | partition=7
Sent to transaction_events: user_1205 | ticket_booked       | partition=1  ← dual-write (event_id >= 5)
```

**In Kafka UI** → Topics → `behavior_events` → **Messages** tab:
- Events are flowing in real time
- Each message shows key, value (JSON), partition, offset

---

## Step 5 — Observe Spark Cluster

**Spark Master UI**: http://localhost:8081
**Spark Worker UI**: http://localhost:8082

In Spark Master UI you will see:
- 1 worker registered with 2 cores, 1GB RAM
- `spark-bronze-job` running as an active application

```bash
# See streaming job logs
docker compose logs -f spark-bronze-job
```

When the first 30-second batch fires, you will see:
```
[Batch 0] Wrote 47 rows to bronze/behavior_events
Glue table registered: bronze.behavior_events -> s3a://projectde2026/bronze/behavior_events/
[Batch 0] Wrote 3 rows to bronze/transaction_events
Glue table registered: bronze.transaction_events -> s3a://projectde2026/bronze/transaction_events/
```

**In Spark Master UI** → click on the application → **Streaming** tab to see micro-batch timing.

**On AWS S3 Console** → `projectde2026/bronze/` → files will appear partitioned by `log_date=YYYY-MM-DD/`.

---

## Step 6 — Kafka Consumers (optional, not deployed)

Consumer code exists in `ingestion/consumers/` but is **not deployed as Docker services** in the current stack.
The consumers are designed for optional real-time use cases (notification, AI recommender) and can be
run manually if needed:

```bash
# Run consumer manually inside the producer container
docker exec python-producer python ingestion/consumers/consumer_behavior.py
docker exec python-producer python ingestion/consumers/consumer_transaction.py
```

Consumer groups (when running):
- `datalake-sync` — consuming `behavior_events`
- `ai-recommender` — consuming `behavior_events`
- `notification-service` — consuming `transaction_events`
- `dwh-dashboard` — consuming `transaction_events`

---

## Step 7 — Run the Batch Pipeline (Silver + Gold) via Airflow

**URL**: http://localhost:8085
**Login**: `admin` / `admin`

The Airflow DAG `daily_silver_gold_pipeline` is scheduled at 02:00 UTC daily.
To test manually for yesterday's data:

```bash
docker exec airflow airflow dags trigger daily_silver_gold_pipeline \
  --conf '{"date":"2026-04-13"}'
```

In **Airflow UI** → DAGs → `daily_silver_gold_pipeline`:
- Click the latest run → **Graph view**
- Watch `bronze_to_silver` turn green, then `silver_to_gold` starts

```bash
# Follow Silver job logs
docker compose logs -f airflow | grep -i "silver\|gold\|spark"
```

**Silver job** output:
```
Bronze rows for 2026-04-13: 2340
Silver rows to write: 2298
Silver MERGE complete for 2026-04-13
Glue table registered: silver.events -> s3a://projectde2026/silver/events/
```

**Gold job** output:
```
Silver rows for 2026-04-13: 2298
Gold funnel_hourly written (42 rows)
Glue table registered: gold.funnel_hourly -> ...
Gold movie_revenue_hourly written (18 rows)
Glue table registered: gold.movie_revenue_hourly -> ...
Gold app_health_hourly written (56 rows)
Glue table registered: gold.app_health_hourly -> ...
Gold user_activity_daily written (310 rows)
Glue table registered: gold.user_activity_daily -> ...
```

---

## Step 8 — Verify Tables in AWS Glue

**AWS Console → Glue → Tables**

After Steps 5 and 7 complete, you will see 6 tables registered:

| Database | Table |
|----------|-------|
| bronze | behavior_events |
| bronze | transaction_events |
| silver | events |
| gold | funnel_hourly |
| gold | movie_revenue_hourly |
| gold | app_health_hourly |
| gold | user_activity_daily |

---

## Step 9 — Query via Trino (Kafka UI for Trino)

**URL**: http://localhost:8090

Or query via CLI:

```bash
# List all Gold tables
docker exec trino trino --catalog lakehouse \
  --execute "SHOW TABLES IN gold;"

# Funnel drop-off analysis
docker exec trino trino --catalog lakehouse \
  --execute "
    SELECT event_type,
           SUM(event_count) AS total,
           ROUND(SUM(success_count) * 1.0 / SUM(event_count) * 100, 1) AS success_pct
    FROM gold.funnel_hourly
    GROUP BY event_type
    ORDER BY total DESC;
  "

# Top movies by revenue
docker exec trino trino --catalog lakehouse \
  --execute "
    SELECT movie_name,
           SUM(tickets_sold) AS tickets,
           SUM(total_revenue) AS revenue_vnd
    FROM gold.movie_revenue_hourly
    GROUP BY movie_name
    ORDER BY revenue_vnd DESC
    LIMIT 5;
  "
```

---

## Step 10 — Query via DBeaver

Connect DBeaver to Trino to query Gold tables with a GUI:

| Field | Value |
|-------|-------|
| Driver | Trino |
| Host | `localhost` |
| Port | `8090` |
| Database | `lakehouse` |
| Username | `admin` |
| Password | *(leave empty)* |

After connecting, navigate: `lakehouse → gold → funnel_hourly` → right-click → **Read Data**.

---

## Step 11 — Explore via Jupyter

**URL**: http://localhost:8888
**Token**: `admin123`

Open `notebooks/01_lakehouse_exploration.ipynb` to run PySpark queries interactively
against the Delta tables on S3.

---

## Full Data Flow Summary

```
[data-simulator :8000]
        │  HTTP GET /events/single
        ▼
[python-producer]  ──────────────────────────┐
        │ 100% events                         │ event_id >= 5 only
        ▼                                     ▼
[behavior_events] (10 partitions)   [transaction_events] (3 partitions)
        │                                     │
        └──────────────┬──────────────────────┘
                       │ every 30 seconds
                       ▼
        [spark-bronze-job — Spark Streaming]
                       │ append + register Glue
                       ▼
        S3: bronze/behavior_events/log_date=.../
        S3: bronze/transaction_events/log_date=.../
                       │
              [Airflow — 02:00 daily]
                       │
              Task 1: batch_silver_transform.py
                       │ MERGE INTO + register Glue
                       ▼
              S3: silver/events/log_date=.../
                       │
              Task 2: batch_gold_aggregate.py
                       │ replaceWhere + register Glue
                       ▼
              S3: gold/funnel_hourly/year_month=.../
              S3: gold/movie_revenue_hourly/year_month=.../
              S3: gold/app_health_hourly/year_month=.../
              S3: gold/user_activity_daily/log_date=.../
                       │
              [Trino ← AWS Glue Catalog]
                       │
              DBeaver / Jupyter
```

---

## Shutdown

```bash
# Stop all services, keep volumes (data preserved)
docker compose down

# Stop and wipe everything (full reset)
docker compose down -v
```
