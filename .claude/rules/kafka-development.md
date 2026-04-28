---
description: "Rules for writing Kafka producer/consumer code"
paths:
  - "ingestion/**"
  - "scripts/**"
---

# Kafka Development Rules

## Producer Config
- `acks=all` + `retries=5` — guarantees no data loss.
- Use `os.getenv("KAFKA_BOOTSTRAP_SERVERS")` to get broker address, fallback to localhost ports.
- Message key = `user_id` (encoded UTF-8) — Kafka hashes to the same partition — strict ordering per user.
- Producer calls `http://data-simulator:8000/events/single` to get event data. Do NOT generate events inline inside producer.py.

## Topic Routing
- ALL events go to `behavior_events` (full clickstream funnel, 13 event types).
- `transaction_events` topic removed — transaction/order data is sourced from MySQL via batch ETL.

## Consumer Pattern
- Each consumer file uses multi-threading (`threading.Thread`) to run multiple consumer groups in parallel within one process.
- Each thread has its own `group.id` so Kafka distributes partitions independently.
- `auto.offset.reset = "earliest"` to avoid missing data when a new consumer joins.

## Topic Config
- `behavior_events`: 10 partitions, replication_factor=2 (high volume, scale throughput).

## Notes
- Call `producer.flush()` every 100 messages to avoid buffer overflow.
- Consumers use `poll(1.0)` with a 1-second timeout.

## Event Types (Apparel Retail Clickstream)

| event_id | event_type | Funnel stage |
|----------|-----------|-------------|
| 1 | search | Upper |
| 2 | view_item | Upper |
| 3 | select_item_variant | Upper |
| 4 | add_to_cart | Mid |
| 5 | view_cart | Mid |
| 6 | remove_from_cart | Mid (negative signal) |
| 7 | update_cart_item | Mid |
| 8 | begin_checkout | Lower |
| 9 | add_shipping_info | Lower |
| 10 | add_coupon | Lower |
| 11 | add_payment_info | Lower |
| 12 | place_order | Lower |
| 13 | payment_callback | Lower (system) |

## data-simulator API Contract
- `GET /events/single` — returns one random event JSON.
- `GET /events/batch?count=100` — returns a list of N events.
- `POST /events/scenario` — body: `{event_type, count, error_rate}` for controlled scenarios.
- `GET /health` — health check.
- Producer always calls `/events/single` in a loop (not `/batch`) to maintain real-time throughput simulation.
