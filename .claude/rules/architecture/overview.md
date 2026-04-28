---
description: "System overview — Medallion data flow diagram, Kafka layer, event schema (14 types), traffic ratio"
---

# Architecture Overview

## Pattern: Medallion Architecture (Bronze / Silver / Gold)

```
[FastAPI data-simulator :8000]          [MySQL — backend operational data]
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
[streaming_to_bronze.py]            [batch_bronze_sql.py]
  always-on, trigger 30s              T-1 batch (02:00 daily)
        |                                       |
        v                                       v
AWS S3 / Delta Lake — BRONZE
  s3://{BUCKET}/bronze/behavior_events/         (partitioned by log_date)
  s3://{BUCKET}/bronze/customers/               (partitioned by report_date)
  s3://{BUCKET}/bronze/products/                (partitioned by report_date)
  s3://{BUCKET}/bronze/branches/                (partitioned by report_date)
  s3://{BUCKET}/bronze/transactions/            (partitioned by report_date)
  s3://{BUCKET}/bronze/transaction_details/     (partitioned by report_date)
        |
        v  [Airflow DAG — Task 2: bronze_to_silver_daily]
[batch_silver_events_discovery.py] -- Bronze behavior_events (event_id 1-4)  -> silver.events_discovery
[batch_silver_events_cart.py]      -- Bronze behavior_events (event_id 5-8)  -> silver.events_cart
[batch_silver_events_checkout.py]  -- Bronze behavior_events (event_id 9-14) -> silver.events_checkout
[batch_silver_nou.py]        -- Bronze transactions -> silver.nou (MERGE new customers)
        |                       ↓ (dependency)
[batch_silver_transactions.py] -- Bronze transactions + silver.nou -> silver.transactions
        |
        v
AWS S3 / Delta Lake — SILVER
  s3://{BUCKET}/silver/events_discovery/    (partitioned by log_date — Discovery & Product)
  s3://{BUCKET}/silver/events_cart/         (partitioned by log_date — Cart Activities)
  s3://{BUCKET}/silver/events_checkout/     (partitioned by log_date — Checkout & Payments)
  s3://{BUCKET}/silver/nou/                 (partitioned by ym — customer first-order registry)
  s3://{BUCKET}/silver/transactions/        (partitioned by branch_id/ym/ymd, CDF + Deletion Vectors enabled)
  s3://{BUCKET}/silver/customers/           (partitioned by source — SCD1 dim, CDF + DV)
  s3://{BUCKET}/silver/products/            (partitioned by sales_unit — SCD1 dim, CDF + DV)
  s3://{BUCKET}/silver/branches/            (partitioned by region — SCD1 dim, CDF + DV)
        |
        v  [Airflow DAG — Task 3]
[batch_gold_events_discovery.py] -- Silver events_discovery -> Gold
[batch_gold_events_cart.py]      -- Silver events_cart -> Gold
[batch_gold_events_checkout.py]  -- Silver events_checkout -> Gold
[batch_gold_transactions.py]     -- Silver transactions (STOCK + FLOW) -> Gold
        |
        v
AWS S3 / Delta Lake — GOLD
  s3://{BUCKET}/gold/events_discovery/          (partitioned by log_date)
  s3://{BUCKET}/gold/events_cart/               (partitioned by log_date)
  s3://{BUCKET}/gold/events_checkout/           (partitioned by log_date)
  s3://{BUCKET}/gold/daily_customer_sales/      (partitioned by branch_id/ym/ymd)
  s3://{BUCKET}/gold/daily_logistics_aging/     (partitioned by branch_id/ym/ymd)
        |
        v
[Trino] <-- [AWS Glue Data Catalog]
        |
        v
DA -> Jupyter / SQL client
```

## Kafka Layer

- Only `behavior_events` topic is consumed by Spark Streaming (Bronze).
- `transaction_events` topic removed — transaction data now sourced from MySQL.

## Kafka Consumer Groups (optional real-time use cases)

### behavior_events
- `datalake-sync`: batch 20 events -> S3 write (superseded by Spark Streaming in new stack).
- `ai-recommender`: real-time recommendation / push notification.

### transaction_events
- `notification-service`: SMS/Email on payment events.
- `dwh-dashboard`: aggregate revenue real-time.

## Event Schema (JSON produced by data-simulator)

14 event types representing the full e-commerce user journey on an apparel retail Web/App:

| event_id | event_type | Description |
|----------|-----------|-------------|
| 1 | home_screen_view | User opens app home screen |
| 2 | search | User searches for products by keyword |
| 3 | view_item | User views product detail page |
| 4 | select_item_variant | User selects color/size variant |
| 5 | add_to_cart | User adds item to cart |
| 6 | view_cart | User opens cart screen |
| 7 | remove_from_cart | User removes item from cart |
| 8 | update_cart_item | User changes qty/variant in cart |
| 9 | begin_checkout | User starts checkout flow |
| 10 | add_shipping_info | User fills shipping address |
| 11 | add_coupon | User applies promotion code |
| 12 | add_payment_info | User selects payment method |
| 13 | place_order | User submits order |
| 14 | payment_callback | Payment gateway result callback |

```json
{
  "event_uuid": "uuid4",
  "event_id": "1-14",
  "event_type": "home_screen_view|search|view_item|select_item_variant|add_to_cart|view_cart|remove_from_cart|update_cart_item|begin_checkout|add_shipping_info|add_coupon|add_payment_info|place_order|payment_callback",
  "timestamp": 1234567890,
  "log_date": "YYYY-MM-DD",
  "created_at": "ISO-8601",
  "user_id": "user_XXXX",
  "device_os": "iOS|Android",
  "app_version": "1.0.0|1.1.0|1.1.5|2.0.0-beta",
  "metadata": { ... }
}
```

### Metadata per event type

| event_type | metadata fields |
|-----------|----------------|
| home_screen_view | app_version |
| search | search_keyword, result_count |
| view_item | product_id, product_name, base_price |
| select_item_variant | product_id, variant_type, variant_value |
| add_to_cart | product_id, product_name, variant_color, variant_size, base_price, quantity, cart_total_value |
| view_cart | total_items, cart_total_value |
| remove_from_cart | product_id, product_name, variant_color, variant_size, removed_quantity, cart_total_value |
| update_cart_item | product_id, action_type, old_quantity, new_quantity, cart_total_value |
| begin_checkout | total_items, cart_total_value, items_list |
| add_shipping_info | shipping_method, shipping_fee, city_province |
| add_coupon | promotion_id, promotion_type, discount_amount, is_valid, error_message |
| add_payment_info | payment_method, final_amount |
| place_order | order_id, payment_method, final_amount |
| payment_callback | order_id, transaction_id, payment_method, is_success, error_code |

## Traffic Ratio (weighted funnel distribution)
- ~73% upper funnel: home_screen_view (23%), search (19%), view_item (19%), select_item_variant (12%)
- ~15% mid funnel: add_to_cart (8%), view_cart (5%), remove_from_cart (1%), update_cart_item (1%)
- ~12% lower funnel: begin_checkout (4%), add_shipping_info (2%), add_coupon (1%), add_payment_info (1%), place_order (1%), payment_callback (1%)
