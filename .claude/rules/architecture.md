---
description: "Medallion Architecture — Bronze/Silver/Gold on Delta Lake + S3, event schema, data flow"
---

# System Architecture

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
[batch_silver_transform.py]  -- reads T-1 Bronze behavior_events
[batch_silver_nou.py]        -- Bronze transactions -> silver.nou (MERGE new customers)
        |                       ↓ (dependency)
[batch_silver_transactions.py] -- Bronze transactions + silver.nou -> silver.transactions
        |
        v
AWS S3 / Delta Lake — SILVER
  s3://{BUCKET}/silver/events/              (partitioned by log_date)
  s3://{BUCKET}/silver/nou/                 (partitioned by ym — customer first-order registry)
  s3://{BUCKET}/silver/transactions/        (partitioned by branch_id/ym/ymd, CDF + Deletion Vectors enabled)
  s3://{BUCKET}/silver/customers/           (partitioned by source — SCD1 dim, CDF + DV)
  s3://{BUCKET}/silver/products/            (partitioned by sales_unit — SCD1 dim, CDF + DV)
  s3://{BUCKET}/silver/branches/            (partitioned by region — SCD1 dim, CDF + DV)
        |
        v  [Airflow DAG — Task 3]
[batch_gold_aggregate.py]    -- reads T-1 Silver partition
[batch_gold_transactions.py] -- reads silver.transactions (STOCK + FLOW)
        |
        v
AWS S3 / Delta Lake — GOLD
  s3://{BUCKET}/gold/funnel_hourly/             (partitioned by year_month)
  s3://{BUCKET}/gold/movie_revenue_hourly/      (partitioned by year_month)
  s3://{BUCKET}/gold/app_health_hourly/         (partitioned by year_month)
  s3://{BUCKET}/gold/user_activity_daily/       (partitioned by log_date)
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

13 event types representing the full e-commerce user journey on an apparel retail Web/App:

| event_id | event_type | Description |
|----------|-----------|-------------|
| 1 | search | User searches for products by keyword |
| 2 | view_item | User views product detail page |
| 3 | select_item_variant | User selects color/size variant |
| 4 | add_to_cart | User adds item to cart |
| 5 | view_cart | User opens cart screen |
| 6 | remove_from_cart | User removes item from cart |
| 7 | update_cart_item | User changes qty/variant in cart |
| 8 | begin_checkout | User starts checkout flow |
| 9 | add_shipping_info | User fills shipping address |
| 10 | add_coupon | User applies promotion code |
| 11 | add_payment_info | User selects payment method |
| 12 | place_order | User submits order |
| 13 | payment_callback | Payment gateway result callback |

```json
{
  "event_uuid": "uuid4",
  "event_id": "1-13",
  "event_type": "search|view_item|select_item_variant|add_to_cart|view_cart|remove_from_cart|update_cart_item|begin_checkout|add_shipping_info|add_coupon|add_payment_info|place_order|payment_callback",
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

## Bronze Schema (Delta — raw landing zone)

| Column | Type | Notes |
|--------|------|-------|
| event_uuid | string | |
| event_id | int | |
| event_type | string | |
| event_ts | long | unix timestamp |
| log_date | date | partition key |
| created_at | string | ISO string, unparsed intentionally |
| user_id | string | |
| device_os | string | |
| app_version | string | |
| raw_metadata | string | original metadata JSON as string — immutable source of truth |
| _kafka_topic | string | source topic name |
| _kafka_partition | int | for targeted reprocessing |
| _kafka_offset | long | for targeted reprocessing |
| _ingested_at | timestamp | |

## Bronze Schema — MySQL Batch Tables (Delta — T-1 append-only)

All MySQL batch tables use incremental ingestion: `DATE(COALESCE(updated_at, created_at)) = ymd`.
Bronze never uses MERGE or UPDATE — append-only, preserving full change history.

### bronze.customers  (partition: report_date)
| Column | Type | Notes |
|--------|------|-------|
| report_date | string (yyyyMMdd) | partition key |
| report_month | string (yyyyMM) | |
| customer_id | string | |
| first_name | string | nullable |
| last_name | string | nullable |
| phone | string | raw, not masked (10 digits, '09xxxxxxxx') |
| dob | date | |
| age | int | |
| gender | int | 0=Other, 1=Nam, 2=Nu |
| address_line | string | raw VN address |
| is_deleted | int | 0=alive, 1=soft-deleted |
| registered_datetime | timestamp | |
| created_at | timestamp | needed downstream for CDC Insert detection |
| updated_at | timestamp | |
| source | string | "offline" / "online_web" / "app_store" |
| _loaded_at | timestamp | |

### bronze.category  (NO physical partition — low cardinality, ~5 rows)
| Column | Type | Notes |
|--------|------|-------|
| report_date | string (yyyyMMdd) | data column only — Silver T-1 filter, NOT partition |
| report_month | string (yyyyMM) | |
| category_id | string | |
| category_name | string | |
| is_current | boolean | preserved from source as-is |
| created_at | timestamp | |
| updated_at | timestamp | |
| _loaded_at | timestamp | |

### bronze.products  (partition: report_date)
| Column | Type | Notes |
|--------|------|-------|
| report_date | string (yyyyMMdd) | partition key |
| report_month | string (yyyyMM) | |
| product_id | string | |
| product_name | string | system code (e.g. Basic Cotton T-Shirt) |
| product_display_name | string | English marketing name |
| category_id | string | FK -> category.category_id |
| sales_unit | string | |
| color | string | default variant color |
| size | string | default variant size |
| unit_price | decimal(15,2) | list price (VND) |
| created_at | timestamp | |
| updated_at | timestamp | |
| is_current | boolean | preserved from source as-is |
| _loaded_at | timestamp | |

### bronze.branches  (partition: report_date)
| Column | Type | Notes |
|--------|------|-------|
| report_date | string (yyyyMMdd) | partition key |
| report_month | string (yyyyMM) | |
| branch_id | string | |
| branch_name | string | |
| branch_type | string | flagship \| mall_kiosk \| outlet \| standard |
| region | string | |
| city_province | string | |
| ward | string | nullable |
| status | string | |
| open_date | date | branch opening date |
| created_at | timestamp | |
| updated_at | timestamp | |
| is_current | boolean | preserved from source as-is |
| _loaded_at | timestamp | |

### bronze.transactions  (partition: report_date)
| Column | Type | Notes |
|--------|------|-------|
| report_date | string (yyyyMMdd) | partition key |
| report_month | string (yyyyMM) | |
| channel | string | "offline" / "online" |
| branch_id | string | |
| transaction_id | string | |
| transaction_datetime | timestamp | |
| payment_type | string | |
| trans_total_amount | decimal(15,2) | |
| trans_total_line | int | null for online |
| trans_total_sell_sku | int | null for online |
| customer_id | string | |
| order_status | string | W\|R\|O\|D\|I\|C\|B\|P\|F |
| delivery_date | date | null if not yet delivered |
| created_at | timestamp | |
| updated_at | timestamp | |
| _loaded_at | timestamp | |

### bronze.transaction_details  (partition: report_date — pre-aggregated per transaction)
| Column | Type | Notes |
|--------|------|-------|
| report_date | string (yyyyMMdd) | partition key |
| channel | string | "offline" / "online" |
| branch_id | string | |
| transaction_id | string | |
| order_total_line | bigint | non-promo line count |
| order_total_sku | bigint | non-promo qty sum |
| promo_total_line | bigint | promo line count |
| promo_total_sku | bigint | promo qty sum |
| is_promo | int | 1 if any promo item exists |
| trans_metadata | array<array<string>> | detail rows: [detail_id, product_id, is_promo, uom, qty, amount, variant_size, variant_color] |
| created_at | timestamp | |
| _loaded_at | timestamp | |

## Silver Schema (Delta — cleaned + deduplicated)

### silver.events  (partition: log_date)
| Column | Type | Notes |
|--------|------|-------|
| event_uuid | string | MERGE key (dedup) |
| event_id | int | |
| event_type | string | |
| event_time | timestamp | parsed from created_at |
| log_date | date | partition key |
| user_id | string | |
| device_os | string | |
| app_version | string | |
| is_success | int | nullable — from metadata (1=success, 0=fail) |
| error_message | string | nullable |
| product_id | string | nullable — from metadata (view_item, add_to_cart, etc.) |
| product_name | string | nullable — from metadata (view_item, add_to_cart, etc.) |
| item_quantity | int | default 0 — COALESCE(quantity, removed_quantity, new_quantity, total_items) |
| is_transaction | boolean | true if event_id IN (12, 13): place_order / payment_callback |
| source_topic | string | behavior_events or transaction_events |
| _processed_at | timestamp | |

### silver.nou  (partition: ym — customer first-order registry)
| Column | Type | Notes |
|--------|------|-------|
| customer_id | string | MERGE key (unique, 1 row per customer) |
| nou_ymd | string (yyyyMMdd) | first-ever order date |
| ym | string (yyyyMM) | partition key = month of first order |
| etl_datetime | timestamp | |

### silver.transactions  (partition: branch_id, ym, ymd — multi-level; CDF + Deletion Vectors enabled)
MERGE keys: `(trans_id, branch_id, ym, ymd)`. `update_on_match=True` so `order_status` mutations (W → I → B) replace the existing row.
Constraints: `order_status IN ('W','R','O','D','I','C','B','P','F')`, `channel IN ('offline','online')`, `user_type IN (1,2,3)`.
Intra-batch dedup: Window `partitionBy(transaction_id, branch_id) orderBy(created_at desc)` — bronze append-only, latest `created_at` = newest snapshot version.

| Column | Type | Notes |
|--------|------|-------|
| trans_month | string (yyyyMM) | |
| trans_date | date | |
| trans_time | string (HH:mm:ss) | |
| timestamp | long | unix timestamp |
| trans_id | string | Smart Key: {ymd}\_{customer\_id}\_{transaction\_id} |
| customer_id | string | |
| user_type | int | 1=NOU, 2=Retention, 3=Resurrected |
| NOU | int | 1 only for first-ever transaction per customer |
| nou_ymd | string (yyyyMMdd) | from silver.nou |
| branch_id | string | partition key (level 1) |
| channel | string | "offline" / "online" |
| order_status | string | W\|R\|O\|D\|I\|C\|B\|P\|F (mutable — drives MERGE UPDATE) |
| payment_type | string | cash\|card\|momo\|zalopay\|vnpay |
| order_total_amount | decimal | |
| created_at | timestamp | = bronze.created_at of the FIRST snapshot — original order creation; **preserved on MERGE update** |
| updated_at | timestamp | = bronze.created_at of the LATEST snapshot version — refreshed every MERGE update; used by gold for diff_date |
| etl_datetime | timestamp | |
| ym | string (yyyyMM) | partition key (level 2) |
| ymd | string (yyyyMMdd) | partition key (level 3) |

### silver.customers  (partition: source — SCD1 dim, CDF + DV)
MERGE keys: `(customer_id)`. `update_on_match=True`. 1 row per customer (snapshot-only, no `is_current`).

| Column | Type | Notes |
|--------|------|-------|
| customer_id | string | MERGE key |
| full_name | string | TRIM(CONCAT(last_name, ' ', first_name)); null parts dropped via concat_ws |
| phone | string | masked: last 3 digits visible, rest replaced by 'x' (e.g. `xxxxxxx567`) |
| dob | date | nullable |
| age | int | nullable; null = unknown |
| gender | string | M / F / O |
| city | string | last segment of address_line after comma; 'unknown' if missing |
| _cdc_operation | int | 0=Insert, 1=Update, 2=Deleted (derived from is_deleted + created_at) |
| registered_datetime | timestamp | |
| updated_at | timestamp | latest mutation timestamp |
| source | string | "offline" / "online_web" / "app_store" — partition key |
| _processed_at | timestamp | |

### silver.category  (NO partition — SCD1 dim, CDF + DV)
MERGE keys: `(category_id)`. `update_on_match=True`. 1 row per category (snapshot-only, `is_current` dropped).

| Column | Type | Notes |
|--------|------|-------|
| category_id | string | MERGE key |
| category_name | string | |
| _cdc_operation | int | 0=Insert, 1=Update, 2=Discontinued (derived from is_current + created_at) |
| created_at | timestamp | |
| updated_at | timestamp | |
| _processed_at | timestamp | |

### silver.products  (partition: sales_unit — SCD1 dim, CDF + DV)
MERGE keys: `(product_id)`. `update_on_match=True`. 1 row per product. `is_current` dropped.
Denormalized via JOIN silver.category — `category_name` carried alongside `category_id`.
DAG dependency: silver.category MUST run BEFORE silver.products.

| Column | Type | Notes |
|--------|------|-------|
| product_id | string | MERGE key |
| product_name | string | system code |
| product_display_name | string | English marketing name |
| category_id | string | FK |
| category_name | string | denormalized via JOIN silver.category |
| sales_unit | string | partition key (PCS / PRS) |
| color | string | default variant color |
| size | string | default variant size |
| unit_price | decimal(15,2) | list price (VND) |
| _cdc_operation | int | 0=Insert, 1=Update, 2=Discontinued (derived from is_current + created_at) |
| created_at | timestamp | |
| updated_at | timestamp | |
| _processed_at | timestamp | |

### silver.branches  (partition: region — SCD1 dim, CDF + DV)
MERGE keys: `(branch_id)`. `update_on_match=True`. 1 row per branch. `is_current` dropped.

| Column | Type | Notes |
|--------|------|-------|
| branch_id | string | MERGE key |
| branch_name | string | |
| branch_type | string | flagship \| mall_kiosk \| outlet \| standard |
| region | string | partition key (North / South / Central) |
| city_province | string | |
| ward | string | nullable |
| status | string | "active" / "closed" |
| open_date | date | branch opening date |
| _cdc_operation | int | 0=Insert, 1=Update, 2=Closed (derived from is_current + created_at) |
| created_at | timestamp | |
| updated_at | timestamp | |
| _processed_at | timestamp | |

## Gold Tables (Delta — pre-aggregated, T-1 overwrite)

### funnel_hourly
DA query: "Where do users drop off? Which app version converts better?"
Columns: `hour`, `event_type`, `app_version`, `device_os`, `event_count`, `success_count`, `unique_users`

### movie_revenue_hourly
DA query: "Which movies earned the most? Peak hour (19-21h) patterns?"
Columns: `hour`, `movie_id`, `movie_name`, `tickets_sold`, `total_revenue`, `unique_buyers`

### app_health_hourly
DA query: "Does version 2.0.0-beta have higher error rate than 1.1.5?"
Columns: `hour`, `app_version`, `event_type`, `total_count`, `error_count`

### user_activity_daily
DA query: "DAU trend? Which user segment is most valuable?"
Columns: `log_date`, `user_id`, `session_count`, `total_events`, `booked_count`, `total_spent`, `preferred_device`, `last_app_version`

### gold.daily_customer_sales  (partition: branch_id, ym, ymd — Sales dashboard)
DA query: "Hôm nay tiền về bao nhiêu? Khách nào mua? NOU/Retention/Resurrected? Chi nhánh nào ngon? Kênh nào mạnh?"
Source: `silver.transactions` (STOCK statuses W/O/D/P + FLOW statuses I/B/C/R updated today, F excluded).
Grain: `(snapshot_date, customer_id, branch_id, user_type, channel, payment_type)`.
Write mode: `replaceWhere ymd = '{run_date}'` (idempotent T-1 overwrite).

| Column | Type | Notes |
|--------|------|-------|
| snapshot_date | date | report date (T-1) |
| customer_id | string | |
| branch_id | string | partition (level 1) |
| user_type | int | 1=NOU, 2=Retention, 3=Resurrected |
| channel | string | offline / online |
| payment_type | string | |
| processing_rev | decimal(15,2) | SUM order_total_amount for STOCK (W/O/D/P) |
| processing_orders | int | COUNT distinct trans_id for STOCK |
| recognized_rev | decimal(15,2) | SUM for I |
| recognized_orders | int | COUNT for I |
| returned_rev | decimal(15,2) | SUM for B |
| returned_orders | int | COUNT for B |
| cancelled_rev | decimal(15,2) | SUM for C, R |
| cancelled_orders | int | COUNT for C, R |
| ym | string (yyyyMM) | partition (level 2) |
| ymd | string (yyyyMMdd) | partition (level 3) |
| etl_datetime | timestamp | |

### gold.daily_logistics_aging  (partition: branch_id, ym, ymd — Operations dashboard)
DA query: "Tiền kẹt ở đâu? Rổ aging nào lớn nhất? Chi nhánh nào ngâm đơn lâu? Tỷ lệ hủy theo channel/payment?"
Source: `silver.transactions` (STOCK + FLOW today, F excluded). No customer_id in grain.
Grain: `(snapshot_date, aging_category, branch_id, order_status, channel, payment_type)`.
Aging buckets (mutually exclusive): `<1`, `<=3`, `<=7`, `<=14`, `<=28`, `>28`.
diff_date logic:
- STOCK (W/O/D/P): `DATEDIFF(snapshot_date, trans_date)`
- FLOW (I/B/C/R): `DATEDIFF(DATE(updated_at), trans_date)`

Write mode: `replaceWhere ymd = '{run_date}'` (idempotent T-1 overwrite).

| Column | Type | Notes |
|--------|------|-------|
| snapshot_date | date | report date (T-1) |
| aging_category | string | <1 \| <=3 \| <=7 \| <=14 \| <=28 \| >28 |
| branch_id | string | partition (level 1) |
| order_status | string | 8 codes (W/O/D/P/I/B/C/R) — F excluded |
| channel | string | offline / online |
| payment_type | string | |
| total_rev | decimal(15,2) | SUM order_total_amount |
| total_orders | int | COUNT distinct trans_id |
| ym | string (yyyyMM) | partition (level 2) |
| ymd | string (yyyyMMdd) | partition (level 3) |
| etl_datetime | timestamp | |

## Traffic Ratio (weighted funnel distribution)
- ~65% upper funnel: search (25%), view_item (25%), select_item_variant (15%)
- ~20% mid funnel: add_to_cart (10%), view_cart (6%), remove_from_cart (2%), update_cart_item (2%)
- ~15% lower funnel: begin_checkout (5%), add_shipping_info (3%), add_coupon (2%), add_payment_info (2%), place_order (2%), payment_callback (1%)
