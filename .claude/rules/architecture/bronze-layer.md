---
description: "Bronze layer schemas — behavior_events (Kafka streaming) + 6 MySQL batch tables, all append-only Delta"
---

# Bronze Layer

Raw landing zone. Append-only, immutable. No MERGE or UPDATE ever applied.

## bronze.behavior_events  (partition: log_date — Kafka Streaming)

Written by `streaming_to_bronze.py` (always-on, 30s trigger). `raw_metadata` stored as JSON string — never flattened at Bronze.

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

## Bronze — MySQL Batch Tables (Delta — T-1 append-only)

All MySQL batch tables use incremental ingestion: `DATE(COALESCE(updated_at, created_at)) = ymd`.

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
