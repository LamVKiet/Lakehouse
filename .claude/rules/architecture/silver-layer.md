---
description: "Silver layer schemas — 8 tables: events_discovery/cart/checkout, nou, transactions, customers, category, products, branches"
---

# Silver Layer

Cleaned + deduplicated via Delta MERGE INTO. Types parsed, nulls filtered. Smart Keys generated.

## silver.events_discovery  (partition: log_date — Discovery & Product)
Filter: `event_id IN (1,2,3,4)` — home_screen_view, search, view_item, select_item_variant.
MERGE keys: `(event_uuid, log_date)`. Insert-only.

| Column | Type | Notes |
|--------|------|-------|
| event_uuid | string | MERGE key |
| trans_event_id | string | Smart Key: `{ymd}_{user_id}_{event_id}` |
| event_id | int | |
| event_type | string | |
| event_time | timestamp | parsed from created_at |
| log_date | date | partition key |
| session_id | string | |
| user_id | string | |
| device_os | string | |
| app_version | string | top-level (Kafka header) |
| meta_app_version | string | from metadata — only home_screen_view |
| source_screen | string | |
| source_element | string | |
| position | int | item position on list when user clicks |
| search_keyword | string | |
| result_count | int | |
| product_id | string | |
| product_name | string | |
| base_price | long | |
| variant_type | string | |
| variant_value | string | |
| _processed_at | timestamp | |

## silver.events_cart  (partition: log_date — Cart Activities)
Filter: `event_id IN (5,6,7,8)` — add_to_cart, view_cart, remove_from_cart, update_cart_item.
MERGE keys: `(event_uuid, log_date)`. Insert-only.

| Column | Type | Notes |
|--------|------|-------|
| event_uuid | string | MERGE key |
| trans_event_id | string | Smart Key |
| event_id | int | |
| event_type | string | |
| event_time | timestamp | |
| log_date | date | partition key |
| session_id | string | |
| user_id | string | |
| device_os | string | |
| app_version | string | |
| source_screen | string | screen where user triggered action |
| source_element | string | |
| product_id | string | nullable for view_cart |
| variant_color | string | |
| variant_size | string | |
| quantity | int | |
| removed_quantity | int | |
| action_type | string | update_cart_item only |
| old_quantity | int | |
| new_quantity | int | |
| total_items | int | view_cart |
| cart_total_value | long | |
| _processed_at | timestamp | |

## silver.events_checkout  (partition: log_date — Checkout & Payments)
Filter: `event_id IN (9,10,11,12,13,14)` — begin_checkout, add_shipping_info, add_coupon, add_payment_info, place_order, payment_callback.
MERGE keys: `(event_uuid, log_date)`. Insert-only.

| Column | Type | Notes |
|--------|------|-------|
| event_uuid | string | MERGE key |
| trans_event_id | string | Smart Key |
| event_id | int | |
| event_type | string | |
| event_time | timestamp | |
| log_date | date | partition key |
| session_id | string | |
| user_id | string | |
| device_os | string | |
| app_version | string | |
| items_list | array<string> | begin_checkout |
| shipping_method | string | |
| shipping_fee | long | |
| city_province | string | |
| promotion_id | string | |
| promotion_type | string | |
| discount_amount | long | |
| is_valid | int | add_coupon |
| error_message | string | coupon error |
| payment_method | string | |
| final_amount | long | |
| order_id | string | link to silver.transactions |
| transaction_id | string | |
| is_success | int | payment_callback |
| error_code | string | payment error |
| _processed_at | timestamp | |

## silver.nou  (partition: ym — customer first-order registry)
| Column | Type | Notes |
|--------|------|-------|
| customer_id | string | MERGE key (unique, 1 row per customer) |
| nou_ymd | string (yyyyMMdd) | first-ever order date |
| ym | string (yyyyMM) | partition key = month of first order |
| etl_datetime | timestamp | |

## silver.transactions  (partition: branch_id, ym, ymd — multi-level; CDF + Deletion Vectors enabled)
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

## silver.customers  (partition: source — SCD1 dim, CDF + DV)
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

## silver.category  (NO partition — SCD1 dim, CDF + DV)
MERGE keys: `(category_id)`. `update_on_match=True`. 1 row per category (snapshot-only, `is_current` dropped).

| Column | Type | Notes |
|--------|------|-------|
| category_id | string | MERGE key |
| category_name | string | |
| _cdc_operation | int | 0=Insert, 1=Update, 2=Discontinued (derived from is_current + created_at) |
| created_at | timestamp | |
| updated_at | timestamp | |
| _processed_at | timestamp | |

## silver.products  (partition: sales_unit — SCD1 dim, CDF + DV)
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

## silver.branches  (partition: region — SCD1 dim, CDF + DV)
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
