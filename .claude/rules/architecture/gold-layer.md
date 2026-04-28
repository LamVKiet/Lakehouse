---
description: "Gold layer schemas — 5 tables: events_discovery/cart/checkout (clickstream), daily_customer_sales, daily_logistics_aging"
---

# Gold Layer

Pre-aggregated, T-1 overwrite (`replaceWhere`). Optimized for DA queries — no runtime joins needed.

## gold.events_discovery  (partition: log_date — Discovery & Search behavior)
DA query: "Which screens drive most engagement? What are users searching for? Which products attract most views?"
Source: `silver.events_discovery` (T-1 filter). Write: `replaceWhere log_date`.
Grain: `(event_type, log_date, session_id, user_id, source_screen, source_element, position, search_keyword, product_id)`.

| Column | Type | Notes |
|--------|------|-------|
| event_type | string | |
| log_date | date | partition key |
| session_id | string | |
| user_id | string | |
| source_screen | string | |
| source_element | string | |
| position | int | |
| search_keyword | string | |
| product_id | string | |
| event_count | long | COUNT(event_uuid) |

## gold.events_cart  (partition: log_date — Cart Activity)
DA query: "Cart add vs remove ratio? Which products get abandoned? Which screens drive cart actions?"
Source: `silver.events_cart` (T-1 filter). Write: `replaceWhere log_date`.
Grain: `(event_type, log_date, session_id, user_id, source_screen, source_element, product_id)`.

| Column | Type | Notes |
|--------|------|-------|
| event_type | string | |
| log_date | date | partition key |
| session_id | string | |
| user_id | string | |
| source_screen | string | |
| source_element | string | |
| product_id | string | |
| event_count | long | COUNT(event_uuid) |
| qty_added | long | SUM(quantity) for add_to_cart only |
| qty_removed | long | SUM(removed_quantity) for remove_from_cart only |

## gold.events_checkout  (partition: log_date — Checkout & Payment funnel)
DA query: "Which payment methods succeed most? Which promotions work? What's the failure rate by step?"
Source: `silver.events_checkout` (T-1 filter). Write: `replaceWhere log_date`.
Grain: `(event_type, log_date, session_id, user_id, payment_method, shipping_method, promotion_id, is_success, error_code, error_message)`.

| Column | Type | Notes |
|--------|------|-------|
| event_type | string | |
| log_date | date | partition key |
| session_id | string | |
| user_id | string | |
| payment_method | string | |
| shipping_method | string | |
| promotion_id | string | |
| is_success | int | 1=success, 0=fail — payment_callback only |
| error_code | string | |
| error_message | string | |
| event_count | long | COUNT(event_uuid) |

## gold.daily_customer_sales  (partition: branch_id, ym, ymd — Sales dashboard)
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

## gold.daily_logistics_aging  (partition: branch_id, ym, ymd — Operations dashboard)
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
