"""
Seed fake data into MySQL for the Apparel Retail backend raw layer.
Run once after `docker compose up mysql` to populate test data.

Usage:
    docker exec python-producer python infra/mysql/seed_data.py
"""

import os
import random
import string
import uuid
from collections import Counter
from datetime import date, datetime, timedelta

import mysql.connector

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "apparel_retail") # Đổi tên DB
MYSQL_USER = os.getenv("MYSQL_USER", "app_user")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "app_pass")

# Product taxonomy (parent dim of products.category_id)
CATEGORIES = [
    ("cat_001", "T-Shirt"),
    ("cat_002", "Jeans"),
    ("cat_003", "Outerwear"),
    ("cat_004", "Hoodie"),
    ("cat_005", "Footwear"),
]

# Branches (id, name, type, region, city, ward, open_date)
BRANCHES = [
    ("br_001", "Flagship Store - District 1",  "flagship",   "South",   "Ho Chi Minh City", "Ben Nghe",  date(2018, 5, 10)),
    ("br_002", "Mall Kiosk - District 7",      "mall_kiosk", "South",   "Ho Chi Minh City", "Tan Phong", date(2020, 8, 15)),
    ("br_003", "Downtown Branch - HN",         "standard",   "North",   "Hanoi",            "Hang Bai",  date(2017, 3, 20)),
    ("br_004", "Suburban Outlet - HN",         "outlet",     "North",   "Hanoi",            "Dich Vong", date(2021, 11, 5)),
    ("br_005", "Central Hub - Da Nang",        "flagship",   "Central", "Da Nang",          "Hoa Hai",   date(2019, 7, 1)),
]

# Apparel products (id, name [system], display [marketing EN], unit, category_id, color, size, unit_price)
PRODUCTS = [
    ("prd_001", "Basic Cotton T-Shirt",      "Cotton Crew Neck Tee",          "PCS", "cat_001", "Black", "M",        250000),
    ("prd_002", "Classic Denim Jeans",       "Slim Fit Denim Jeans",          "PCS", "cat_002", "Navy",  "L",        550000),
    ("prd_003", "Lightweight Windbreaker",   "Lightweight Hooded Windbreaker","PCS", "cat_003", "Beige", "L",        450000),
    ("prd_004", "Fleece Hoodie",             "Premium Fleece Pullover Hoodie","PCS", "cat_004", "Grey",  "XL",       600000),
    ("prd_005", "Canvas Sneakers",           "Low-Top Canvas Sneakers",       "PRS", "cat_005", "White", "Freesize", 750000),
]

# Variant pool — must cover every default color/size in PRODUCTS to keep transaction_details consistent.
SIZES  = ["S", "M", "L", "XL", "Freesize"]
COLORS = ["Black", "White", "Navy", "Beige", "Grey"]

PAYMENT_TYPES = ["cash", "card", "momo", "zalopay", "vnpay"]

VN_FIRST = ["An", "Bao", "Chi", "Dung", "Giang", "Huong", "Khoa", "Lan", "Minh", "Nam",
            "Phuc", "Quan", "Son", "Thao", "Trang", "Tuan", "Vy", "Yen"]
VN_LAST  = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Vo", "Dang", "Bui", "Do", "Ngo", "Duong"]
VN_CITIES    = ["Ho Chi Minh", "Hanoi", "Da Nang", "Can Tho", "Hai Phong", "Nha Trang", "Hue", "Vung Tau"]
VN_DISTRICTS = ["Quan 1", "Quan 3", "Quan 7", "Ba Dinh", "Hoan Kiem", "Cau Giay", "Hai Chau", "Ngu Hanh Son"]
VN_STREETS   = ["Le Loi", "Nguyen Hue", "Tran Hung Dao", "Hai Ba Trung", "Ly Thuong Kiet", "Pasteur", "Vo Van Tan"]


def random_phone():
    return "09" + "".join(random.choices(string.digits, k=8))


def random_address():
    return f"{random.randint(1, 999)} {random.choice(VN_STREETS)}, {random.choice(VN_DISTRICTS)}, {random.choice(VN_CITIES)}"

# W=Waiting, R=Rejected, O=Open, D=Delivering, I=Invoiced (completed),
# C=Cancelled, B=Returned, P=PendingPayment, F=PaymentFailed
ORDER_STATUS_WEIGHTS = {
    "I": 60, "O": 15, "D": 10, "W": 5, "P": 3, "C": 3, "R": 2, "B": 1, "F": 1,
}
ORDER_STATUSES = list(ORDER_STATUS_WEIGHTS.keys())
ORDER_WEIGHTS  = list(ORDER_STATUS_WEIGHTS.values())


def get_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
        user=MYSQL_USER, password=MYSQL_PASS,
    )


def seed_categories(cursor):
    now = datetime(2026, 1, 1, 0, 0, 0)
    rows = [(cid, name, 1, now, now) for cid, name in CATEGORIES]
    cursor.executemany(
        "INSERT IGNORE INTO category (category_id, category_name, is_current, created_at, updated_at) "
        "VALUES (%s,%s,%s,%s,%s)",
        rows,
    )


def seed_branches(cursor):
    now = datetime(2026, 1, 1, 0, 0, 0)
    rows = [
        (bid, name, btype, region, city, ward, "active", open_dt, now, now, 1)
        for bid, name, btype, region, city, ward, open_dt in BRANCHES
    ]
    cursor.executemany(
        "INSERT IGNORE INTO branches "
        "(branch_id, branch_name, branch_type, region, city_province, ward, status, open_date, created_at, updated_at, is_current) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows,
    )


def seed_products(cursor):
    now = datetime(2026, 1, 1, 0, 0, 0)
    rows = [
        (pid, name, display, cat_id, unit, color, size, price, now, now, 1)
        for pid, name, display, unit, cat_id, color, size, price in PRODUCTS
    ]
    cursor.executemany(
        "INSERT IGNORE INTO products "
        "(product_id, product_name, product_display_name, category_id, sales_unit, color, size, unit_price, created_at, updated_at, is_current) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows,
    )


def seed_customers(cursor, n=1236):
    sources = ["offline", "online_web", "app_store"]
    base = datetime(2025, 6, 1)
    today = datetime(2026, 4, 24)
    rows = []
    for i in range(1, n + 1):
        reg = base + timedelta(days=random.randint(0, 200))
        first = None if random.random() < 0.05 else random.choice(VN_FIRST)
        last  = None if random.random() < 0.05 else random.choice(VN_LAST)
        dob   = datetime(random.randint(1970, 2008), random.randint(1, 12), random.randint(1, 28))
        age   = today.year - dob.year
        gender = random.choices([0, 1, 2], weights=[5, 50, 45])[0]
        rows.append((
            f"cust_{i:04d}", first, last,
            random_phone(), dob.date(), age, gender, random_address(),
            0,
            reg, reg, reg + timedelta(days=random.randint(0, 100)),
            random.choice(sources),
        ))
    cursor.executemany(
        "INSERT IGNORE INTO customers (customer_id, first_name, last_name, phone, dob, age, gender, address_line, is_deleted, registered_datetime, created_at, updated_at, source) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows,
    )


def seed_transactions(cursor, date_str: str, n_pos=50, n_online=30):
    """Seed POS (in-store) and online transactions for a given date (YYYY-MM-DD)."""
    base_dt = datetime.strptime(date_str, "%Y-%m-%d")

    pos_headers, pos_details = [], []
    onl_headers, onl_details = [], []

    # 1. Giao dịch mua trực tiếp tại cửa hàng (POS)
    for _ in range(n_pos):
        tid = f"pos_{uuid.uuid4().hex[:12]}"
        br_id = random.choice(BRANCHES)[0]
        cust   = f"cust_{random.randint(1, 1236):04d}"
        ts     = base_dt + timedelta(hours=random.randint(9, 22), minutes=random.randint(0, 59))
        n_items = random.randint(1, 4)
        total_amount = 0
        total_line   = 0
        total_sku    = 0

        for j in range(n_items):
            did     = f"d_{uuid.uuid4().hex[:10]}"
            prd_id, _, _, unit, _, _, _, base_price = random.choice(PRODUCTS)
            
            is_promo = 1 if random.random() < 0.2 else 0
            qty     = random.randint(1, 3)
            # Khuyến mãi giảm 20%
            price   = int(base_price * 0.8) if is_promo else base_price 
            amount  = qty * price
            
            variant_size = random.choice(SIZES)
            variant_color = random.choice(COLORS)
            
            pos_details.append((did, tid, br_id, prd_id, is_promo, unit, qty, amount, variant_size, variant_color, ts))
            total_amount += amount
            total_line   += 1
            total_sku    += qty

        status = random.choices(ORDER_STATUSES, weights=ORDER_WEIGHTS, k=1)[0]
        is_delivery_requested = random.random() < 0.1
        dlv = ts.date() if not is_delivery_requested and status == "I" else (
            (ts + timedelta(days=random.randint(1, 3))).date() if status in ("I", "D", "B") else None
        )
        pos_headers.append((tid, br_id, ts, random.choice(PAYMENT_TYPES), total_amount, total_line, total_sku, cust, status, dlv, ts, ts))

    # 2. Giao dịch mua Online trên Web/App
    for _ in range(n_online):
        tid = f"onl_{uuid.uuid4().hex[:12]}"
        br_id = random.choice(BRANCHES)[0]
        cust   = f"cust_{random.randint(1, 1236):04d}"
        ts     = base_dt + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
        n_items = random.randint(1, 5)
        total_amount = 0

        for j in range(n_items):
            did     = f"d_{uuid.uuid4().hex[:10]}"
            prd_id, _, _, unit, _, _, _, base_price = random.choice(PRODUCTS)
            
            is_promo = 1 if random.random() < 0.3 else 0 # Online hay có flash sale hơn
            qty     = random.randint(1, 2)
            price   = int(base_price * 0.8) if is_promo else base_price
            amount  = qty * price
            
            variant_size = random.choice(SIZES)
            variant_color = random.choice(COLORS)
            
            onl_details.append((did, tid, prd_id, is_promo, unit, qty, amount, variant_size, variant_color, ts))
            total_amount += amount

        status = random.choices(ORDER_STATUSES, weights=ORDER_WEIGHTS, k=1)[0]
        dlv = (ts + timedelta(days=random.randint(2, 5))).date() if status in ("I", "D", "B") else None
        onl_headers.append((tid, br_id, ts, random.choice(PAYMENT_TYPES), total_amount, cust, status, dlv, ts, ts))

    # Thay đổi câu lệnh INSERT để map với cột variant_size và variant_color
    cursor.executemany(
        "INSERT IGNORE INTO pos_transactions (transaction_id, branch_id, transaction_datetime, payment_type, trans_total_amount, trans_total_line, trans_total_sell_sku, customer_id, order_status, delivery_date, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        pos_headers,
    )
    cursor.executemany(
        "INSERT IGNORE INTO pos_transaction_details (transaction_detail_id, transaction_id, branch_id, product_id, is_promo, product_uom_code, trans_qty, trans_line_amount, variant_size, variant_color, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        pos_details,
    )
    cursor.executemany(
        "INSERT IGNORE INTO online_transactions (transaction_id, branch_id, transaction_datetime, payment_type, trans_total_amount, customer_id, order_status, delivery_date, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        onl_headers,
    )
    cursor.executemany(
        "INSERT IGNORE INTO online_transaction_details (transaction_detail_id, transaction_id, product_id, is_promo, product_uom_code, trans_qty, trans_line_amount, variant_size, variant_color, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        onl_details,
    )


def main():
    conn   = get_conn()
    cursor = conn.cursor()

    print("Seeding Retail Branches...")
    seed_branches(cursor)
    print("Seeding Product Categories...")
    seed_categories(cursor)
    print("Seeding Apparel Products...")
    seed_products(cursor)
    print("Seeding Customers (1236)...")
    seed_customers(cursor)

    # Phân phối ngẫu nhiên 10000 orders (6500 POS + 3500 online) vào Feb-Apr 2026
    start = datetime(2026, 2, 1).date()
    end   = datetime(2026, 4, 30).date()
    days  = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    pos_counts = Counter(random.choices(days, k=6500))
    onl_counts = Counter(random.choices(days, k=3500))

    for day in days:
        n_pos    = pos_counts.get(day, 0)
        n_online = onl_counts.get(day, 0)
        if n_pos == 0 and n_online == 0:
            continue
        print(f"Seeding {day}: {n_pos} POS + {n_online} online...")
        seed_transactions(cursor, day.strftime("%Y-%m-%d"), n_pos=n_pos, n_online=n_online)
        conn.commit()

    cursor.close()
    conn.close()
    print("Done. MySQL seed complete for Apparel Retail.")


if __name__ == "__main__":
    main()