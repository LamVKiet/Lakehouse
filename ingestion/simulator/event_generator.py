"""
Fake apparel retail clickstream event generator.
Generates user behavior events for an omnichannel fashion retail platform.
Served via FastAPI data-simulator.
"""

import uuid
import random
from datetime import datetime

EVENT_TYPES = {
    1: "home_screen_view",
    2: "search",
    3: "view_item",
    4: "select_item_variant",
    5: "add_to_cart",
    6: "view_cart",
    7: "remove_from_cart",
    8: "update_cart_item",
    9: "begin_checkout",
    10: "add_shipping_info",
    11: "add_coupon",
    12: "add_payment_info",
    13: "place_order",
    14: "payment_callback",
}

# Reverse lookup: event_type string -> event_id
EVENT_TYPE_TO_ID = {v: k for k, v in EVENT_TYPES.items()}

# Traffic weights — funnel narrows from browse (top) to purchase (bottom)
EVENT_WEIGHTS = [30, 25, 25, 15, 10, 6, 2, 2, 5, 3, 2, 2, 2, 1]

APP_VERSIONS = ["1.0.0", "1.1.0", "1.1.5", "2.0.0-beta"]

PRODUCTS = [
    {"id": "prd_001", "name": "Áo thun Basic", "base_price": 250000},
    {"id": "prd_002", "name": "Quần Jean Classic", "base_price": 550000},
    {"id": "prd_003", "name": "Áo khoác gió", "base_price": 450000},
    {"id": "prd_004", "name": "Áo Hoodie nỉ", "base_price": 600000},
    {"id": "prd_005", "name": "Giày Canvas", "base_price": 750000},
]

SIZES = ["S", "M", "L", "XL", "Freesize"]
COLORS = ["Black", "White", "Navy", "Beige", "Grey"]
SEARCH_KEYWORDS = [
    "áo thun đen", "quần jean nam", "hoodie nữ", "giày sneaker",
    "áo khoác gió", "váy đầm", "áo polo", "quần short",
]
SHIPPING_METHODS = ["standard", "express"]
PAYMENT_METHODS = ["momo", "vnpay", "cod", "credit_card"]
PROMOTION_IDS = ["FREESHIP_T4", "SALE20", "NEWUSER10", "FLASHSALE", "VIP_DISCOUNT"]
PROMOTION_TYPES = ["freeship", "discount_percentage", "discount_amount"]
CITIES = ["Ho Chi Minh City", "Hanoi", "Da Nang", "Hai Phong", "Can Tho"]
COUPON_ERRORS = ["Coupon expired", "Minimum order not met", "Already used", "Invalid code"]
PAYMENT_ERRORS = ["insufficient_balance", "user_cancelled", "gateway_timeout", "card_declined"]


def _random_product():
    return random.choice(PRODUCTS)


def _build_metadata(event_type: str) -> dict:
    """Build event-specific metadata dict."""
    if event_type == "home_screen_view":
        return {"app_version": "1.0.0"}
    if event_type == "search":
        return {"search_keyword": random.choice(SEARCH_KEYWORDS), "result_count": random.randint(0, 50)}
    if event_type == "view_item":
        p = _random_product()
        return {"product_id": p["id"], "product_name": p["name"], "base_price": p["base_price"]}
    if event_type == "select_item_variant":
        p = _random_product()
        vtype = random.choice(["color", "size"])
        vvalue = random.choice(COLORS) if vtype == "color" else random.choice(SIZES)
        return {"product_id": p["id"], "variant_type": vtype, "variant_value": vvalue}
    if event_type == "add_to_cart":
        p = _random_product()
        qty = random.randint(1, 3)
        return {
            "product_id": p["id"], "product_name": p["name"],
            "variant_color": random.choice(COLORS), "variant_size": random.choice(SIZES),
            "base_price": p["base_price"], "quantity": qty,
            "cart_total_value": p["base_price"] * qty + random.randint(0, 3) * 250000,
        }
    if event_type == "view_cart":
        items = random.randint(1, 6)
        return {"total_items": items, "cart_total_value": items * random.choice([250000, 450000, 550000])}
    if event_type == "remove_from_cart":
        p = _random_product()
        return {
            "product_id": p["id"], "product_name": p["name"],
            "variant_color": random.choice(COLORS), "variant_size": random.choice(SIZES),
            "removed_quantity": random.randint(1, 2),
            "cart_total_value": random.randint(1, 4) * 350000,
        }
    if event_type == "update_cart_item":
        p = _random_product()
        old_qty = random.randint(1, 3)
        action = random.choice(["increase_quantity", "decrease_quantity", "change_variant"])
        new_qty = old_qty + 1 if action == "increase_quantity" else max(1, old_qty - 1)
        return {
            "product_id": p["id"], "action_type": action,
            "old_quantity": old_qty, "new_quantity": new_qty,
            "cart_total_value": new_qty * p["base_price"],
        }
    if event_type == "begin_checkout":
        items = random.randint(1, 5)
        return {
            "total_items": items,
            "cart_total_value": items * random.choice([250000, 450000, 550000]),
            "items_list": [_random_product()["id"] for _ in range(items)],
        }
    if event_type == "add_shipping_info":
        return {
            "shipping_method": random.choice(SHIPPING_METHODS),
            "shipping_fee": random.choice([0, 15000, 25000, 35000]),
            "city_province": random.choice(CITIES),
        }
    if event_type == "add_coupon":
        is_valid = 1 if random.random() > 0.2 else 0
        meta = {
            "promotion_id": random.choice(PROMOTION_IDS),
            "promotion_type": random.choice(PROMOTION_TYPES),
            "discount_amount": random.choice([0, 20000, 50000, 100000]),
            "is_valid": is_valid,
        }
        if is_valid == 0:
            meta["error_message"] = random.choice(COUPON_ERRORS)
        return meta
    if event_type == "add_payment_info":
        return {"payment_method": random.choice(PAYMENT_METHODS), "final_amount": random.randint(2, 8) * 100000}
    if event_type == "place_order":
        return {
            "order_id": f"ord_{uuid.uuid4().hex[:12]}",
            "payment_method": random.choice(PAYMENT_METHODS),
            "final_amount": random.randint(2, 8) * 100000,
        }
    if event_type == "payment_callback":
        is_success = 1 if random.random() > 0.1 else 0
        meta = {
            "order_id": f"ord_{uuid.uuid4().hex[:12]}",
            "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
            "payment_method": random.choice(PAYMENT_METHODS),
            "is_success": is_success,
        }
        if is_success == 0:
            meta["error_code"] = random.choice(PAYMENT_ERRORS)
        return meta
    return {}


def generate_event() -> dict:
    """Generate a single fake apparel retail clickstream event."""
    event_id = random.choices(list(EVENT_TYPES.keys()), weights=EVENT_WEIGHTS, k=1)[0]
    event_type = EVENT_TYPES[event_id]
    user_id = f"user_{random.randint(1000, 5000)}"
    now = datetime.now()
    return {
        "event_uuid": str(uuid.uuid4()),
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": int(now.timestamp()),
        "log_date": now.strftime("%Y-%m-%d"),
        "created_at": now.isoformat(),
        "session_id": str(uuid.uuid4()),
        "user_id": user_id,
        "device_os": random.choice(["iOS", "Android"]),
        "app_version": random.choice(["1.0.0", "1.1.0", "1.1.5", "2.0.0-beta"]),
        "metadata": _build_metadata(event_type),
    }
