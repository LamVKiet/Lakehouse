"""
Shared Spark StructType schema matching the event JSON produced by data-simulator.
Used by streaming_to_bronze, batch_silver_transform, and batch_gold_aggregate.

Metadata is a superset of all possible fields across 13 event types — each event
only populates a subset; the rest are null.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType

# Superset schema for the nested metadata object inside each event
METADATA_SCHEMA = StructType([
    # search
    StructField("search_keyword", StringType()),
    StructField("result_count", IntegerType()),
    # view_item / add_to_cart / remove_from_cart
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("base_price", LongType()),
    # select_item_variant
    StructField("variant_type", StringType()),
    StructField("variant_value", StringType()),
    # add_to_cart / remove_from_cart
    StructField("variant_color", StringType()),
    StructField("variant_size", StringType()),
    StructField("quantity", IntegerType()),
    StructField("removed_quantity", IntegerType()),
    StructField("cart_total_value", LongType()),
    # view_cart / begin_checkout
    StructField("total_items", IntegerType()),
    StructField("items_list", ArrayType(StringType())),
    # update_cart_item
    StructField("action_type", StringType()),
    StructField("old_quantity", IntegerType()),
    StructField("new_quantity", IntegerType()),
    # add_shipping_info
    StructField("shipping_method", StringType()),
    StructField("shipping_fee", LongType()),
    StructField("city_province", StringType()),
    # add_coupon
    StructField("promotion_id", StringType()),
    StructField("promotion_type", StringType()),
    StructField("discount_amount", LongType()),
    StructField("is_valid", IntegerType()),
    StructField("error_message", StringType()),
    # add_payment_info / place_order / payment_callback
    StructField("payment_method", StringType()),
    StructField("final_amount", LongType()),
    StructField("order_id", StringType()),
    StructField("transaction_id", StringType()),
    # payment_callback
    StructField("is_success", IntegerType()),
    StructField("error_code", StringType()),
])

# Full event schema matching the JSON produced by data-simulator / producer
EVENT_SCHEMA = StructType([
    StructField("event_uuid", StringType()),
    StructField("event_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("timestamp", LongType()),
    StructField("log_date", StringType()),
    StructField("created_at", StringType()),
    StructField("user_id", StringType()),
    StructField("device_os", StringType()),
    StructField("app_version", StringType()),
    StructField("metadata", METADATA_SCHEMA),
])
