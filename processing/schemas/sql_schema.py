"""
Spark StructType schemas for MySQL raw layer tables.
Used by batch_bronze_*.py when reading via JDBC.
"""

from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType,
    StringType, StructField, StructType, TimestampType,
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",          StringType(),    False),
    StructField("first_name",           StringType(),    True),
    StructField("last_name",            StringType(),    True),
    StructField("phone",                StringType(),    True),
    StructField("dob",                  DateType(),      True),
    StructField("age",                  IntegerType(),   True),
    StructField("gender",               IntegerType(),   True),
    StructField("address_line",         StringType(),    True),
    StructField("is_deleted",           IntegerType(),   False),
    StructField("registered_datetime",  TimestampType(), False),
    StructField("created_at",           TimestampType(), False),
    StructField("updated_at",           TimestampType(), False),
    StructField("source",               StringType(),    False),
])

CATEGORY_SCHEMA = StructType([
    StructField("category_id",   StringType(),    False),
    StructField("category_name", StringType(),    False),
    StructField("is_current",    BooleanType(),   False),
    StructField("created_at",    TimestampType(), False),
    StructField("updated_at",    TimestampType(), False),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",            StringType(),       False),
    StructField("product_name",          StringType(),       False),
    StructField("product_display_name",  StringType(),       True),
    StructField("category_id",           StringType(),       False),
    StructField("sales_unit",            StringType(),       True),
    StructField("color",                 StringType(),       False),
    StructField("size",                  StringType(),       False),
    StructField("unit_price",            DecimalType(15, 2), False),
    StructField("created_at",            TimestampType(),    False),
    StructField("updated_at",            TimestampType(),    False),
    StructField("is_current",            BooleanType(),      False),
])

BRANCHES_SCHEMA = StructType([
    StructField("branch_id",     StringType(),    False),
    StructField("branch_name",   StringType(),    False),
    StructField("branch_type",   StringType(),    False),
    StructField("region",        StringType(),    True),
    StructField("city_province", StringType(),    True),
    StructField("ward",          StringType(),    True),
    StructField("status",        StringType(),    False),
    StructField("open_date",     DateType(),      False),
    StructField("created_at",    TimestampType(), False),
    StructField("updated_at",    TimestampType(), False),
    StructField("is_current",    BooleanType(),   False),
])

POS_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id",       StringType(),          False),
    StructField("branch_id",            StringType(),          False),
    StructField("transaction_datetime", TimestampType(),       False),
    StructField("payment_type",         StringType(),          True),
    StructField("trans_total_amount",   DecimalType(15, 2),    False),
    StructField("trans_total_line",     IntegerType(),         False),
    StructField("trans_total_sell_sku", IntegerType(),         False),
    StructField("customer_id",          StringType(),          True),
    StructField("order_status",         StringType(),          False),
    StructField("delivery_date",        DateType(),            True),
    StructField("created_at",           TimestampType(),       False),
    StructField("updated_at",           TimestampType(),       False),
])

POS_TRANSACTION_DETAILS_SCHEMA = StructType([
    StructField("transaction_detail_id", StringType(),       False),
    StructField("transaction_id",        StringType(),       False),
    StructField("branch_id",             StringType(),       False),
    StructField("product_id",            StringType(),       False),
    StructField("is_promo",              IntegerType(),      False),
    StructField("product_uom_code",      StringType(),       True),
    StructField("trans_qty",             IntegerType(),      False),
    StructField("trans_line_amount",     DecimalType(15, 2), False),
    StructField("variant_size",          StringType(),       True),
    StructField("variant_color",         StringType(),       True),
    StructField("created_at",            TimestampType(),    False),
])

ONLINE_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id",       StringType(),       False),
    StructField("branch_id",            StringType(),       False),
    StructField("transaction_datetime", TimestampType(),    False),
    StructField("payment_type",         StringType(),       True),
    StructField("trans_total_amount",   DecimalType(15, 2), False),
    StructField("customer_id",          StringType(),       True),
    StructField("order_status",         StringType(),       False),
    StructField("delivery_date",        DateType(),         True),
    StructField("created_at",           TimestampType(),    False),
    StructField("updated_at",           TimestampType(),    False),
])

ONLINE_TRANSACTION_DETAILS_SCHEMA = StructType([
    StructField("transaction_detail_id", StringType(),       False),
    StructField("transaction_id",        StringType(),       False),
    StructField("product_id",            StringType(),       False),
    StructField("is_promo",              IntegerType(),      False),
    StructField("product_uom_code",      StringType(),       True),
    StructField("trans_qty",             IntegerType(),      False),
    StructField("trans_line_amount",     DecimalType(15, 2), False),
    StructField("variant_size",          StringType(),       True),
    StructField("variant_color",         StringType(),       True),
    StructField("created_at",            TimestampType(),    False),
])
