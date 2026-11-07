#!/usr/bin/env python3
"""
E-Commerce Data Lakehouse — Schema Definitions
================================================
PySpark StructType schemas for all data sources.
Used for schema validation and enforcement during ETL processing.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ──────────────────────────────────────────────
# Line Item Schema (nested inside transactions)
# ──────────────────────────────────────────────

LINE_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("sku", StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("discount_percent", IntegerType(), nullable=True),
    StructField("line_total", DoubleType(), nullable=False),
])

# ──────────────────────────────────────────────
# Transaction Schema (Bronze / raw)
# ──────────────────────────────────────────────

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("order_date", StringType(), nullable=False),
    StructField("order_status", StringType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    StructField("subtotal", DoubleType(), nullable=True),
    StructField("tax_rate", DoubleType(), nullable=True),
    StructField("tax_amount", DoubleType(), nullable=True),
    StructField("shipping_cost", DoubleType(), nullable=True),
    StructField("grand_total", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("shipping_address_city", StringType(), nullable=True),
    StructField("shipping_address_state", StringType(), nullable=True),
    StructField("shipping_address_zip", StringType(), nullable=True),
    StructField("line_items", ArrayType(LINE_ITEM_SCHEMA), nullable=True),
    StructField("line_item_count", IntegerType(), nullable=True),
    StructField("source_system", StringType(), nullable=True),
    StructField("processing_timestamp", StringType(), nullable=True),
])

# ──────────────────────────────────────────────
# Clickstream Event Schema
# ──────────────────────────────────────────────

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_timestamp", StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("device_type", StringType(), nullable=True),
    StructField("browser", StringType(), nullable=True),
    StructField("os", StringType(), nullable=True),
    StructField("ip_address", StringType(), nullable=True),
    StructField("user_agent", StringType(), nullable=True),
    StructField("page_url", StringType(), nullable=True),
    StructField("page_category", StringType(), nullable=True),
    StructField("referrer_url", StringType(), nullable=True),
    StructField("duration_seconds", IntegerType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("search_query", StringType(), nullable=True),
    StructField("search_results_count", IntegerType(), nullable=True),
    StructField("geo_country", StringType(), nullable=True),
    StructField("geo_region", StringType(), nullable=True),
    StructField("geo_city", StringType(), nullable=True),
])

# ──────────────────────────────────────────────
# Product Review Schema
# ──────────────────────────────────────────────

REVIEW_SCHEMA = StructType([
    StructField("review_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("rating", IntegerType(), nullable=True),
    StructField("review_title", StringType(), nullable=True),
    StructField("review_text", StringType(), nullable=True),
    StructField("review_date", StringType(), nullable=True),
    StructField("verified_purchase", BooleanType(), nullable=True),
    StructField("helpful_votes", IntegerType(), nullable=True),
    StructField("unhelpful_votes", IntegerType(), nullable=True),
    StructField("response_from_seller", StringType(), nullable=True),
    StructField("images_count", IntegerType(), nullable=True),
])

# ──────────────────────────────────────────────
# Customer Profile Schema (CSV input)
# ──────────────────────────────────────────────

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("email", StringType(), nullable=True),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("phone", StringType(), nullable=True),
    StructField("address_line1", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip_code", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),
    StructField("loyalty_tier", StringType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=True),
    StructField("preferred_payment", StringType(), nullable=True),
    StructField("marketing_opt_in", BooleanType(), nullable=True),
    StructField("referral_source", StringType(), nullable=True),
])

# ──────────────────────────────────────────────
# Product Catalog Schema (CSV input)
# ──────────────────────────────────────────────

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("sku", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("sub_category", StringType(), nullable=True),
    StructField("brand", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("cost", DoubleType(), nullable=True),
    StructField("weight_kg", DoubleType(), nullable=True),
    StructField("inventory_count", IntegerType(), nullable=True),
    StructField("rating_avg", DoubleType(), nullable=True),
    StructField("review_count", IntegerType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=True),
    StructField("created_date", StringType(), nullable=True),
    StructField("supplier_id", StringType(), nullable=True),
    StructField("tags", StringType(), nullable=True),
])
