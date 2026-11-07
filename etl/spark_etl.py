#!/usr/bin/env python3
"""
E-Commerce Data Lakehouse — PySpark ETL Processing
====================================================
Implements the Medallion Architecture (Bronze → Silver → Gold) ETL pipeline.

Bronze Layer: Raw ingested data from S3 (as-is from source systems)
Silver Layer: Cleaned, validated, deduplicated, and typed data
Gold Layer:   Business-level aggregations and denormalized analytical tables

This script simulates an AWS Glue job running PySpark transformations.
"""

import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    TimestampType,
)
from pyspark.sql.window import Window

from schema_definitions import (
    CLICKSTREAM_SCHEMA,
    CUSTOMER_SCHEMA,
    LINE_ITEM_SCHEMA,
    PRODUCT_SCHEMA,
    REVIEW_SCHEMA,
    TRANSACTION_SCHEMA,
)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spark_etl")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "lakehouse_secret_2024")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "ecommerce-raw")
S3_BUCKET_CURATED = os.getenv("S3_BUCKET_CURATED", "ecommerce-curated")


def create_spark_session() -> SparkSession:
    """Create and configure a SparkSession with S3/MinIO support."""
    spark = (
        SparkSession.builder
        .appName("ECommerce-DataLakehouse-ETL")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: %s", spark.sparkContext.applicationId)
    return spark


# ──────────────────────────────────────────────
# Helper Functions
# ──────────────────────────────────────────────


def log_dataframe_info(df: DataFrame, name: str) -> None:
    """Log basic DataFrame statistics."""
    count = df.count()
    cols = len(df.columns)
    logger.info("DataFrame [%s]: %d rows × %d columns", name, count, cols)


def add_audit_columns(df: DataFrame, layer: str) -> DataFrame:
    """Add standard audit/metadata columns to a DataFrame."""
    return (
        df
        .withColumn("_etl_layer", F.lit(layer))
        .withColumn("_etl_timestamp", F.current_timestamp())
        .withColumn("_etl_date", F.current_date())
        .withColumn("_etl_version", F.lit("1.0.0"))
    )


def ensure_curated_bucket(spark: SparkSession) -> None:
    """Ensure the curated S3 bucket exists using boto3."""
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=S3_BUCKET_CURATED)
    except Exception:
        s3.create_bucket(Bucket=S3_BUCKET_CURATED)
        logger.info("Created curated bucket: %s", S3_BUCKET_CURATED)


# ──────────────────────────────────────────────
# BRONZE LAYER — Raw Data Ingestion
# ──────────────────────────────────────────────


def read_bronze_transactions(spark: SparkSession) -> DataFrame:
    """Read raw transaction JSON data from the Bronze layer."""
    logger.info("Reading Bronze transactions from s3a://%s/transactions/", S3_BUCKET_RAW)
    df = (
        spark.read
        .schema(TRANSACTION_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(f"s3a://{S3_BUCKET_RAW}/transactions/")
    )
    log_dataframe_info(df, "bronze_transactions")
    return df


def read_bronze_clickstream(spark: SparkSession) -> DataFrame:
    """Read raw clickstream JSON data from the Bronze layer."""
    logger.info("Reading Bronze clickstream from s3a://%s/clickstream/", S3_BUCKET_RAW)
    df = (
        spark.read
        .schema(CLICKSTREAM_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(f"s3a://{S3_BUCKET_RAW}/clickstream/")
    )
    log_dataframe_info(df, "bronze_clickstream")
    return df


def read_bronze_reviews(spark: SparkSession) -> DataFrame:
    """Read raw review JSON data from the Bronze layer."""
    logger.info("Reading Bronze reviews from s3a://%s/reviews/", S3_BUCKET_RAW)
    df = (
        spark.read
        .schema(REVIEW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(f"s3a://{S3_BUCKET_RAW}/reviews/")
    )
    log_dataframe_info(df, "bronze_reviews")
    return df


def read_bronze_customers(spark: SparkSession) -> DataFrame:
    """Read raw customer CSV data from the Bronze layer."""
    logger.info("Reading Bronze customers from s3a://%s/customers/", S3_BUCKET_RAW)
    df = (
        spark.read
        .schema(CUSTOMER_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv(f"s3a://{S3_BUCKET_RAW}/customers/")
    )
    log_dataframe_info(df, "bronze_customers")
    return df


def read_bronze_products(spark: SparkSession) -> DataFrame:
    """Read raw product CSV data from the Bronze layer."""
    logger.info("Reading Bronze products from s3a://%s/products/", S3_BUCKET_RAW)
    df = (
        spark.read
        .schema(PRODUCT_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv(f"s3a://{S3_BUCKET_RAW}/products/")
    )
    log_dataframe_info(df, "bronze_products")
    return df


# ──────────────────────────────────────────────
# SILVER LAYER — Cleaning & Validation
# ──────────────────────────────────────────────


def clean_transactions(df: DataFrame) -> DataFrame:
    """
    Clean and validate transaction data:
    - Drop records with missing transaction_id or order_id
    - Fill missing customer_id with 'UNKNOWN'
    - Recalculate grand_total if missing
    - Parse order_date to timestamp
    - Validate order_status values
    - Deduplicate by transaction_id
    """
    logger.info("Cleaning transactions (Silver layer)...")

    valid_statuses = ["completed", "shipped", "processing", "cancelled", "refunded"]

    df_clean = (
        df
        # Drop completely invalid records
        .filter(
            F.col("transaction_id").isNotNull() &
            (F.col("transaction_id") != "") &
            F.col("order_id").isNotNull() &
            (F.col("order_id") != "")
        )
        # Handle missing customer_id
        .withColumn(
            "customer_id",
            F.when(
                F.col("customer_id").isNull() | (F.col("customer_id") == ""),
                F.lit("UNKNOWN"),
            ).otherwise(F.col("customer_id")),
        )
        # Parse order_date
        .withColumn("order_date", F.to_timestamp("order_date"))
        # Validate and normalize order_status
        .withColumn(
            "order_status",
            F.when(
                F.lower(F.col("order_status")).isin(valid_statuses),
                F.lower(F.col("order_status")),
            ).otherwise(F.lit("unknown")),
        )
        # Recalculate subtotal if missing
        .withColumn(
            "subtotal",
            F.when(F.col("subtotal").isNull(), F.col("grand_total") - F.col("tax_amount") - F.col("shipping_cost"))
            .otherwise(F.col("subtotal")),
        )
        # Ensure non-negative monetary values
        .withColumn("grand_total", F.abs(F.col("grand_total")))
        .withColumn("subtotal", F.abs(F.col("subtotal")))
        .withColumn("tax_amount", F.abs(F.col("tax_amount")))
        .withColumn("shipping_cost", F.abs(F.col("shipping_cost")))
        # Add date partition columns
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_day", F.dayofmonth("order_date"))
    )

    # Deduplicate by transaction_id (keep latest processing_timestamp)
    window = Window.partitionBy("transaction_id").orderBy(F.desc("processing_timestamp"))
    df_deduped = (
        df_clean
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    df_final = add_audit_columns(df_deduped, "silver")
    log_dataframe_info(df_final, "silver_transactions")
    return df_final


def clean_clickstream(df: DataFrame) -> DataFrame:
    """
    Clean and validate clickstream data:
    - Drop records with missing event_id
    - Parse event_timestamp to timestamp
    - Validate event_type values
    - Cap duration_seconds at reasonable maximum
    - Deduplicate by event_id
    """
    logger.info("Cleaning clickstream (Silver layer)...")

    valid_event_types = [
        "page_view", "product_view", "search", "add_to_cart",
        "remove_from_cart", "begin_checkout", "purchase",
        "wishlist_add", "filter_apply", "sort_change", "review_read",
    ]

    df_clean = (
        df
        .filter(F.col("event_id").isNotNull() & (F.col("event_id") != ""))
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn(
            "event_type",
            F.when(F.col("event_type").isin(valid_event_types), F.col("event_type"))
            .otherwise(F.lit("unknown")),
        )
        # Cap duration at 1 hour
        .withColumn(
            "duration_seconds",
            F.when(F.col("duration_seconds") > 3600, F.lit(3600))
            .otherwise(F.col("duration_seconds")),
        )
        # Add date partition columns
        .withColumn("event_year", F.year("event_timestamp"))
        .withColumn("event_month", F.month("event_timestamp"))
        .withColumn("event_day", F.dayofmonth("event_timestamp"))
        # Fill unknown customer_id
        .withColumn(
            "customer_id",
            F.when(F.col("customer_id").isNull(), F.lit("ANONYMOUS"))
            .otherwise(F.col("customer_id")),
        )
    )

    # Deduplicate
    df_deduped = df_clean.dropDuplicates(["event_id"])

    df_final = add_audit_columns(df_deduped, "silver")
    log_dataframe_info(df_final, "silver_clickstream")
    return df_final


def clean_reviews(df: DataFrame) -> DataFrame:
    """
    Clean and validate review data:
    - Drop records with missing review_id or product_id
    - Clamp ratings to 1-5 range
    - Handle empty review text
    - Parse review_date
    - Deduplicate by review_id
    """
    logger.info("Cleaning reviews (Silver layer)...")

    df_clean = (
        df
        .filter(
            F.col("review_id").isNotNull() &
            F.col("product_id").isNotNull() &
            (F.col("review_id") != "") &
            (F.col("product_id") != "")
        )
        # Clamp rating to valid range
        .withColumn(
            "rating",
            F.when(F.col("rating").isNull(), F.lit(3))
            .when(F.col("rating") < 1, F.lit(1))
            .when(F.col("rating") > 5, F.lit(5))
            .otherwise(F.col("rating")),
        )
        # Handle empty review text
        .withColumn(
            "review_text",
            F.when(
                F.col("review_text").isNull() | (F.trim(F.col("review_text")) == ""),
                F.lit("No review text provided."),
            ).otherwise(F.col("review_text")),
        )
        # Parse review_date
        .withColumn("review_date", F.to_timestamp("review_date"))
        # Ensure non-negative vote counts
        .withColumn("helpful_votes", F.abs(F.coalesce(F.col("helpful_votes"), F.lit(0))))
        .withColumn("unhelpful_votes", F.abs(F.coalesce(F.col("unhelpful_votes"), F.lit(0))))
        # Add sentiment label based on rating
        .withColumn(
            "sentiment",
            F.when(F.col("rating") >= 4, F.lit("positive"))
            .when(F.col("rating") == 3, F.lit("neutral"))
            .otherwise(F.lit("negative")),
        )
        # Add date partition columns
        .withColumn("review_year", F.year("review_date"))
        .withColumn("review_month", F.month("review_date"))
    )

    df_deduped = df_clean.dropDuplicates(["review_id"])
    df_final = add_audit_columns(df_deduped, "silver")
    log_dataframe_info(df_final, "silver_reviews")
    return df_final


def clean_customers(df: DataFrame) -> DataFrame:
    """Clean and validate customer data."""
    logger.info("Cleaning customers (Silver layer)...")

    df_clean = (
        df
        .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn(
            "age",
            F.when((F.col("age") < 13) | (F.col("age") > 120), F.lit(None))
            .otherwise(F.col("age")),
        )
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        .withColumn(
            "loyalty_tier",
            F.when(
                F.col("loyalty_tier").isin("bronze", "silver", "gold", "platinum"),
                F.col("loyalty_tier"),
            ).otherwise(F.lit("bronze")),
        )
    )

    df_deduped = df_clean.dropDuplicates(["customer_id"])
    df_final = add_audit_columns(df_deduped, "silver")
    log_dataframe_info(df_final, "silver_customers")
    return df_final


def clean_products(df: DataFrame) -> DataFrame:
    """Clean and validate product data."""
    logger.info("Cleaning products (Silver layer)...")

    df_clean = (
        df
        .filter(F.col("product_id").isNotNull() & (F.col("product_id") != ""))
        .withColumn("created_date", F.to_date("created_date"))
        .withColumn(
            "price",
            F.when(F.col("price") <= 0, F.lit(None)).otherwise(F.col("price")),
        )
        .withColumn(
            "cost",
            F.when(F.col("cost") <= 0, F.lit(None)).otherwise(F.col("cost")),
        )
        .withColumn(
            "rating_avg",
            F.when((F.col("rating_avg") < 1) | (F.col("rating_avg") > 5), F.lit(None))
            .otherwise(F.col("rating_avg")),
        )
        .withColumn(
            "profit_margin",
            F.when(
                F.col("price").isNotNull() & F.col("cost").isNotNull() & (F.col("price") > 0),
                F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 2),
            ).otherwise(F.lit(None)),
        )
    )

    df_deduped = df_clean.dropDuplicates(["product_id"])
    df_final = add_audit_columns(df_deduped, "silver")
    log_dataframe_info(df_final, "silver_products")
    return df_final


# ──────────────────────────────────────────────
# GOLD LAYER — Business Aggregations
# ──────────────────────────────────────────────


def build_gold_order_facts(transactions_df: DataFrame) -> DataFrame:
    """
    Gold Layer: Flattened order fact table with exploded line items.
    Each row = one line item in an order (denormalized).
    """
    logger.info("Building Gold order facts...")

    df_exploded = (
        transactions_df
        .select(
            "transaction_id", "order_id", "customer_id", "order_date",
            "order_status", "payment_method", "subtotal", "tax_rate",
            "tax_amount", "shipping_cost", "grand_total", "currency",
            "shipping_address_city", "shipping_address_state",
            "shipping_address_zip", "source_system",
            "order_year", "order_month", "order_day",
            F.explode_outer("line_items").alias("item"),
        )
        .select(
            "transaction_id", "order_id", "customer_id", "order_date",
            "order_status", "payment_method", "subtotal", "tax_rate",
            "tax_amount", "shipping_cost", "grand_total", "currency",
            "shipping_address_city", "shipping_address_state",
            "shipping_address_zip", "source_system",
            "order_year", "order_month", "order_day",
            F.col("item.product_id").alias("product_id"),
            F.col("item.sku").alias("sku"),
            F.col("item.product_name").alias("product_name"),
            F.col("item.category").alias("product_category"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.unit_price").alias("unit_price"),
            F.col("item.discount_percent").alias("discount_percent"),
            F.col("item.line_total").alias("line_total"),
        )
    )

    df_final = add_audit_columns(df_exploded, "gold")
    log_dataframe_info(df_final, "gold_order_facts")
    return df_final


def build_gold_daily_sales_summary(order_facts_df: DataFrame) -> DataFrame:
    """Gold Layer: Daily sales summary aggregation."""
    logger.info("Building Gold daily sales summary...")

    df_agg = (
        order_facts_df
        .filter(F.col("order_status") == "completed")
        .groupBy("order_year", "order_month", "order_day")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("line_total").alias("total_revenue"),
            F.sum("quantity").alias("total_items_sold"),
            F.avg("line_total").alias("avg_line_total"),
            F.avg("grand_total").alias("avg_order_value"),
            F.count("*").alias("total_line_items"),
        )
        .withColumn("revenue_per_customer", F.round(F.col("total_revenue") / F.col("unique_customers"), 2))
        .orderBy("order_year", "order_month", "order_day")
    )

    df_final = add_audit_columns(df_agg, "gold")
    log_dataframe_info(df_final, "gold_daily_sales")
    return df_final


def build_gold_customer_360(
    transactions_df: DataFrame,
    clickstream_df: DataFrame,
    reviews_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    """
    Gold Layer: Customer 360-degree view.
    Combines transaction, clickstream, review, and profile data per customer.
    """
    logger.info("Building Gold Customer 360 view...")

    # Transaction aggregations per customer
    txn_agg = (
        transactions_df
        .filter(F.col("customer_id") != "UNKNOWN")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("grand_total").alias("total_spend"),
            F.avg("grand_total").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("payment_method").alias("payment_methods_used"),
        )
    )

    # Clickstream aggregations per customer
    click_agg = (
        clickstream_df
        .filter(F.col("customer_id") != "ANONYMOUS")
        .groupBy("customer_id")
        .agg(
            F.count("event_id").alias("total_events"),
            F.countDistinct("session_id").alias("total_sessions"),
            F.avg("duration_seconds").alias("avg_page_duration"),
            F.sum(
                F.when(F.col("event_type") == "purchase", 1).otherwise(0)
            ).alias("purchase_events"),
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
            ).alias("cart_add_events"),
        )
    )

    # Review aggregations per customer
    review_agg = (
        reviews_df
        .groupBy("customer_id")
        .agg(
            F.count("review_id").alias("total_reviews"),
            F.avg("rating").alias("avg_rating_given"),
            F.sum("helpful_votes").alias("total_helpful_votes_received"),
        )
    )

    # Join all aggregations with customer profile
    customer_360 = (
        customers_df
        .join(txn_agg, "customer_id", "left")
        .join(click_agg, "customer_id", "left")
        .join(review_agg, "customer_id", "left")
        .withColumn("total_orders", F.coalesce(F.col("total_orders"), F.lit(0)))
        .withColumn("total_spend", F.coalesce(F.col("total_spend"), F.lit(0.0)))
        .withColumn("total_events", F.coalesce(F.col("total_events"), F.lit(0)))
        .withColumn("total_reviews", F.coalesce(F.col("total_reviews"), F.lit(0)))
        .withColumn(
            "customer_lifetime_days",
            F.datediff(F.current_date(), F.col("signup_date")),
        )
        .withColumn(
            "days_since_last_order",
            F.datediff(F.current_date(), F.col("last_order_date")),
        )
        .withColumn(
            "engagement_score",
            F.round(
                (F.coalesce(F.col("total_orders"), F.lit(0)) * 10 +
                 F.coalesce(F.col("total_events"), F.lit(0)) * 0.1 +
                 F.coalesce(F.col("total_reviews"), F.lit(0)) * 5),
                2,
            ),
        )
    )

    df_final = add_audit_columns(customer_360, "gold")
    log_dataframe_info(df_final, "gold_customer_360")
    return df_final


def build_gold_product_performance(
    order_facts_df: DataFrame,
    reviews_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Gold Layer: Product performance analytics."""
    logger.info("Building Gold product performance...")

    # Sales aggregations per product
    sales_agg = (
        order_facts_df
        .filter(F.col("order_status") == "completed")
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.sum("line_total").alias("total_revenue"),
            F.avg("unit_price").alias("avg_selling_price"),
            F.avg("discount_percent").alias("avg_discount"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_buyers"),
        )
    )

    # Review aggregations per product
    review_agg = (
        reviews_df
        .groupBy("product_id")
        .agg(
            F.count("review_id").alias("review_count"),
            F.avg("rating").alias("avg_review_rating"),
            F.sum("helpful_votes").alias("total_helpful_votes"),
            F.sum(
                F.when(F.col("sentiment") == "positive", 1).otherwise(0)
            ).alias("positive_reviews"),
            F.sum(
                F.when(F.col("sentiment") == "negative", 1).otherwise(0)
            ).alias("negative_reviews"),
        )
    )

    product_perf = (
        products_df
        .join(sales_agg, "product_id", "left")
        .join(review_agg, "product_id", "left")
        .withColumn("total_units_sold", F.coalesce(F.col("total_units_sold"), F.lit(0)))
        .withColumn("total_revenue", F.coalesce(F.col("total_revenue"), F.lit(0.0)))
        .withColumn("review_count", F.coalesce(F.col("review_count"), F.lit(0)))
        .withColumn(
            "revenue_per_unit",
            F.when(F.col("total_units_sold") > 0,
                   F.round(F.col("total_revenue") / F.col("total_units_sold"), 2))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "sentiment_ratio",
            F.when(F.col("review_count") > 0,
                   F.round(F.col("positive_reviews") / F.col("review_count"), 2))
            .otherwise(F.lit(None)),
        )
    )

    df_final = add_audit_columns(product_perf, "gold")
    log_dataframe_info(df_final, "gold_product_performance")
    return df_final


# ──────────────────────────────────────────────
# Write Functions
# ──────────────────────────────────────────────


def write_silver_layer(df: DataFrame, table_name: str, partition_cols: list[str] | None = None) -> None:
    """Write a Silver layer DataFrame to S3 in Parquet format."""
    path = f"s3a://{S3_BUCKET_CURATED}/silver/{table_name}"
    logger.info("Writing Silver layer → %s", path)

    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    logger.info("Silver layer written: %s", table_name)


def write_gold_layer(df: DataFrame, table_name: str, partition_cols: list[str] | None = None) -> None:
    """Write a Gold layer DataFrame to S3 in Parquet format."""
    path = f"s3a://{S3_BUCKET_CURATED}/gold/{table_name}"
    logger.info("Writing Gold layer → %s", path)

    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    logger.info("Gold layer written: %s", table_name)


def write_for_warehouse_loading(df: DataFrame, table_name: str) -> None:
    """Write a DataFrame as CSV for warehouse bulk loading."""
    path = f"s3a://{S3_BUCKET_CURATED}/warehouse_staging/{table_name}"
    logger.info("Writing warehouse staging CSV → %s", path)

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(path)
    )
    logger.info("Warehouse staging written: %s", table_name)


# ──────────────────────────────────────────────
# Main ETL Pipeline
# ──────────────────────────────────────────────


def main() -> None:
    """Execute the complete ETL pipeline."""
    logger.info("=" * 60)
    logger.info("E-Commerce Data Lakehouse — PySpark ETL Starting")
    logger.info("=" * 60)

    spark = create_spark_session()
    ensure_curated_bucket(spark)

    try:
        # ─── BRONZE LAYER (Read Raw) ────────────────
        logger.info("── BRONZE LAYER ──")
        bronze_transactions = read_bronze_transactions(spark)
        bronze_clickstream = read_bronze_clickstream(spark)
        bronze_reviews = read_bronze_reviews(spark)
        bronze_customers = read_bronze_customers(spark)
        bronze_products = read_bronze_products(spark)

        # ─── SILVER LAYER (Clean & Validate) ────────
        logger.info("── SILVER LAYER ──")
        silver_transactions = clean_transactions(bronze_transactions)
        silver_clickstream = clean_clickstream(bronze_clickstream)
        silver_reviews = clean_reviews(bronze_reviews)
        silver_customers = clean_customers(bronze_customers)
        silver_products = clean_products(bronze_products)

        # Write Silver layer
        write_silver_layer(silver_transactions, "transactions", ["order_year", "order_month"])
        write_silver_layer(silver_clickstream, "clickstream", ["event_year", "event_month"])
        write_silver_layer(silver_reviews, "reviews", ["review_year", "review_month"])
        write_silver_layer(silver_customers, "customers")
        write_silver_layer(silver_products, "products")

        # ─── GOLD LAYER (Business Aggregations) ─────
        logger.info("── GOLD LAYER ──")
        gold_order_facts = build_gold_order_facts(silver_transactions)
        gold_daily_sales = build_gold_daily_sales_summary(gold_order_facts)
        gold_customer_360 = build_gold_customer_360(
            silver_transactions, silver_clickstream, silver_reviews, silver_customers
        )
        gold_product_perf = build_gold_product_performance(
            gold_order_facts, silver_reviews, silver_products
        )

        # Write Gold layer
        write_gold_layer(gold_order_facts, "order_facts", ["order_year", "order_month"])
        write_gold_layer(gold_daily_sales, "daily_sales_summary")
        write_gold_layer(gold_customer_360, "customer_360")
        write_gold_layer(gold_product_perf, "product_performance")

        # Write warehouse staging CSVs
        write_for_warehouse_loading(gold_order_facts, "order_facts")
        write_for_warehouse_loading(gold_daily_sales, "daily_sales_summary")
        write_for_warehouse_loading(gold_customer_360, "customer_360")
        write_for_warehouse_loading(gold_product_perf, "product_performance")

        # ─── Summary ────────────────────────────────
        logger.info("=" * 60)
        logger.info("ETL Pipeline Complete!")
        logger.info("  Silver tables: transactions, clickstream, reviews, customers, products")
        logger.info("  Gold tables:   order_facts, daily_sales_summary, customer_360, product_performance")
        logger.info("=" * 60)

    except Exception as exc:
        logger.exception("ETL pipeline failed: %s", exc)
        raise
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
