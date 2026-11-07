#!/usr/bin/env python3
"""
E-Commerce Data Lakehouse — Warehouse Loading Script
======================================================
Bulk-loads processed Parquet/CSV data from the curated S3 layer
into PostgreSQL analytical tables (simulating Amazon Redshift).

Loading Strategy:
  1. Download curated CSVs from S3 staging area
  2. Bulk-load into staging tables using COPY
  3. Transform and insert into analytics tables
  4. Refresh materialized views
  5. Validate row counts and data integrity
"""

import csv
import io
import logging
import os
import sys
import time
from pathlib import Path

import boto3
import psycopg2
import psycopg2.extras
from botocore.client import Config
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("warehouse_loader")

# S3 / MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "lakehouse_secret_2024")
S3_BUCKET_CURATED = os.getenv("S3_BUCKET_CURATED", "ecommerce-curated")

# PostgreSQL config
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "ecommerce_warehouse")
PG_USER = os.getenv("POSTGRES_USER", "warehouse_admin")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "warehouse_secret_2024")

DDL_FILE = os.getenv("DDL_FILE", "/app/warehouse/ddl.sql")


# ──────────────────────────────────────────────
# S3 Client
# ──────────────────────────────────────────────


def get_s3_client():
    """Create an S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ──────────────────────────────────────────────
# Database Connection
# ──────────────────────────────────────────────


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=2, max=30))
def get_db_connection():
    """Create a PostgreSQL connection with retry logic."""
    logger.info("Connecting to PostgreSQL at %s:%d/%s", PG_HOST, PG_PORT, PG_DB)
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )
    conn.autocommit = False
    logger.info("Connected to PostgreSQL successfully.")
    return conn


# ──────────────────────────────────────────────
# DDL Execution
# ──────────────────────────────────────────────


def execute_ddl(conn) -> None:
    """Execute the DDL script to create schemas, tables, views, and indexes."""
    logger.info("Executing DDL from: %s", DDL_FILE)
    with open(DDL_FILE, "r") as f:
        ddl_sql = f.read()

    with conn.cursor() as cur:
        cur.execute(ddl_sql)
    conn.commit()
    logger.info("DDL executed successfully.")


# ──────────────────────────────────────────────
# S3 Data Download
# ──────────────────────────────────────────────


def list_csv_files(s3_client, prefix: str) -> list[str]:
    """List all CSV files under a given S3 prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET_CURATED, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    logger.info("Found %d CSV files under s3://%s/%s", len(keys), S3_BUCKET_CURATED, prefix)
    return keys


def download_csv_from_s3(s3_client, key: str) -> str:
    """Download a CSV file from S3 and return its content as a string."""
    response = s3_client.get_object(Bucket=S3_BUCKET_CURATED, Key=key)
    content = response["Body"].read().decode("utf-8")
    logger.info("Downloaded s3://%s/%s (%d bytes)", S3_BUCKET_CURATED, key, len(content))
    return content


# ──────────────────────────────────────────────
# Bulk Loading Functions
# ──────────────────────────────────────────────


def bulk_load_csv_to_table(conn, csv_content: str, table_name: str) -> int:
    """
    Bulk-load CSV data into a staging table using COPY.
    Returns the number of rows loaded.
    """
    # Read CSV to determine columns
    reader = csv.DictReader(io.StringIO(csv_content))
    rows = list(reader)
    if not rows:
        logger.warning("No data found for table %s", table_name)
        return 0

    columns = list(rows[0].keys())

    # Use COPY for efficient bulk loading
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    with conn.cursor() as cur:
        # Truncate staging table first
        cur.execute(f"TRUNCATE TABLE {table_name}")
        # COPY data
        cur.execute(f"SET datestyle = 'ISO, MDY'")
        copy_sql = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER NULL ''"
        cur.copy_expert(copy_sql, buf)

    conn.commit()
    logger.info("Bulk-loaded %d rows into %s", len(rows), table_name)
    return len(rows)


def load_staging_tables(conn, s3_client) -> dict[str, int]:
    """Load all staging tables from S3 curated CSV files."""
    logger.info("── Loading Staging Tables ──")

    staging_mappings = {
        "warehouse_staging/order_facts": "staging.stg_order_facts",
        "warehouse_staging/daily_sales_summary": "staging.stg_daily_sales",
        "warehouse_staging/customer_360": "staging.stg_customer_360",
        "warehouse_staging/product_performance": "staging.stg_product_performance",
    }

    load_counts = {}
    for prefix, table_name in staging_mappings.items():
        csv_keys = list_csv_files(s3_client, prefix)
        total_rows = 0
        for key in csv_keys:
            csv_content = download_csv_from_s3(s3_client, key)
            total_rows += bulk_load_csv_to_table(conn, csv_content, table_name)
        load_counts[table_name] = total_rows
        logger.info("Total rows in %s: %d", table_name, total_rows)

    return load_counts


# ──────────────────────────────────────────────
# Analytics Table Population
# ──────────────────────────────────────────────


def populate_analytics_tables(conn) -> None:
    """Transform staging data and insert into analytics tables."""
    logger.info("── Populating Analytics Tables ──")

    with conn.cursor() as cur:
        # Fact Orders
        logger.info("Loading analytics.fact_orders...")
        cur.execute("TRUNCATE TABLE analytics.fact_orders RESTART IDENTITY CASCADE")
        cur.execute("""
            INSERT INTO analytics.fact_orders (
                transaction_id, order_id, customer_id, product_id,
                order_date, order_status, payment_method,
                subtotal, tax_rate, tax_amount, shipping_cost, grand_total,
                currency, shipping_city, shipping_state, shipping_zip,
                source_system, product_category, quantity, unit_price,
                discount_percent, line_total, order_year, order_month, order_day
            )
            SELECT
                transaction_id, order_id, customer_id, product_id,
                order_date, order_status, payment_method,
                subtotal, tax_rate, tax_amount, shipping_cost, grand_total,
                currency, shipping_address_city, shipping_address_state, shipping_address_zip,
                source_system, product_category, quantity, unit_price,
                COALESCE(discount_percent, 0), line_total, order_year, order_month, order_day
            FROM staging.stg_order_facts
            WHERE transaction_id IS NOT NULL
        """)
        fact_count = cur.rowcount
        logger.info("  → Inserted %d rows into analytics.fact_orders", fact_count)

        # Dim Customers
        logger.info("Loading analytics.dim_customers...")
        cur.execute("TRUNCATE TABLE analytics.dim_customers CASCADE")
        cur.execute("""
            INSERT INTO analytics.dim_customers (
                customer_id, email, first_name, last_name, gender, age,
                age_group, city, state, zip_code, country,
                signup_date, loyalty_tier, is_active, preferred_payment,
                marketing_opt_in, referral_source,
                total_orders, total_spend, avg_order_value,
                first_order_date, last_order_date,
                total_events, total_sessions, total_reviews,
                avg_rating_given, customer_lifetime_days,
                days_since_last_order, engagement_score
            )
            SELECT
                customer_id, email, first_name, last_name, gender, age,
                CASE
                    WHEN age < 25  THEN '18-24'
                    WHEN age < 35  THEN '25-34'
                    WHEN age < 45  THEN '35-44'
                    WHEN age < 55  THEN '45-54'
                    WHEN age < 65  THEN '55-64'
                    ELSE '65+'
                END AS age_group,
                city, state, zip_code, country,
                signup_date, loyalty_tier, COALESCE(is_active, TRUE), preferred_payment,
                COALESCE(marketing_opt_in, FALSE), referral_source,
                COALESCE(total_orders, 0), COALESCE(total_spend, 0), COALESCE(avg_order_value, 0),
                first_order_date, last_order_date,
                COALESCE(total_events, 0), COALESCE(total_sessions, 0), COALESCE(total_reviews, 0),
                avg_rating_given, customer_lifetime_days,
                days_since_last_order, COALESCE(engagement_score, 0)
            FROM staging.stg_customer_360
            WHERE customer_id IS NOT NULL
        """)
        cust_count = cur.rowcount
        logger.info("  → Inserted %d rows into analytics.dim_customers", cust_count)

        # Dim Products
        logger.info("Loading analytics.dim_products...")
        cur.execute("TRUNCATE TABLE analytics.dim_products CASCADE")
        cur.execute("""
            INSERT INTO analytics.dim_products (
                product_id, sku, name, category, sub_category, brand,
                price, cost, profit_margin, weight_kg, inventory_count,
                is_active, created_date, supplier_id, tags,
                total_units_sold, total_revenue, avg_selling_price,
                avg_discount, order_count, unique_buyers,
                review_count, avg_review_rating, sentiment_ratio, revenue_per_unit
            )
            SELECT
                product_id, sku, name, category, sub_category, brand,
                price, cost, profit_margin, weight_kg, inventory_count,
                COALESCE(is_active, TRUE), created_date, supplier_id, tags,
                COALESCE(total_units_sold, 0), COALESCE(total_revenue, 0), avg_selling_price,
                avg_discount, COALESCE(order_count, 0), COALESCE(unique_buyers, 0),
                COALESCE(review_count, 0), avg_review_rating, sentiment_ratio, revenue_per_unit
            FROM staging.stg_product_performance
            WHERE product_id IS NOT NULL
        """)
        prod_count = cur.rowcount
        logger.info("  → Inserted %d rows into analytics.dim_products", prod_count)

        # Agg Daily Sales
        logger.info("Loading analytics.agg_daily_sales...")
        cur.execute("TRUNCATE TABLE analytics.agg_daily_sales RESTART IDENTITY CASCADE")
        cur.execute("""
            INSERT INTO analytics.agg_daily_sales (
                order_year, order_month, order_day,
                total_orders, unique_customers, total_revenue,
                total_items_sold, avg_line_total, avg_order_value,
                total_line_items, revenue_per_customer
            )
            SELECT
                order_year, order_month, order_day,
                total_orders, unique_customers, total_revenue,
                total_items_sold, avg_line_total, avg_order_value,
                total_line_items, revenue_per_customer
            FROM staging.stg_daily_sales
            WHERE order_year IS NOT NULL
        """)
        sales_count = cur.rowcount
        logger.info("  → Inserted %d rows into analytics.agg_daily_sales", sales_count)

    conn.commit()
    logger.info("Analytics tables populated successfully.")


# ──────────────────────────────────────────────
# Materialized View Refresh
# ──────────────────────────────────────────────


def refresh_materialized_views(conn) -> None:
    """Refresh all materialized views in the reporting schema."""
    logger.info("Refreshing materialized views...")

    mat_views = [
        "reporting.mv_weekly_sales",
        "reporting.mv_category_performance",
    ]

    with conn.cursor() as cur:
        for view in mat_views:
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
                logger.info("  → Refreshed %s", view)
            except psycopg2.Error as e:
                logger.warning("  → Could not refresh %s concurrently, trying non-concurrent: %s", view, e)
                conn.rollback()
                cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
                logger.info("  → Refreshed %s (non-concurrent)", view)

    conn.commit()
    logger.info("Materialized views refreshed.")


# ──────────────────────────────────────────────
# Validation
# ──────────────────────────────────────────────


def validate_loads(conn) -> bool:
    """Validate data integrity after loading."""
    logger.info("── Validating Data Loads ──")

    validation_queries = {
        "analytics.fact_orders": "SELECT COUNT(*) FROM analytics.fact_orders",
        "analytics.dim_customers": "SELECT COUNT(*) FROM analytics.dim_customers",
        "analytics.dim_products": "SELECT COUNT(*) FROM analytics.dim_products",
        "analytics.agg_daily_sales": "SELECT COUNT(*) FROM analytics.agg_daily_sales",
    }

    all_valid = True
    with conn.cursor() as cur:
        for table, query in validation_queries.items():
            cur.execute(query)
            count = cur.fetchone()[0]
            status = "✓" if count > 0 else "✗ EMPTY"
            if count == 0:
                all_valid = False
            logger.info("  %s %s: %d rows", status, table, count)

        # Check reporting views
        cur.execute("SELECT COUNT(*) FROM reporting.v_monthly_revenue")
        mv_count = cur.fetchone()[0]
        logger.info("  %s reporting.v_monthly_revenue: %d rows", "✓" if mv_count > 0 else "✗", mv_count)

        cur.execute("SELECT COUNT(*) FROM reporting.v_customer_segments")
        cs_count = cur.fetchone()[0]
        logger.info("  %s reporting.v_customer_segments: %d rows", "✓" if cs_count > 0 else "✗", cs_count)

    return all_valid


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────


def main() -> None:
    """Execute the complete warehouse loading pipeline."""
    logger.info("=" * 60)
    logger.info("E-Commerce Data Lakehouse — Warehouse Loading Starting")
    logger.info("=" * 60)

    start_time = time.time()

    # Step 1: Connect to database
    conn = get_db_connection()

    try:
        # Step 2: Execute DDL
        execute_ddl(conn)

        # Step 3: Load staging tables from S3
        s3 = get_s3_client()
        load_counts = load_staging_tables(conn, s3)

        # Step 4: Populate analytics tables
        populate_analytics_tables(conn)

        # Step 5: Refresh materialized views
        refresh_materialized_views(conn)

        # Step 6: Validate
        is_valid = validate_loads(conn)

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Warehouse Loading Complete! (%.1f seconds)", elapsed)
        logger.info("  Staging loads: %s", load_counts)
        logger.info("  Validation: %s", "PASSED" if is_valid else "FAILED")
        logger.info("=" * 60)

        if not is_valid:
            sys.exit(1)

    except Exception as exc:
        logger.exception("Warehouse loading failed: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
