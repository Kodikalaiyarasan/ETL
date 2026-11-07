#!/usr/bin/env python3
"""
E-Commerce Data Lakehouse — Verification & Health Check Script
==============================================================
Validates that all pipeline stages completed and queries:
  1. MinIO: Confirms raw and curated buckets and data files exist
  2. PostgreSQL: Validates record counts across facts, dimensions, and views
  3. Elasticsearch: Validates indices document counts and executes sample queries
"""

import json
import logging
import os
import sys

import boto3
from botocore.client import Config
import psycopg2
from elasticsearch import Elasticsearch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("verifier")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "lakehouse_secret_2024")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "ecommerce-raw")
S3_BUCKET_CURATED = os.getenv("S3_BUCKET_CURATED", "ecommerce-curated")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "ecommerce_warehouse")
PG_USER = os.getenv("POSTGRES_USER", "warehouse_admin")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "warehouse_secret_2024")

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
ES_SCHEME = os.getenv("ELASTICSEARCH_SCHEME", "http")


def verify_s3():
    logger.info("=" * 60)
    logger.info("1. Verifying S3 / MinIO Object Storage")
    logger.info("=" * 60)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    for bucket in [S3_BUCKET_RAW, S3_BUCKET_CURATED]:
        try:
            objs = s3.list_objects_v2(Bucket=bucket)
            count = objs.get("KeyCount", 0)
            logger.info("  ✓ Bucket '%s' exists: %d objects found", bucket, count)
        except Exception as e:
            logger.error("  ✗ Error accessing bucket '%s': %s", bucket, e)


def verify_postgres():
    logger.info("=" * 60)
    logger.info("2. Verifying PostgreSQL Analytical Warehouse (Redshift Stand-in)")
    logger.info("=" * 60)
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    with conn.cursor() as cur:
        tables = [
            ("analytics.fact_orders", "SELECT COUNT(*) FROM analytics.fact_orders"),
            ("analytics.dim_customers", "SELECT COUNT(*) FROM analytics.dim_customers"),
            ("analytics.dim_products", "SELECT COUNT(*) FROM analytics.dim_products"),
            ("analytics.agg_daily_sales", "SELECT COUNT(*) FROM analytics.agg_daily_sales"),
            ("reporting.mv_weekly_sales", "SELECT COUNT(*) FROM reporting.mv_weekly_sales"),
            ("reporting.mv_category_performance", "SELECT COUNT(*) FROM reporting.mv_category_performance"),
        ]
        for name, query in tables:
            try:
                cur.execute(query)
                cnt = cur.fetchone()[0]
                logger.info("  ✓ %s: %d rows", name, cnt)
            except Exception as e:
                logger.error("  ✗ Failed querying %s: %s", name, e)

        # Sample analytic query: Top 3 product categories by revenue
        logger.info("\n  -- Top Product Categories by Revenue --")
        cur.execute("""
            SELECT product_category, SUM(line_total) as total_rev, SUM(quantity) as items_sold
            FROM analytics.fact_orders
            WHERE product_category IS NOT NULL
            GROUP BY product_category
            ORDER BY total_rev DESC
            LIMIT 3;
        """)
        for row in cur.fetchall():
            logger.info("    Category: %-25s | Rev: $%10.2f | Items: %d", row[0], float(row[1]), row[2])
    conn.close()


def verify_elasticsearch():
    logger.info("=" * 60)
    logger.info("3. Verifying Elasticsearch Search & Log Analytics")
    logger.info("=" * 60)
    es = Elasticsearch([f"{ES_SCHEME}://{ES_HOST}:{ES_PORT}"])
    indices = ["ecommerce-reviews", "ecommerce-clickstream"]
    for idx in indices:
        try:
            if es.indices.exists(index=idx):
                cnt = es.count(index=idx)["count"]
                logger.info("  ✓ Index '%s': %d documents", idx, cnt)
            else:
                logger.warning("  ✗ Index '%s' does not exist", idx)
        except Exception as e:
            logger.error("  ✗ Error querying index '%s': %s", idx, e)

    try:
        # Sample query: Search reviews with term "quality"
        res = es.search(
            index="ecommerce-reviews",
            body={
                "query": {"match": {"review_text": "quality"}},
                "size": 2,
            },
        )
        total_hits = res["hits"]["total"]["value"]
        logger.info("\n  -- Sample Search Query ('quality' in reviews) --")
        logger.info("    Total Matches: %d", total_hits)
        for hit in res["hits"]["hits"]:
            src = hit["_source"]
            logger.info("    [Rating: ★%s] Title: %s", src.get("rating"), src.get("review_title", "")[:50])
    except Exception as e:
        logger.error("  ✗ Sample ES query failed: %s", e)


if __name__ == "__main__":
    logger.info("Starting End-to-End Lakehouse Verification...")
    try:
        verify_s3()
        verify_postgres()
        verify_elasticsearch()
        logger.info("\nVerification complete!")
    except Exception as exc:
        logger.exception("Verification encountered error: %s", exc)
        sys.exit(1)

