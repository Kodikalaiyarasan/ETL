#!/usr/bin/env python3
"""
E-Commerce Data Lakehouse — Elasticsearch Indexing Pipeline
=============================================================
Indexes unstructured data into Elasticsearch for full-text search
and analytical filtering:

  1. Product Reviews → ecommerce-reviews index (full-text search + sentiment)
  2. Clickstream Logs → ecommerce-clickstream index (log analytics + filtering)
"""

import json
import logging
import os
import sys
import time

import boto3
from botocore.client import Config
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
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
logger = logging.getLogger("es_indexer")

# S3 / MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "lakehouse_secret_2024")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "ecommerce-raw")

# Elasticsearch config
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ES_PORT = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
ES_SCHEME = os.getenv("ELASTICSEARCH_SCHEME", "http")

# Index names
REVIEWS_INDEX = "ecommerce-reviews"
CLICKSTREAM_INDEX = "ecommerce-clickstream"

# Bulk indexing config
BULK_CHUNK_SIZE = 500
BULK_MAX_RETRIES = 3


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
# Elasticsearch Client
# ──────────────────────────────────────────────


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=2, min=2, max=60))
def get_es_client() -> Elasticsearch:
    """Create Elasticsearch client with retry logic."""
    es_url = f"{ES_SCHEME}://{ES_HOST}:{ES_PORT}"
    logger.info("Connecting to Elasticsearch at %s", es_url)

    es = Elasticsearch(
        [es_url],
        request_timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )

    if not es.ping():
        raise ConnectionError("Elasticsearch ping failed")

    info = es.info()
    logger.info(
        "Connected to Elasticsearch cluster: %s (version %s)",
        info["cluster_name"],
        info["version"]["number"],
    )
    return es


# ──────────────────────────────────────────────
# Index Definitions
# ──────────────────────────────────────────────

REVIEWS_INDEX_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "review_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "stop",
                        "snowball",
                    ],
                }
            }
        },
    },
    "mappings": {
        "properties": {
            "review_id": {"type": "keyword"},
            "product_id": {"type": "keyword"},
            "product_name": {
                "type": "text",
                "analyzer": "review_analyzer",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 256}
                },
            },
            "customer_id": {"type": "keyword"},
            "rating": {"type": "integer"},
            "review_title": {
                "type": "text",
                "analyzer": "review_analyzer",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 256}
                },
            },
            "review_text": {
                "type": "text",
                "analyzer": "review_analyzer",
            },
            "review_date": {"type": "date"},
            "verified_purchase": {"type": "boolean"},
            "helpful_votes": {"type": "integer"},
            "unhelpful_votes": {"type": "integer"},
            "response_from_seller": {"type": "text"},
            "images_count": {"type": "integer"},
            "sentiment": {"type": "keyword"},
        }
    },
}

CLICKSTREAM_INDEX_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "event_id": {"type": "keyword"},
            "event_type": {"type": "keyword"},
            "event_timestamp": {"type": "date"},
            "session_id": {"type": "keyword"},
            "customer_id": {"type": "keyword"},
            "device_type": {"type": "keyword"},
            "browser": {"type": "keyword"},
            "os": {"type": "keyword"},
            "ip_address": {"type": "ip"},
            "user_agent": {"type": "text"},
            "page_url": {"type": "keyword"},
            "page_category": {"type": "keyword"},
            "referrer_url": {"type": "keyword"},
            "duration_seconds": {"type": "integer"},
            "product_id": {"type": "keyword"},
            "search_query": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 256}
                },
            },
            "search_results_count": {"type": "integer"},
            "geo_country": {"type": "keyword"},
            "geo_region": {"type": "keyword"},
            "geo_city": {"type": "keyword"},
        }
    },
}


# ──────────────────────────────────────────────
# Index Management
# ──────────────────────────────────────────────


def create_or_recreate_index(es: Elasticsearch, index_name: str, settings: dict) -> None:
    """Delete and recreate an index with the given settings."""
    if es.indices.exists(index=index_name):
        logger.info("Deleting existing index: %s", index_name)
        es.indices.delete(index=index_name)

    logger.info("Creating index: %s", index_name)
    es.indices.create(index=index_name, body=settings)
    logger.info("Index %s created successfully.", index_name)


# ──────────────────────────────────────────────
# Data Reading from S3
# ──────────────────────────────────────────────


def read_ndjson_from_s3(s3_client, prefix: str) -> list[dict]:
    """Read all newline-delimited JSON files under a prefix."""
    records = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=S3_BUCKET_RAW, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") or key.endswith("/data.json"):
                response = s3_client.get_object(Bucket=S3_BUCKET_RAW, Key=key)
                content = response["Body"].read().decode("utf-8")
                for line in content.strip().split("\n"):
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            logger.warning("Skipping malformed JSON in %s: %s", key, e)

    logger.info("Read %d records from s3://%s/%s", len(records), S3_BUCKET_RAW, prefix)
    return records


# ──────────────────────────────────────────────
# Review Data Enrichment
# ──────────────────────────────────────────────


def enrich_review(review: dict) -> dict:
    """Enrich a review record with computed fields for indexing."""
    # Add sentiment label based on rating
    rating = review.get("rating")
    if rating is not None and isinstance(rating, (int, float)):
        if rating >= 4:
            review["sentiment"] = "positive"
        elif rating == 3:
            review["sentiment"] = "neutral"
        elif rating >= 1:
            review["sentiment"] = "negative"
        else:
            review["sentiment"] = "unknown"
    else:
        review["sentiment"] = "unknown"
        review["rating"] = 3  # default

    # Clean empty review text
    if not review.get("review_text", "").strip():
        review["review_text"] = "No review text provided."

    # Remove nested/non-ES-compatible fields
    review.pop("line_items", None)

    return review


# ──────────────────────────────────────────────
# Bulk Indexing
# ──────────────────────────────────────────────


def generate_actions(records: list[dict], index_name: str, id_field: str):
    """Generate bulk indexing actions."""
    for record in records:
        doc_id = record.get(id_field, None)
        action = {
            "_index": index_name,
            "_source": record,
        }
        if doc_id:
            action["_id"] = doc_id
        yield action


def bulk_index(es: Elasticsearch, records: list[dict], index_name: str, id_field: str) -> tuple[int, int]:
    """
    Bulk-index records into Elasticsearch.
    Returns (success_count, error_count).
    """
    logger.info("Bulk indexing %d records into %s...", len(records), index_name)

    success_count = 0
    error_count = 0

    try:
        success, errors = helpers.bulk(
            es,
            generate_actions(records, index_name, id_field),
            chunk_size=BULK_CHUNK_SIZE,
            max_retries=BULK_MAX_RETRIES,
            raise_on_error=False,
            raise_on_exception=False,
        )
        success_count = success
        if errors:
            error_count = len(errors)
            for err in errors[:5]:
                logger.warning("  Indexing error: %s", err)
    except Exception as exc:
        logger.exception("Bulk indexing failed: %s", exc)
        raise

    logger.info(
        "Indexed %d/%d records into %s (%d errors)",
        success_count, len(records), index_name, error_count,
    )
    return success_count, error_count


# ──────────────────────────────────────────────
# Verification Queries
# ──────────────────────────────────────────────


def verify_indices(es: Elasticsearch) -> None:
    """Run verification queries against indexed data."""
    logger.info("── Verifying Elasticsearch Indices ──")

    # Review index stats
    review_count = es.count(index=REVIEWS_INDEX)["count"]
    logger.info("  %s: %d documents", REVIEWS_INDEX, review_count)

    # Clickstream index stats
    click_count = es.count(index=CLICKSTREAM_INDEX)["count"]
    logger.info("  %s: %d documents", CLICKSTREAM_INDEX, click_count)

    # Sample full-text search on reviews
    logger.info("  Sample search: 'excellent quality'")
    results = es.search(
        index=REVIEWS_INDEX,
        body={
            "query": {
                "multi_match": {
                    "query": "excellent quality",
                    "fields": ["review_text", "review_title"],
                }
            },
            "size": 3,
        },
    )
    hits = results["hits"]["total"]["value"]
    logger.info("    → Found %d matching reviews", hits)
    for hit in results["hits"]["hits"][:3]:
        logger.info(
            "      %.2f | ★%s | %s",
            hit["_score"],
            hit["_source"].get("rating", "?"),
            hit["_source"].get("review_title", "N/A")[:60],
        )

    # Aggregation: average rating by sentiment
    logger.info("  Aggregation: average rating by sentiment")
    agg_results = es.search(
        index=REVIEWS_INDEX,
        body={
            "size": 0,
            "aggs": {
                "by_sentiment": {
                    "terms": {"field": "sentiment"},
                    "aggs": {
                        "avg_rating": {"avg": {"field": "rating"}},
                        "doc_count": {"value_count": {"field": "rating"}},
                    },
                }
            },
        },
    )
    for bucket in agg_results["aggregations"]["by_sentiment"]["buckets"]:
        logger.info(
            "    %s: avg_rating=%.2f count=%d",
            bucket["key"],
            bucket["avg_rating"]["value"],
            bucket["doc_count"]["value"],
        )

    # Clickstream: top event types
    logger.info("  Aggregation: top event types")
    event_agg = es.search(
        index=CLICKSTREAM_INDEX,
        body={
            "size": 0,
            "aggs": {
                "event_types": {
                    "terms": {"field": "event_type", "size": 15},
                }
            },
        },
    )
    for bucket in event_agg["aggregations"]["event_types"]["buckets"]:
        logger.info("    %s: %d events", bucket["key"], bucket["doc_count"])


# ──────────────────────────────────────────────
# Main Pipeline
# ──────────────────────────────────────────────


def main() -> None:
    """Execute the complete Elasticsearch indexing pipeline."""
    logger.info("=" * 60)
    logger.info("E-Commerce Data Lakehouse — Elasticsearch Indexing Starting")
    logger.info("=" * 60)

    start_time = time.time()

    # Connect to services
    s3 = get_s3_client()
    es = get_es_client()

    try:
        # ─── Reviews Index ────────────────────────
        logger.info("── Processing Reviews ──")
        create_or_recreate_index(es, REVIEWS_INDEX, REVIEWS_INDEX_SETTINGS)

        raw_reviews = read_ndjson_from_s3(s3, "reviews/")
        enriched_reviews = [enrich_review(r) for r in raw_reviews]
        review_success, review_errors = bulk_index(es, enriched_reviews, REVIEWS_INDEX, "review_id")

        # ─── Clickstream Index ────────────────────
        logger.info("── Processing Clickstream ──")
        create_or_recreate_index(es, CLICKSTREAM_INDEX, CLICKSTREAM_INDEX_SETTINGS)

        raw_clickstream = read_ndjson_from_s3(s3, "clickstream/")
        click_success, click_errors = bulk_index(es, raw_clickstream, CLICKSTREAM_INDEX, "event_id")

        # ─── Refresh indices ─────────────────────
        logger.info("Refreshing indices...")
        es.indices.refresh(index=REVIEWS_INDEX)
        es.indices.refresh(index=CLICKSTREAM_INDEX)

        # ─── Verify ──────────────────────────────
        verify_indices(es)

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Elasticsearch Indexing Complete! (%.1f seconds)", elapsed)
        logger.info("  Reviews:     %d indexed, %d errors", review_success, review_errors)
        logger.info("  Clickstream: %d indexed, %d errors", click_success, click_errors)
        logger.info("=" * 60)

    except Exception as exc:
        logger.exception("Elasticsearch indexing failed: %s", exc)
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
