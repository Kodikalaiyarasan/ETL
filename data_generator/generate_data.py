#!/usr/bin/env python3
"""
E-Commerce Data Lakehouse — Multi-Source Data Generator
========================================================
Generates realistic mock e-commerce data and uploads to S3-compatible storage (MinIO).

Data Sources Generated:
  1. Transaction Logs (JSON) — order-level records with line items
  2. User Behavior / Clickstream Logs (JSON) — page views, clicks, searches, cart events
  3. Product Reviews (Text/JSON) — user-submitted reviews with ratings and text
  4. Customer Profiles (CSV) — master customer dimension data
  5. Product Catalog (CSV) — master product dimension data
"""

import csv
import io
import json
import logging
import os
import random
import sys
import uuid
from datetime import datetime, timedelta
from typing import Any

import boto3
from botocore.client import Config
from dotenv import load_dotenv
from faker import Faker

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("data_generator")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "lakehouse_secret_2024")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "ecommerce-raw")

NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", "500"))
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", "200"))
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", "5000"))
NUM_REVIEWS = int(os.getenv("NUM_REVIEWS", "2000"))
NUM_CLICKSTREAM_EVENTS = int(os.getenv("NUM_CLICKSTREAM_EVENTS", "10000"))
GENERATION_SEED = int(os.getenv("GENERATION_SEED", "42"))

# Reproducibility
random.seed(GENERATION_SEED)
fake = Faker()
Faker.seed(GENERATION_SEED)

# ──────────────────────────────────────────────
# S3 Client Setup
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


def ensure_bucket_exists(s3_client, bucket_name: str) -> None:
    """Create the S3 bucket if it does not already exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info("Bucket '%s' already exists.", bucket_name)
    except s3_client.exceptions.ClientError:
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info("Created bucket '%s'.", bucket_name)


def upload_json_to_s3(s3_client, bucket: str, key: str, data: list[dict]) -> None:
    """Serialize a list of dicts to newline-delimited JSON and upload."""
    body = "\n".join(json.dumps(record, default=str) for record in data)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    logger.info("Uploaded %d records → s3://%s/%s", len(data), bucket, key)


def upload_csv_to_s3(
    s3_client, bucket: str, key: str, rows: list[dict], fieldnames: list[str]
) -> None:
    """Serialize a list of dicts to CSV and upload."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))
    logger.info("Uploaded %d records → s3://%s/%s", len(rows), bucket, key)


# ──────────────────────────────────────────────
# Reference Data (categories, brands, etc.)
# ──────────────────────────────────────────────

PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Books", "Sports & Outdoors",
    "Beauty & Personal Care", "Toys & Games", "Automotive", "Health & Wellness",
    "Grocery & Gourmet", "Office Products", "Pet Supplies", "Garden & Outdoor",
    "Musical Instruments", "Jewelry",
]

BRANDS = [
    "TechVault", "UrbanThread", "HomeNest", "PageTurner", "PeakGear",
    "GlowUp", "PlaySphere", "DriveMax", "VitalCore", "FreshHarvest",
    "DeskPro", "PawPrime", "GreenScape", "SoundWave", "LuxCraft",
    "NovaElectric", "ComfyWear", "ChefMate", "ReadMore", "ActiveEdge",
]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
ORDER_STATUSES = ["completed", "completed", "completed", "completed", "shipped", "processing", "cancelled", "refunded"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
OS_LIST = ["Windows", "macOS", "iOS", "Android", "Linux"]
EVENT_TYPES = [
    "page_view", "page_view", "page_view",  # weighted more common
    "product_view", "product_view",
    "search", "search",
    "add_to_cart",
    "remove_from_cart",
    "begin_checkout",
    "purchase",
    "wishlist_add",
    "filter_apply",
    "sort_change",
    "review_read",
]
PAGE_CATEGORIES = [
    "homepage", "category_page", "product_detail", "search_results",
    "cart", "checkout", "account", "order_history", "help", "about",
]

REVIEW_TEMPLATES_POSITIVE = [
    "Absolutely love this {product}! {reason}. Would definitely recommend to friends.",
    "Great quality {product}. {reason}. Five stars from me!",
    "This {product} exceeded my expectations. {reason}. Very happy with the purchase.",
    "Best {product} I've ever owned. {reason}. Will buy again.",
    "{product} arrived quickly and {reason}. Couldn't be happier.",
    "Outstanding {product}! {reason}. Worth every penny.",
    "I've been using this {product} for a while now and {reason}. Highly recommended!",
]

REVIEW_TEMPLATES_NEUTRAL = [
    "The {product} is decent. {reason}. It does what it's supposed to.",
    "OK {product}. {reason}. Nothing spectacular but not bad either.",
    "Average {product}. {reason}. You get what you pay for.",
    "The {product} is fine. {reason}. Meets basic expectations.",
]

REVIEW_TEMPLATES_NEGATIVE = [
    "Disappointed with this {product}. {reason}. Would not recommend.",
    "The {product} broke after a week. {reason}. Returning it.",
    "Not worth the money. {reason}. Expected much better from this {product}.",
    "Poor quality {product}. {reason}. Save your money.",
    "Terrible experience with the {product}. {reason}. Very frustrated.",
]

POSITIVE_REASONS = [
    "The build quality is excellent", "It works perfectly out of the box",
    "The design is sleek and modern", "Customer service was very helpful",
    "It's very easy to use", "Great value for the price",
    "The materials feel premium", "Battery life is impressive",
    "Setup was a breeze", "Performance is outstanding",
]

NEUTRAL_REASONS = [
    "The instructions could be clearer", "Packaging was adequate",
    "It took a bit to set up", "The color is slightly different from the photo",
    "It's a bit bulky", "Delivery was on time",
]

NEGATIVE_REASONS = [
    "The quality feels cheap", "It stopped working after a few uses",
    "Customer support was unresponsive", "The product looks nothing like the photo",
    "It arrived damaged", "Assembly instructions were terrible",
    "It makes a strange noise", "The size is much smaller than described",
]


# ──────────────────────────────────────────────
# Data Generators
# ──────────────────────────────────────────────


def generate_customers(n: int) -> list[dict[str, Any]]:
    """Generate realistic customer profile records."""
    logger.info("Generating %d customer profiles...", n)
    customers = []
    tiers = ["bronze", "silver", "gold", "platinum"]
    tier_weights = [0.40, 0.30, 0.20, 0.10]

    for i in range(n):
        signup_date = fake.date_between(start_date="-5y", end_date="-30d")
        customer = {
            "customer_id": f"CUST-{i+1:06d}",
            "email": fake.unique.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "gender": random.choice(["M", "F", "Non-binary", "Prefer not to say"]),
            "age": random.randint(18, 75),
            "phone": fake.phone_number(),
            "address_line1": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "country": "US",
            "signup_date": signup_date.isoformat(),
            "loyalty_tier": random.choices(tiers, weights=tier_weights, k=1)[0],
            "is_active": random.random() > 0.1,
            "preferred_payment": random.choice(PAYMENT_METHODS),
            "marketing_opt_in": random.random() > 0.3,
            "referral_source": random.choice(
                ["organic", "google_ads", "facebook", "instagram", "referral", "email", "direct"]
            ),
        }
        customers.append(customer)

    logger.info("Generated %d customer profiles.", len(customers))
    return customers


def generate_products(n: int) -> list[dict[str, Any]]:
    """Generate realistic product catalog records."""
    logger.info("Generating %d products...", n)
    products = []

    for i in range(n):
        category = random.choice(PRODUCT_CATEGORIES)
        brand = random.choice(BRANDS)
        base_price = round(random.uniform(5.99, 999.99), 2)
        product = {
            "product_id": f"PROD-{i+1:05d}",
            "sku": fake.bothify(text="???-########").upper(),
            "name": f"{brand} {fake.catch_phrase()} {category.split(' ')[0]}",
            "category": category,
            "sub_category": fake.word().capitalize(),
            "brand": brand,
            "price": base_price,
            "cost": round(base_price * random.uniform(0.3, 0.7), 2),
            "weight_kg": round(random.uniform(0.1, 25.0), 2),
            "inventory_count": random.randint(0, 5000),
            "rating_avg": round(random.uniform(1.0, 5.0), 1),
            "review_count": random.randint(0, 500),
            "is_active": random.random() > 0.05,
            "created_date": fake.date_between(start_date="-3y", end_date="-60d").isoformat(),
            "supplier_id": f"SUP-{random.randint(1, 50):04d}",
            "tags": ",".join(random.sample(
                ["bestseller", "new", "sale", "premium", "eco-friendly",
                 "limited-edition", "bulk", "clearance", "trending"],
                k=random.randint(1, 4),
            )),
        }
        products.append(product)

    logger.info("Generated %d products.", len(products))
    return products


def generate_transactions(
    n: int,
    customer_ids: list[str],
    product_catalog: list[dict],
) -> list[dict[str, Any]]:
    """Generate realistic transaction / order records with line items."""
    logger.info("Generating %d transactions...", n)
    transactions = []

    for i in range(n):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        num_items = random.choices([1, 2, 3, 4, 5], weights=[0.35, 0.30, 0.20, 0.10, 0.05], k=1)[0]
        selected_products = random.sample(product_catalog, min(num_items, len(product_catalog)))

        line_items = []
        subtotal = 0.0
        for product in selected_products:
            qty = random.choices([1, 2, 3, 4, 5], weights=[0.50, 0.25, 0.15, 0.07, 0.03], k=1)[0]
            discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20, 25])
            unit_price = product["price"]
            line_total = round(unit_price * qty * (1 - discount_pct / 100), 2)
            subtotal += line_total
            line_items.append({
                "product_id": product["product_id"],
                "sku": product["sku"],
                "product_name": product["name"],
                "category": product["category"],
                "quantity": qty,
                "unit_price": unit_price,
                "discount_percent": discount_pct,
                "line_total": line_total,
            })

        tax_rate = round(random.uniform(0.05, 0.12), 4)
        tax_amount = round(subtotal * tax_rate, 2)
        shipping = round(random.choice([0, 0, 4.99, 7.99, 12.99, 19.99]), 2)
        grand_total = round(subtotal + tax_amount + shipping, 2)

        # Introduce some intentional data quality issues (~5% of records)
        status = random.choice(ORDER_STATUSES)
        is_dirty = random.random() < 0.05
        transaction = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "order_id": f"ORD-{i+1:07d}",
            "customer_id": customer_id if not is_dirty else (None if random.random() < 0.5 else ""),
            "order_date": order_date.isoformat(),
            "order_status": status,
            "payment_method": random.choice(PAYMENT_METHODS),
            "subtotal": subtotal if not is_dirty else (None if random.random() < 0.3 else subtotal),
            "tax_rate": tax_rate,
            "tax_amount": tax_amount,
            "shipping_cost": shipping,
            "grand_total": grand_total,
            "currency": "USD",
            "shipping_address_city": fake.city(),
            "shipping_address_state": fake.state_abbr(),
            "shipping_address_zip": fake.zipcode(),
            "line_items": line_items,
            "line_item_count": len(line_items),
            "source_system": random.choice(["web", "mobile_app", "pos", "marketplace"]),
            "processing_timestamp": datetime.utcnow().isoformat(),
        }
        transactions.append(transaction)

    logger.info("Generated %d transactions.", len(transactions))
    return transactions


def generate_clickstream(
    n: int,
    customer_ids: list[str],
    product_ids: list[str],
) -> list[dict[str, Any]]:
    """Generate realistic user behavior / clickstream events."""
    logger.info("Generating %d clickstream events...", n)
    events = []

    for _ in range(n):
        event_type = random.choice(EVENT_TYPES)
        session_id = f"SESS-{uuid.uuid4().hex[:16]}"
        event_ts = fake.date_time_between(start_date="-90d", end_date="now")
        customer_id = random.choice(customer_ids) if random.random() > 0.2 else None  # 20% anonymous

        event = {
            "event_id": f"EVT-{uuid.uuid4().hex[:12].upper()}",
            "event_type": event_type,
            "event_timestamp": event_ts.isoformat(),
            "session_id": session_id,
            "customer_id": customer_id,
            "device_type": random.choice(DEVICE_TYPES),
            "browser": random.choice(BROWSERS),
            "os": random.choice(OS_LIST),
            "ip_address": fake.ipv4(),
            "user_agent": fake.user_agent(),
            "page_url": f"https://shop.example.com/{random.choice(PAGE_CATEGORIES)}",
            "page_category": random.choice(PAGE_CATEGORIES),
            "referrer_url": random.choice([
                None, "https://google.com", "https://facebook.com",
                "https://instagram.com", "https://twitter.com",
                "https://shop.example.com",
            ]),
            "duration_seconds": random.randint(1, 600) if event_type == "page_view" else None,
            "product_id": random.choice(product_ids) if event_type in [
                "product_view", "add_to_cart", "remove_from_cart", "purchase", "wishlist_add"
            ] else None,
            "search_query": fake.bs() if event_type == "search" else None,
            "search_results_count": random.randint(0, 200) if event_type == "search" else None,
            "geo_country": "US",
            "geo_region": fake.state_abbr(),
            "geo_city": fake.city(),
        }
        events.append(event)

    logger.info("Generated %d clickstream events.", len(events))
    return events


def generate_reviews(
    n: int,
    customer_ids: list[str],
    product_catalog: list[dict],
) -> list[dict[str, Any]]:
    """Generate realistic product reviews with text, ratings, and sentiment."""
    logger.info("Generating %d product reviews...", n)
    reviews = []

    for i in range(n):
        product = random.choice(product_catalog)
        rating = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.08, 0.15, 0.35, 0.37], k=1)[0]

        # Select template and reason based on rating
        product_name = product["name"].split()[-1]  # short name for templates
        if rating >= 4:
            template = random.choice(REVIEW_TEMPLATES_POSITIVE)
            reason = random.choice(POSITIVE_REASONS)
        elif rating == 3:
            template = random.choice(REVIEW_TEMPLATES_NEUTRAL)
            reason = random.choice(NEUTRAL_REASONS)
        else:
            template = random.choice(REVIEW_TEMPLATES_NEGATIVE)
            reason = random.choice(NEGATIVE_REASONS)

        review_text = template.format(product=product_name, reason=reason)

        # Introduce some dirty data: ~3% have empty review text, ~2% have invalid ratings
        is_dirty = random.random() < 0.03
        rating_dirty = random.random() < 0.02

        review = {
            "review_id": f"REV-{i+1:06d}",
            "product_id": product["product_id"],
            "product_name": product["name"],
            "customer_id": random.choice(customer_ids),
            "rating": rating if not rating_dirty else random.choice([-1, 0, 6, None]),
            "review_title": fake.sentence(nb_words=random.randint(3, 8)).rstrip("."),
            "review_text": review_text if not is_dirty else "",
            "review_date": fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
            "verified_purchase": random.random() > 0.2,
            "helpful_votes": random.randint(0, 150),
            "unhelpful_votes": random.randint(0, 20),
            "response_from_seller": fake.sentence() if random.random() < 0.15 else None,
            "images_count": random.choices([0, 0, 0, 1, 2, 3], weights=[0.5, 0.15, 0.1, 0.12, 0.08, 0.05], k=1)[0],
        }
        reviews.append(review)

    logger.info("Generated %d product reviews.", len(reviews))
    return reviews


# ──────────────────────────────────────────────
# Partitioned Upload Helper
# ──────────────────────────────────────────────


def upload_partitioned_by_date(
    s3_client, bucket: str, prefix: str, records: list[dict], date_key: str
) -> None:
    """Partition records by date (YYYY/MM/DD) and upload each partition separately."""
    partitions: dict[str, list[dict]] = {}
    for record in records:
        raw_date = record.get(date_key, "")
        if raw_date:
            try:
                dt = datetime.fromisoformat(str(raw_date))
                partition_key = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
            except (ValueError, TypeError):
                partition_key = "year=unknown/month=00/day=00"
        else:
            partition_key = "year=unknown/month=00/day=00"
        partitions.setdefault(partition_key, []).append(record)

    for partition_key, partition_records in partitions.items():
        key = f"{prefix}/{partition_key}/data.json"
        upload_json_to_s3(s3_client, bucket, key, partition_records)

    logger.info(
        "Uploaded %d records across %d partitions → s3://%s/%s/",
        len(records), len(partitions), bucket, prefix,
    )


# ──────────────────────────────────────────────
# Main Execution
# ──────────────────────────────────────────────


def main() -> None:
    """Generate all data sources and upload to S3."""
    logger.info("=" * 60)
    logger.info("E-Commerce Data Lakehouse — Data Generation Starting")
    logger.info("=" * 60)

    s3 = get_s3_client()
    ensure_bucket_exists(s3, S3_BUCKET_RAW)

    # 1. Customer Profiles (CSV)
    customers = generate_customers(NUM_CUSTOMERS)
    customer_ids = [c["customer_id"] for c in customers]
    customer_fields = [
        "customer_id", "email", "first_name", "last_name", "gender", "age",
        "phone", "address_line1", "city", "state", "zip_code", "country",
        "signup_date", "loyalty_tier", "is_active", "preferred_payment",
        "marketing_opt_in", "referral_source",
    ]
    upload_csv_to_s3(s3, S3_BUCKET_RAW, "customers/customers.csv", customers, customer_fields)

    # 2. Product Catalog (CSV)
    products = generate_products(NUM_PRODUCTS)
    product_ids = [p["product_id"] for p in products]
    product_fields = [
        "product_id", "sku", "name", "category", "sub_category", "brand",
        "price", "cost", "weight_kg", "inventory_count", "rating_avg",
        "review_count", "is_active", "created_date", "supplier_id", "tags",
    ]
    upload_csv_to_s3(s3, S3_BUCKET_RAW, "products/products.csv", products, product_fields)

    # 3. Transaction Logs (JSON, partitioned by date)
    transactions = generate_transactions(NUM_TRANSACTIONS, customer_ids, products)
    upload_partitioned_by_date(s3, S3_BUCKET_RAW, "transactions", transactions, "order_date")

    # 4. Clickstream Events (JSON, partitioned by date)
    clickstream = generate_clickstream(NUM_CLICKSTREAM_EVENTS, customer_ids, product_ids)
    upload_partitioned_by_date(s3, S3_BUCKET_RAW, "clickstream", clickstream, "event_timestamp")

    # 5. Product Reviews (JSON, partitioned by date)
    reviews = generate_reviews(NUM_REVIEWS, customer_ids, products)
    upload_partitioned_by_date(s3, S3_BUCKET_RAW, "reviews", reviews, "review_date")

    logger.info("=" * 60)
    logger.info("Data Generation Complete!")
    logger.info("  Customers:    %d", NUM_CUSTOMERS)
    logger.info("  Products:     %d", NUM_PRODUCTS)
    logger.info("  Transactions: %d", NUM_TRANSACTIONS)
    logger.info("  Clickstream:  %d", NUM_CLICKSTREAM_EVENTS)
    logger.info("  Reviews:      %d", NUM_REVIEWS)
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception("Data generation failed: %s", exc)
        sys.exit(1)
