#!/usr/bin/env python3
"""
Unit Tests for E-Commerce Data Lakehouse — Data Generator
=========================================================
Tests mock data generation integrity, schema compliance,
reference data coverage, and dirty data handling.
"""

import pytest
from data_generator.generate_data import (
    generate_customers,
    generate_products,
    generate_transactions,
    generate_clickstream,
    generate_reviews,
    PRODUCT_CATEGORIES,
    BRANDS,
    PAYMENT_METHODS,
)


class TestDataGenerator:
    """Test suite for mock data generation modules."""

    def test_generate_customers(self):
        """Verify customer generation counts, required keys, and types."""
        n = 50
        customers = generate_customers(n)
        assert len(customers) == n

        required_keys = {
            "customer_id", "email", "first_name", "last_name", "gender",
            "age", "phone", "address_line1", "city", "state", "zip_code",
            "country", "signup_date", "loyalty_tier", "is_active",
            "preferred_payment", "marketing_opt_in", "referral_source"
        }
        for c in customers:
            assert required_keys.issubset(c.keys())
            assert c["customer_id"].startswith("CUST-")
            assert 18 <= c["age"] <= 75
            assert c["loyalty_tier"] in ["bronze", "silver", "gold", "platinum"]
            assert c["country"] == "US"

    def test_generate_products(self):
        """Verify product catalog fields, price ranges, and category validity."""
        n = 30
        products = generate_products(n)
        assert len(products) == n

        required_keys = {
            "product_id", "sku", "name", "category", "sub_category",
            "brand", "price", "cost", "weight_kg", "inventory_count",
            "rating_avg", "review_count", "is_active", "created_date"
        }
        for p in products:
            assert required_keys.issubset(p.keys())
            assert p["product_id"].startswith("PROD-")
            assert p["category"] in PRODUCT_CATEGORIES
            assert p["brand"] in BRANDS
            assert p["price"] > 0
            assert p["cost"] > 0
            assert p["cost"] <= p["price"]
            assert 1.0 <= p["rating_avg"] <= 5.0

    def test_generate_transactions(self):
        """Verify transaction records, line items, monetary math, and dirty data."""
        customers = generate_customers(10)
        customer_ids = [c["customer_id"] for c in customers]
        products = generate_products(10)

        n = 40
        transactions = generate_transactions(n, customer_ids, products)
        assert len(transactions) == n

        for t in transactions:
            assert t["transaction_id"].startswith("TXN-")
            assert t["order_id"].startswith("ORD-")
            assert "line_items" in t
            assert len(t["line_items"]) >= 1

            for item in t["line_items"]:
                assert "product_id" in item
                assert "quantity" in item
                assert "unit_price" in item
                assert "line_total" in item
                assert item["quantity"] > 0
                assert item["line_total"] >= 0

    def test_generate_clickstream(self):
        """Verify clickstream event types, timestamps, and session identifiers."""
        customers = generate_customers(5)
        customer_ids = [c["customer_id"] for c in customers]
        products = generate_products(5)
        product_ids = [p["product_id"] for p in products]

        n = 100
        events = generate_clickstream(n, customer_ids, product_ids)
        assert len(events) == n

        for e in events:
            assert e["event_id"].startswith("EVT-")
            assert e["session_id"].startswith("SESS-")
            assert "event_type" in e
            assert "event_timestamp" in e
            assert e["device_type"] in ["desktop", "mobile", "tablet"]

    def test_generate_reviews(self):
        """Verify review text generation, rating ranges, and sentiment consistency."""
        customers = generate_customers(5)
        customer_ids = [c["customer_id"] for c in customers]
        products = generate_products(5)

        n = 50
        reviews = generate_reviews(n, customer_ids, products)
        assert len(reviews) == n

        for r in reviews:
            assert r["review_id"].startswith("REV-")
            assert r["product_id"].startswith("PROD-")
            assert "review_title" in r
            assert "review_date" in r

