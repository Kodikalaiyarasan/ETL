#!/usr/bin/env python3
"""
Unit Tests for E-Commerce Data Lakehouse — Warehouse Loader & DDL
==================================================================
Tests DDL SQL completeness, schema definitions, and table relationships.
"""

import os
import re
import pytest


class TestWarehouseDDL:
    """Test suite for analytical warehouse SQL schema definitions."""

    @pytest.fixture
    def ddl_content(self):
        """Read ddl.sql file from disk."""
        path = os.path.join(os.path.dirname(__file__), "..", "warehouse", "ddl.sql")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def test_schemas_created(self, ddl_content):
        """Verify staging, analytics, and reporting schemas are defined."""
        assert "CREATE SCHEMA IF NOT EXISTS analytics;" in ddl_content
        assert "CREATE SCHEMA IF NOT EXISTS staging;" in ddl_content
        assert "CREATE SCHEMA IF NOT EXISTS reporting;" in ddl_content

    def test_staging_tables_exist(self, ddl_content):
        """Verify staging tables exist for COPY loading."""
        expected_tables = [
            "staging.stg_order_facts",
            "staging.stg_daily_sales",
            "staging.stg_customer_360",
            "staging.stg_product_performance",
        ]
        for tbl in expected_tables:
            assert f"CREATE TABLE {tbl}" in ddl_content

    def test_analytics_tables_exist(self, ddl_content):
        """Verify production star-schema tables exist."""
        expected_tables = [
            "analytics.fact_orders",
            "analytics.dim_customers",
            "analytics.dim_products",
            "analytics.agg_daily_sales",
        ]
        for tbl in expected_tables:
            assert f"CREATE TABLE {tbl}" in ddl_content

    def test_materialized_views_exist(self, ddl_content):
        """Verify materialized views and unique indexes exist."""
        assert "reporting.mv_weekly_sales" in ddl_content
        assert "reporting.mv_category_performance" in ddl_content
        assert "idx_mv_weekly_sales" in ddl_content
        assert "idx_mv_category_perf" in ddl_content

    def test_reporting_views_exist(self, ddl_content):
        """Verify standard analytical reporting views exist."""
        views = [
            "reporting.v_monthly_revenue",
            "reporting.v_top_products",
            "reporting.v_customer_segments",
            "reporting.v_category_payment_analysis",
            "reporting.v_geographic_sales",
        ]
        for v in views:
            assert f"VIEW {v}" in ddl_content

