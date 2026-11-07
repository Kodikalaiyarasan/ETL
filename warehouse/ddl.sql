-- ═══════════════════════════════════════════════
-- E-Commerce Data Lakehouse — Warehouse DDL
-- PostgreSQL (Amazon Redshift Simulation)
-- ═══════════════════════════════════════════════
-- This DDL creates analytical tables, indexes,
-- distribution strategies, and materialized views
-- optimized for analytical query patterns.
-- ═══════════════════════════════════════════════

-- ─────────────────────────────────────────────
-- Schema Setup
-- ─────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS reporting;

-- ─────────────────────────────────────────────
-- Staging Tables (for bulk loading)
-- ─────────────────────────────────────────────

DROP TABLE IF EXISTS staging.stg_order_facts CASCADE;
CREATE TABLE staging.stg_order_facts (
    transaction_id       VARCHAR(64),
    order_id             VARCHAR(64),
    customer_id          VARCHAR(64),
    order_date           TIMESTAMP,
    order_status         VARCHAR(32),
    payment_method       VARCHAR(32),
    subtotal             NUMERIC(12, 2),
    tax_rate             NUMERIC(6, 4),
    tax_amount           NUMERIC(12, 2),
    shipping_cost        NUMERIC(12, 2),
    grand_total          NUMERIC(12, 2),
    currency             VARCHAR(8),
    shipping_address_city    VARCHAR(128),
    shipping_address_state   VARCHAR(8),
    shipping_address_zip     VARCHAR(16),
    source_system        VARCHAR(32),
    order_year           INTEGER,
    order_month          INTEGER,
    order_day            INTEGER,
    product_id           VARCHAR(32),
    sku                  VARCHAR(32),
    product_name         VARCHAR(256),
    product_category     VARCHAR(64),
    quantity             INTEGER,
    unit_price           NUMERIC(10, 2),
    discount_percent     INTEGER,
    line_total           NUMERIC(12, 2),
    _etl_layer           VARCHAR(16),
    _etl_timestamp       TIMESTAMP,
    _etl_date            DATE,
    _etl_version         VARCHAR(16)
);

DROP TABLE IF EXISTS staging.stg_daily_sales CASCADE;
CREATE TABLE staging.stg_daily_sales (
    order_year           INTEGER,
    order_month          INTEGER,
    order_day            INTEGER,
    total_orders         BIGINT,
    unique_customers     BIGINT,
    total_revenue        NUMERIC(14, 2),
    total_items_sold     BIGINT,
    avg_line_total       NUMERIC(10, 2),
    avg_order_value      NUMERIC(10, 2),
    total_line_items     BIGINT,
    revenue_per_customer NUMERIC(10, 2),
    _etl_layer           VARCHAR(16),
    _etl_timestamp       TIMESTAMP,
    _etl_date            DATE,
    _etl_version         VARCHAR(16)
);

DROP TABLE IF EXISTS staging.stg_customer_360 CASCADE;
CREATE TABLE staging.stg_customer_360 (
    customer_id          VARCHAR(64),
    email                VARCHAR(256),
    first_name           VARCHAR(64),
    last_name            VARCHAR(64),
    gender               VARCHAR(32),
    age                  INTEGER,
    phone                VARCHAR(32),
    address_line1        VARCHAR(256),
    city                 VARCHAR(128),
    state                VARCHAR(8),
    zip_code             VARCHAR(16),
    country              VARCHAR(8),
    signup_date          DATE,
    loyalty_tier         VARCHAR(16),
    is_active            BOOLEAN,
    preferred_payment    VARCHAR(32),
    marketing_opt_in     BOOLEAN,
    referral_source      VARCHAR(32),
    total_orders         BIGINT,
    total_spend          NUMERIC(14, 2),
    avg_order_value      NUMERIC(10, 2),
    first_order_date     TIMESTAMP,
    last_order_date      TIMESTAMP,
    payment_methods_used BIGINT,
    total_events         BIGINT,
    total_sessions       BIGINT,
    avg_page_duration    NUMERIC(10, 2),
    purchase_events      BIGINT,
    cart_add_events      BIGINT,
    total_reviews        BIGINT,
    avg_rating_given     NUMERIC(4, 2),
    total_helpful_votes_received BIGINT,
    customer_lifetime_days INTEGER,
    days_since_last_order INTEGER,
    engagement_score     NUMERIC(10, 2),
    _etl_layer           VARCHAR(16),
    _etl_timestamp       TIMESTAMP,
    _etl_date            DATE,
    _etl_version         VARCHAR(16)
);

DROP TABLE IF EXISTS staging.stg_product_performance CASCADE;
CREATE TABLE staging.stg_product_performance (
    product_id           VARCHAR(32),
    sku                  VARCHAR(32),
    name                 VARCHAR(256),
    category             VARCHAR(64),
    sub_category         VARCHAR(64),
    brand                VARCHAR(64),
    price                NUMERIC(10, 2),
    cost                 NUMERIC(10, 2),
    weight_kg            NUMERIC(8, 2),
    inventory_count      INTEGER,
    rating_avg           NUMERIC(4, 2),
    review_count         INTEGER,
    is_active            BOOLEAN,
    created_date         DATE,
    supplier_id          VARCHAR(16),
    tags                 TEXT,
    profit_margin        NUMERIC(6, 2),
    total_units_sold     BIGINT,
    total_revenue        NUMERIC(14, 2),
    avg_selling_price    NUMERIC(10, 2),
    avg_discount         NUMERIC(6, 2),
    order_count          BIGINT,
    unique_buyers        BIGINT,
    avg_review_rating    NUMERIC(4, 2),
    total_helpful_votes  BIGINT,
    positive_reviews     BIGINT,
    negative_reviews     BIGINT,
    revenue_per_unit     NUMERIC(10, 2),
    sentiment_ratio      NUMERIC(4, 2),
    _etl_layer           VARCHAR(16),
    _etl_timestamp       TIMESTAMP,
    _etl_date            DATE,
    _etl_version         VARCHAR(16)
);

-- ─────────────────────────────────────────────
-- Analytics Tables (production / curated)
-- ─────────────────────────────────────────────

DROP TABLE IF EXISTS analytics.fact_orders CASCADE;
CREATE TABLE analytics.fact_orders (
    fact_order_id        BIGSERIAL PRIMARY KEY,
    transaction_id       VARCHAR(64) NOT NULL,
    order_id             VARCHAR(64) NOT NULL,
    customer_id          VARCHAR(64) NOT NULL,
    product_id           VARCHAR(32),
    order_date           TIMESTAMP NOT NULL,
    order_status         VARCHAR(32),
    payment_method       VARCHAR(32),
    subtotal             NUMERIC(12, 2),
    tax_rate             NUMERIC(6, 4),
    tax_amount           NUMERIC(12, 2),
    shipping_cost        NUMERIC(12, 2),
    grand_total          NUMERIC(12, 2),
    currency             VARCHAR(8) DEFAULT 'USD',
    shipping_city        VARCHAR(128),
    shipping_state       VARCHAR(8),
    shipping_zip         VARCHAR(16),
    source_system        VARCHAR(32),
    product_category     VARCHAR(64),
    quantity             INTEGER,
    unit_price           NUMERIC(10, 2),
    discount_percent     INTEGER DEFAULT 0,
    line_total           NUMERIC(12, 2),
    order_year           INTEGER,
    order_month          INTEGER,
    order_day            INTEGER,
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns (Redshift-like sort/dist key simulation)
CREATE INDEX idx_fact_orders_date ON analytics.fact_orders (order_date);
CREATE INDEX idx_fact_orders_customer ON analytics.fact_orders (customer_id);
CREATE INDEX idx_fact_orders_product ON analytics.fact_orders (product_id);
CREATE INDEX idx_fact_orders_status ON analytics.fact_orders (order_status);
CREATE INDEX idx_fact_orders_year_month ON analytics.fact_orders (order_year, order_month);


DROP TABLE IF EXISTS analytics.dim_customers CASCADE;
CREATE TABLE analytics.dim_customers (
    customer_id          VARCHAR(64) PRIMARY KEY,
    email                VARCHAR(256),
    first_name           VARCHAR(64),
    last_name            VARCHAR(64),
    full_name            VARCHAR(128) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    gender               VARCHAR(32),
    age                  INTEGER,
    age_group            VARCHAR(16),
    city                 VARCHAR(128),
    state                VARCHAR(8),
    zip_code             VARCHAR(16),
    country              VARCHAR(8) DEFAULT 'US',
    signup_date          DATE,
    loyalty_tier         VARCHAR(16),
    is_active            BOOLEAN DEFAULT TRUE,
    preferred_payment    VARCHAR(32),
    marketing_opt_in     BOOLEAN DEFAULT FALSE,
    referral_source      VARCHAR(32),
    -- Behavioral metrics
    total_orders         BIGINT DEFAULT 0,
    total_spend          NUMERIC(14, 2) DEFAULT 0,
    avg_order_value      NUMERIC(10, 2) DEFAULT 0,
    first_order_date     TIMESTAMP,
    last_order_date      TIMESTAMP,
    total_events         BIGINT DEFAULT 0,
    total_sessions       BIGINT DEFAULT 0,
    total_reviews        BIGINT DEFAULT 0,
    avg_rating_given     NUMERIC(4, 2),
    customer_lifetime_days INTEGER,
    days_since_last_order  INTEGER,
    engagement_score     NUMERIC(10, 2) DEFAULT 0,
    -- Metadata
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customers_tier ON analytics.dim_customers (loyalty_tier);
CREATE INDEX idx_dim_customers_state ON analytics.dim_customers (state);
CREATE INDEX idx_dim_customers_signup ON analytics.dim_customers (signup_date);


DROP TABLE IF EXISTS analytics.dim_products CASCADE;
CREATE TABLE analytics.dim_products (
    product_id           VARCHAR(32) PRIMARY KEY,
    sku                  VARCHAR(32),
    name                 VARCHAR(256),
    category             VARCHAR(64),
    sub_category         VARCHAR(64),
    brand                VARCHAR(64),
    price                NUMERIC(10, 2),
    cost                 NUMERIC(10, 2),
    profit_margin        NUMERIC(6, 2),
    weight_kg            NUMERIC(8, 2),
    inventory_count      INTEGER,
    is_active            BOOLEAN DEFAULT TRUE,
    created_date         DATE,
    supplier_id          VARCHAR(16),
    tags                 TEXT,
    -- Performance metrics
    total_units_sold     BIGINT DEFAULT 0,
    total_revenue        NUMERIC(14, 2) DEFAULT 0,
    avg_selling_price    NUMERIC(10, 2),
    avg_discount         NUMERIC(6, 2),
    order_count          BIGINT DEFAULT 0,
    unique_buyers        BIGINT DEFAULT 0,
    review_count         INTEGER DEFAULT 0,
    avg_review_rating    NUMERIC(4, 2),
    sentiment_ratio      NUMERIC(4, 2),
    revenue_per_unit     NUMERIC(10, 2),
    -- Metadata
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_products_category ON analytics.dim_products (category);
CREATE INDEX idx_dim_products_brand ON analytics.dim_products (brand);
CREATE INDEX idx_dim_products_active ON analytics.dim_products (is_active);


DROP TABLE IF EXISTS analytics.agg_daily_sales CASCADE;
CREATE TABLE analytics.agg_daily_sales (
    sales_date_id        BIGSERIAL PRIMARY KEY,
    order_year           INTEGER NOT NULL,
    order_month          INTEGER NOT NULL,
    order_day            INTEGER NOT NULL,
    sales_date           DATE GENERATED ALWAYS AS (
        make_date(order_year, order_month, order_day)
    ) STORED,
    total_orders         BIGINT,
    unique_customers     BIGINT,
    total_revenue        NUMERIC(14, 2),
    total_items_sold     BIGINT,
    avg_line_total       NUMERIC(10, 2),
    avg_order_value      NUMERIC(10, 2),
    total_line_items     BIGINT,
    revenue_per_customer NUMERIC(10, 2),
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_agg_daily_sales_date ON analytics.agg_daily_sales (order_year, order_month, order_day);


-- ─────────────────────────────────────────────
-- Reporting Views
-- ─────────────────────────────────────────────

-- Monthly Revenue Trend
CREATE OR REPLACE VIEW reporting.v_monthly_revenue AS
SELECT
    order_year,
    order_month,
    SUM(total_revenue)           AS monthly_revenue,
    SUM(total_orders)            AS monthly_orders,
    SUM(unique_customers)        AS monthly_customers,
    SUM(total_items_sold)        AS monthly_items,
    ROUND(SUM(total_revenue) / NULLIF(SUM(total_orders), 0), 2) AS avg_order_value
FROM analytics.agg_daily_sales
GROUP BY order_year, order_month
ORDER BY order_year, order_month;


-- Top Products by Revenue
CREATE OR REPLACE VIEW reporting.v_top_products AS
SELECT
    p.product_id,
    p.name,
    p.category,
    p.brand,
    p.total_revenue,
    p.total_units_sold,
    p.order_count,
    p.avg_review_rating,
    p.sentiment_ratio,
    p.profit_margin,
    RANK() OVER (ORDER BY p.total_revenue DESC) AS revenue_rank,
    RANK() OVER (PARTITION BY p.category ORDER BY p.total_revenue DESC) AS category_rank
FROM analytics.dim_products p
WHERE p.total_revenue > 0;


-- Customer Segmentation
CREATE OR REPLACE VIEW reporting.v_customer_segments AS
SELECT
    customer_id,
    full_name,
    loyalty_tier,
    total_spend,
    total_orders,
    avg_order_value,
    engagement_score,
    days_since_last_order,
    CASE
        WHEN total_spend >= 5000 AND total_orders >= 20 THEN 'VIP'
        WHEN total_spend >= 2000 AND total_orders >= 10 THEN 'High Value'
        WHEN total_spend >= 500  AND total_orders >= 5  THEN 'Medium Value'
        WHEN total_orders >= 1                          THEN 'Low Value'
        ELSE 'Prospect'
    END AS customer_segment,
    CASE
        WHEN days_since_last_order IS NULL              THEN 'Never Purchased'
        WHEN days_since_last_order <= 30                THEN 'Active'
        WHEN days_since_last_order <= 90                THEN 'At Risk'
        WHEN days_since_last_order <= 180               THEN 'Lapsed'
        ELSE 'Churned'
    END AS recency_status
FROM analytics.dim_customers;


-- Revenue by Category and Payment Method
CREATE OR REPLACE VIEW reporting.v_category_payment_analysis AS
SELECT
    product_category,
    payment_method,
    COUNT(DISTINCT order_id)     AS orders,
    SUM(line_total)              AS revenue,
    SUM(quantity)                AS units_sold,
    AVG(discount_percent)        AS avg_discount
FROM analytics.fact_orders
WHERE order_status = 'completed'
GROUP BY product_category, payment_method
ORDER BY revenue DESC;


-- Geographic Sales Distribution
CREATE OR REPLACE VIEW reporting.v_geographic_sales AS
SELECT
    shipping_state               AS state,
    COUNT(DISTINCT order_id)     AS total_orders,
    COUNT(DISTINCT customer_id)  AS unique_customers,
    SUM(grand_total)             AS total_revenue,
    AVG(grand_total)             AS avg_order_value,
    SUM(quantity)                AS total_items
FROM analytics.fact_orders
WHERE order_status = 'completed'
  AND shipping_state IS NOT NULL
GROUP BY shipping_state
ORDER BY total_revenue DESC;


-- ─────────────────────────────────────────────
-- Materialized Views (for dashboard performance)
-- ─────────────────────────────────────────────

-- Materialized: Weekly sales summary
CREATE MATERIALIZED VIEW IF NOT EXISTS reporting.mv_weekly_sales AS
SELECT
    order_year,
    EXTRACT(WEEK FROM order_date) AS order_week,
    COUNT(DISTINCT order_id)      AS weekly_orders,
    COUNT(DISTINCT customer_id)   AS weekly_customers,
    SUM(line_total)               AS weekly_revenue,
    SUM(quantity)                 AS weekly_items_sold,
    AVG(grand_total)              AS avg_order_value
FROM analytics.fact_orders
WHERE order_status = 'completed'
GROUP BY order_year, EXTRACT(WEEK FROM order_date)
ORDER BY order_year, order_week;

CREATE UNIQUE INDEX idx_mv_weekly_sales
    ON reporting.mv_weekly_sales (order_year, order_week);


-- Materialized: Category performance
CREATE MATERIALIZED VIEW IF NOT EXISTS reporting.mv_category_performance AS
SELECT
    product_category              AS category,
    COUNT(DISTINCT order_id)      AS total_orders,
    SUM(quantity)                 AS total_units,
    SUM(line_total)               AS total_revenue,
    AVG(unit_price)               AS avg_price,
    AVG(discount_percent)         AS avg_discount,
    COUNT(DISTINCT customer_id)   AS unique_buyers
FROM analytics.fact_orders
WHERE order_status = 'completed'
  AND product_category IS NOT NULL
GROUP BY product_category
ORDER BY total_revenue DESC;

CREATE UNIQUE INDEX idx_mv_category_perf
    ON reporting.mv_category_performance (category);
