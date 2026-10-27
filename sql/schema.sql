-- Reference DDL of the warehouse (P9). The authoritative, executed version is spark/create_tables.py (Spark owns
-- metadata); this file documents the same schemas in engine-neutral SQL. Catalog: lakehouse. Namespaces: bronze/silver/gold.

-- ===== silver.transactions  (LARGE FACT)  prod ≈ 1.1 B rows/yr, ~300 GB/yr parquet; partition days(transaction_ts); 128 MB files
CREATE TABLE lakehouse.silver.transactions (
  transaction_id BIGINT, transaction_ts TIMESTAMP, transaction_date DATE, store_id INT, customer_id BIGINT,
  product_category_id INT, quantity INT, unit_price DECIMAL(10,2), amount DECIMAL(12,2),
  payment_method VARCHAR, channel VARCHAR, status VARCHAR
) WITH (partitioning = ARRAY['day(transaction_ts)']);

-- ===== silver.events  (MEDIUM EVENT TABLE)  prod ≈ 4 B rows/yr (clickstream), partition days(event_ts)
CREATE TABLE lakehouse.silver.events (
  event_id BIGINT, event_ts TIMESTAMP, event_date DATE, customer_id BIGINT, session_id VARCHAR, event_type VARCHAR,
  channel VARCHAR, device VARCHAR, page VARCHAR, product_category_id INT
) WITH (partitioning = ARRAY['day(event_ts)']);

-- ===== silver.customers  (SMALL DIMENSION, CDC-maintained current state)  prod ≈ 12 M rows, unpartitioned
CREATE TABLE lakehouse.silver.customers (
  customer_id BIGINT, first_name VARCHAR, last_name VARCHAR, segment VARCHAR, country VARCHAR, city VARCHAR,
  loyalty_tier VARCHAR, signup_date DATE, marketing_opt_in BOOLEAN, updated_at TIMESTAMP
);

-- ===== silver.customer_pii  (RESTRICTED, synthetic PII)  prod ≈ 12 M rows; column masks + row filter, admins/restricted only
CREATE TABLE lakehouse.silver.customer_pii (
  customer_id BIGINT, email VARCHAR, phone VARCHAR, national_id VARCHAR, date_of_birth DATE, street_address VARCHAR,
  country VARCHAR, marketing_opt_in BOOLEAN, updated_at TIMESTAMP
);

-- ===== silver.stores  (SMALL LOOKUP)  prod ≈ 1,200 rows   |   silver.product_categories  prod ≈ 400 rows
CREATE TABLE lakehouse.silver.stores (
  store_id INT, store_name VARCHAR, region VARCHAR, country VARCHAR, city VARCHAR, store_type VARCHAR, opened_date DATE
);
CREATE TABLE lakehouse.silver.product_categories (
  category_id INT, category_name VARCHAR, department VARCHAR, margin_pct DECIMAL(5,2)
);

-- ===== gold serving marts (P5) rebuilt by the ETL after each silver load
CREATE TABLE lakehouse.gold.daily_sales_by_store (
  sales_date DATE, store_id INT, store_name VARCHAR, region VARCHAR, channel VARCHAR,
  txn_count BIGINT, units BIGINT, revenue DECIMAL(18,2), distinct_customers BIGINT
) WITH (partitioning = ARRAY['month(sales_date)']);
CREATE TABLE lakehouse.gold.category_sales_daily (
  sales_date DATE, category_id INT, category_name VARCHAR, department VARCHAR, txn_count BIGINT, units BIGINT, revenue DECIMAL(18,2)
) WITH (partitioning = ARRAY['month(sales_date)']);
CREATE TABLE lakehouse.gold.customer_monthly_summary (
  sales_month DATE, customer_id BIGINT, segment VARCHAR, loyalty_tier VARCHAR, country VARCHAR,
  txn_count BIGINT, revenue DECIMAL(18,2), last_purchase_date DATE
) WITH (partitioning = ARRAY['sales_month']);
CREATE TABLE lakehouse.gold.event_funnel_daily (
  event_date DATE, channel VARCHAR, device VARCHAR, sessions BIGINT, page_views BIGINT, searches BIGINT,
  add_to_carts BIGINT, checkouts BIGINT, purchases BIGINT
) WITH (partitioning = ARRAY['month(event_date)']);
