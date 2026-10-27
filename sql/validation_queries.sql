-- Deterministic checks used by tests/ and scripts/validate.sh. Expected values live in tests/expected.json
-- (recorded from a real seed run, then verified identical on a re-seed). Engine-neutral SQL.

-- name: v_count_transactions
SELECT COUNT(*) AS n FROM lakehouse.silver.transactions;

-- name: v_checksum_transactions
SELECT COUNT(*) AS n, SUM(amount) AS sum_amount, SUM(quantity) AS sum_qty, COUNT(DISTINCT transaction_id) AS distinct_ids
FROM lakehouse.silver.transactions;

-- name: v_no_duplicates_in_silver
SELECT COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_rows FROM lakehouse.silver.transactions;

-- name: v_bronze_has_duplicates
SELECT COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_rows FROM lakehouse.bronze.transactions_raw;

-- name: v_count_events
SELECT COUNT(*) AS n FROM lakehouse.silver.events;

-- name: v_customers_after_cdc
SELECT COUNT(*) AS n, SUM(CASE WHEN marketing_opt_in THEN 1 ELSE 0 END) AS opted_in FROM lakehouse.silver.customers;

-- name: v_cdc_deleted_customer_absent
SELECT COUNT(*) AS n FROM lakehouse.silver.customers WHERE customer_id = 97;

-- name: v_cdc_inserted_customer_present
SELECT COUNT(*) AS n FROM lakehouse.silver.customers WHERE customer_id = 50001;

-- name: v_cdc_updated_customer
SELECT segment, loyalty_tier FROM lakehouse.silver.customers WHERE customer_id = 14;

-- name: v_pii_count
SELECT COUNT(*) AS n FROM lakehouse.silver.customer_pii;

-- name: v_gold_daily_sales
SELECT COUNT(*) AS n, SUM(revenue) AS revenue FROM lakehouse.gold.daily_sales_by_store;

-- name: v_gold_matches_silver
SELECT SUM(amount) AS silver_revenue FROM lakehouse.silver.transactions WHERE status = 'completed';

-- name: v_stores
SELECT COUNT(*) AS n FROM lakehouse.silver.stores;

-- name: v_categories
SELECT COUNT(*) AS n FROM lakehouse.silver.product_categories;
