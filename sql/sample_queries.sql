-- Representative analyst queries (P9). Engine-neutral SQL: the SAME file runs on Trino and on Kyuubi/Spark SQL
-- (catalog "lakehouse" exists under that name in both). Each statement is tagged "-- name: <id>" for the runners.

-- name: q1_daily_revenue_by_region_gold
-- P5: answered by the gold mart (≈ 200 stores × 90 days × 3 channels rows) instead of scanning the 1M-row fact
SELECT region, sales_date, SUM(revenue) AS revenue, SUM(txn_count) AS txns
FROM lakehouse.gold.daily_sales_by_store
WHERE sales_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-28'
GROUP BY region, sales_date
ORDER BY region, sales_date;

-- name: q2_top_categories_month_gold
SELECT department, category_name, SUM(revenue) AS revenue, SUM(units) AS units
FROM lakehouse.gold.category_sales_daily
WHERE sales_date BETWEEN DATE '2026-08-01' AND DATE '2026-08-31'
GROUP BY department, category_name
ORDER BY revenue DESC
LIMIT 10;

-- name: q3_fact_join_dimension_silver
-- aggregate transactions joined to the customer dimension (deliberately on silver: fact + dim join)
SELECT c.loyalty_tier, c.segment, COUNT(*) AS txns, SUM(t.amount) AS revenue, COUNT(DISTINCT t.customer_id) AS customers
FROM lakehouse.silver.transactions t
JOIN lakehouse.silver.customers c ON t.customer_id = c.customer_id
WHERE t.transaction_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-14' AND t.status = 'completed'
GROUP BY c.loyalty_tier, c.segment
ORDER BY revenue DESC;

-- name: q4_funnel_conversion_gold
SELECT channel, device, SUM(sessions) AS sessions, SUM(page_views) AS page_views, SUM(add_to_carts) AS add_to_carts,
       SUM(purchases) AS purchases,
       CAST(SUM(purchases) AS DOUBLE) / CAST(SUM(sessions) AS DOUBLE) AS purchase_rate
FROM lakehouse.gold.event_funnel_daily
WHERE event_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-28'
GROUP BY channel, device
ORDER BY purchase_rate DESC;

-- name: q5_events_last_7_days_silver
SELECT event_date, device, COUNT(*) AS events, COUNT(DISTINCT session_id) AS sessions
FROM lakehouse.silver.events
WHERE event_date BETWEEN DATE '2026-09-22' AND DATE '2026-09-28'
GROUP BY event_date, device
ORDER BY event_date, device;

-- name: q6_current_state_customer_after_cdc
-- current-snapshot read: customer 14 was UPDATED by the CDC delta, customer 97 was DELETED, 50001 was INSERTED
SELECT customer_id, segment, loyalty_tier, marketing_opt_in, updated_at
FROM lakehouse.silver.customers
WHERE customer_id IN (14, 97, 50001, 1)
ORDER BY customer_id;

-- name: q7_customer_monthly_top_spenders_gold
SELECT sales_month, customer_id, loyalty_tier, revenue, txn_count
FROM lakehouse.gold.customer_monthly_summary
WHERE sales_month = DATE '2026-08-01'
ORDER BY revenue DESC
LIMIT 20;

-- name: q8_store_lookup_point_query
SELECT s.store_id, s.store_name, s.region, s.city, d.sales_date, d.revenue
FROM lakehouse.silver.stores s
JOIN lakehouse.gold.daily_sales_by_store d ON d.store_id = s.store_id AND d.channel = 'store'
WHERE s.store_id = 42 AND d.sales_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-28'
ORDER BY d.sales_date;

-- name: q9_pii_restricted
-- admin: raw values. restricted_analysts: masked columns + only marketing_opt_in rows. analysts: DENIED.
SELECT customer_id, email, phone, national_id, date_of_birth, marketing_opt_in
FROM lakehouse.silver.customer_pii
WHERE customer_id <= 20
ORDER BY customer_id;
