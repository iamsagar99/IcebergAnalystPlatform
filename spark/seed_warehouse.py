#!/usr/bin/env python3
"""bronze -> silver (CDC MERGE) -> gold. Idempotent: MERGE/overwrite semantics make reruns converge to the same state.

silver.transactions : de-duplicated append (MERGE ... WHEN NOT MATCHED) from bronze.transactions_raw
silver.events       : de-duplicated append
silver.customers    : CDC merge (latest op per key; D deletes, U updates, I inserts) from bronze.customers_cdc
silver.customer_pii : same CDC feed, PII columns only (restricted table, P7)
gold.*              : full rebuild of the serving marts (P5) with INSERT OVERWRITE semantics
"""

from lakehouse import arg_parser, session, sql, timed


def merge_transactions(spark, c):
    sql(
        spark,
        f"""
        MERGE INTO {c}.silver.transactions t
        USING (
          SELECT transaction_id, transaction_ts, CAST(transaction_ts AS DATE) AS transaction_date, store_id, customer_id,
                 product_category_id, quantity, unit_price, amount, payment_method, channel, status
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY ingested_at DESC, ingest_batch_id DESC) AS rn
            FROM {c}.bronze.transactions_raw
          ) WHERE rn = 1
        ) s
        ON t.transaction_id = s.transaction_id
        WHEN MATCHED AND t.status <> s.status THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """,
    )


def merge_events(spark, c):
    sql(
        spark,
        f"""
        MERGE INTO {c}.silver.events t
        USING (
          SELECT event_id, event_ts, CAST(event_ts AS DATE) AS event_date, customer_id, session_id, event_type,
                 channel, device, page, product_category_id
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_batch_id DESC) AS rn
            FROM {c}.bronze.events_raw
          ) WHERE rn = 1
        ) s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """,
    )


CDC_LATEST = """
    SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY op_ts DESC, ingest_batch_id DESC) AS rn
      FROM {c}.bronze.customers_cdc
    ) WHERE rn = 1
"""


def merge_customers(spark, c):
    sql(
        spark,
        f"""
        MERGE INTO {c}.silver.customers t
        USING (
          SELECT op, op_ts, customer_id, first_name, last_name, segment, country, city, loyalty_tier, signup_date,
                 marketing_opt_in
          FROM ({CDC_LATEST.format(c=c)})
        ) s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.op = 'D' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
          first_name = s.first_name, last_name = s.last_name, segment = s.segment, country = s.country, city = s.city,
          loyalty_tier = s.loyalty_tier, signup_date = s.signup_date, marketing_opt_in = s.marketing_opt_in, updated_at = s.op_ts
        WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT
          (customer_id, first_name, last_name, segment, country, city, loyalty_tier, signup_date, marketing_opt_in, updated_at)
          VALUES (s.customer_id, s.first_name, s.last_name, s.segment, s.country, s.city, s.loyalty_tier, s.signup_date, s.marketing_opt_in, s.op_ts)
    """,
    )
    sql(
        spark,
        f"""
        MERGE INTO {c}.silver.customer_pii t
        USING (
          SELECT op, op_ts, customer_id, email, phone, national_id, date_of_birth, street_address, country, marketing_opt_in
          FROM ({CDC_LATEST.format(c=c)})
        ) s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.op = 'D' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
          email = s.email, phone = s.phone, national_id = s.national_id, date_of_birth = s.date_of_birth,
          street_address = s.street_address, country = s.country, marketing_opt_in = s.marketing_opt_in, updated_at = s.op_ts
        WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT
          (customer_id, email, phone, national_id, date_of_birth, street_address, country, marketing_opt_in, updated_at)
          VALUES (s.customer_id, s.email, s.phone, s.national_id, s.date_of_birth, s.street_address, s.country, s.marketing_opt_in, s.op_ts)
    """,
    )


def build_gold(spark, c):
    sql(
        spark,
        f"""
        INSERT OVERWRITE {c}.gold.daily_sales_by_store
        SELECT t.transaction_date AS sales_date, t.store_id, s.store_name, s.region, t.channel,
               COUNT(*) AS txn_count, SUM(t.quantity) AS units, CAST(SUM(t.amount) AS DECIMAL(18,2)) AS revenue,
               COUNT(DISTINCT t.customer_id) AS distinct_customers
        FROM {c}.silver.transactions t JOIN {c}.silver.stores s ON t.store_id = s.store_id
        WHERE t.status = 'completed'
        GROUP BY t.transaction_date, t.store_id, s.store_name, s.region, t.channel
    """,
    )
    sql(
        spark,
        f"""
        INSERT OVERWRITE {c}.gold.category_sales_daily
        SELECT t.transaction_date AS sales_date, t.product_category_id AS category_id, p.category_name, p.department,
               COUNT(*) AS txn_count, SUM(t.quantity) AS units, CAST(SUM(t.amount) AS DECIMAL(18,2)) AS revenue
        FROM {c}.silver.transactions t JOIN {c}.silver.product_categories p ON t.product_category_id = p.category_id
        WHERE t.status = 'completed'
        GROUP BY t.transaction_date, t.product_category_id, p.category_name, p.department
    """,
    )
    sql(
        spark,
        f"""
        INSERT OVERWRITE {c}.gold.customer_monthly_summary
        SELECT CAST(date_trunc('month', t.transaction_date) AS DATE) AS sales_month, t.customer_id, cu.segment, cu.loyalty_tier, cu.country,
               COUNT(*) AS txn_count, CAST(SUM(t.amount) AS DECIMAL(18,2)) AS revenue, MAX(t.transaction_date) AS last_purchase_date
        FROM {c}.silver.transactions t JOIN {c}.silver.customers cu ON t.customer_id = cu.customer_id
        WHERE t.status = 'completed'
        GROUP BY date_trunc('month', t.transaction_date), t.customer_id, cu.segment, cu.loyalty_tier, cu.country
    """,
    )
    sql(
        spark,
        f"""
        INSERT OVERWRITE {c}.gold.event_funnel_daily
        SELECT event_date, channel, device,
               COUNT(DISTINCT session_id) AS sessions,
               SUM(CASE WHEN event_type = 'page_view'   THEN 1 ELSE 0 END) AS page_views,
               SUM(CASE WHEN event_type = 'search'      THEN 1 ELSE 0 END) AS searches,
               SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts,
               SUM(CASE WHEN event_type = 'checkout'    THEN 1 ELSE 0 END) AS checkouts,
               SUM(CASE WHEN event_type = 'purchase'    THEN 1 ELSE 0 END) AS purchases
        FROM {c}.silver.events
        GROUP BY event_date, channel, device
    """,
    )


def main():
    a = arg_parser(__doc__).parse_args()
    spark = session("seed_warehouse")
    c = a.catalog
    with timed("silver.transactions MERGE (dedup)"):
        merge_transactions(spark, c)
    with timed("silver.events MERGE (dedup)"):
        merge_events(spark, c)
    with timed("silver.customers + silver.customer_pii CDC MERGE"):
        merge_customers(spark, c)
    with timed("gold marts rebuild"):
        build_gold(spark, c)

    print("\n== row counts (current snapshot) ==")
    for t in [
        "silver.transactions",
        "silver.events",
        "silver.customers",
        "silver.customer_pii",
        "silver.stores",
        "silver.product_categories",
        "gold.daily_sales_by_store",
        "gold.category_sales_daily",
        "gold.customer_monthly_summary",
        "gold.event_funnel_daily",
    ]:
        print(f"{t:35s} {spark.table(f'{c}.{t}').count():>10}")
    print("\n== checksums ==")
    sql(
        spark,
        f"SELECT COUNT(*) AS n, SUM(amount) AS sum_amount, SUM(quantity) AS sum_qty FROM {c}.silver.transactions",
        show=True,
    )
    sql(
        spark,
        f"SELECT COUNT(*) AS n, SUM(CASE WHEN marketing_opt_in THEN 1 ELSE 0 END) AS opted_in FROM {c}.silver.customer_pii",
        show=True,
    )
    print("\n== snapshots per table ==")
    for t in ["silver.transactions", "silver.customers", "gold.daily_sales_by_store"]:
        sql(
            spark,
            f"SELECT '{t}' AS tbl, snapshot_id, committed_at, operation FROM {c}.{t}.snapshots ORDER BY committed_at",
            show=True,
        )


if __name__ == "__main__":
    main()
