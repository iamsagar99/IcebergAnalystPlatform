#!/usr/bin/env python3
"""Create the namespaces and Iceberg tables (bronze / silver / gold) through the REST catalog.

Spark is the metadata OWNER for all tables: Trino and Kyuubi only read what is created here (P3).
Partitioning, file-size and snapshot-retention properties implement P9/P4/P10 decisions
(see docs/problem-analysis.md). Run with --reset to drop everything first (reproducible seed).
"""
from lakehouse import NAMESPACES, arg_parser, session, sql

# Iceberg table properties shared by every table. LOCAL file sizes are far below the target because the
# data set is small; the properties are what production uses.
COMMON_PROPS = """
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes' = '134217728',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '20',
  'history.expire.max-snapshot-age-ms' = '{snap_age_ms}',
  'history.expire.min-snapshots-to-keep' = '{min_snaps}',
  'write.distribution-mode' = 'hash'
"""

RETENTION = {  # P4 / P10 snapshot retention per layer (snapshot age in days, minimum snapshots kept)
    "bronze": (3, 2),
    "silver": (7, 3),
    "gold": (7, 3),
}


def props(layer):
    days, keep = RETENTION[layer]
    return COMMON_PROPS.format(snap_age_ms=days * 86_400_000, min_snaps=keep)


DDL = {
    # ------------------------------------------------------------------ bronze (raw, append-only, ETL-only)
    "bronze.transactions_raw": """
        CREATE TABLE IF NOT EXISTS {c}.bronze.transactions_raw (
          transaction_id      BIGINT,
          transaction_ts      TIMESTAMP,
          store_id            INT,
          customer_id         BIGINT,
          product_category_id INT,
          quantity            INT,
          unit_price          DECIMAL(10,2),
          amount              DECIMAL(12,2),
          payment_method      STRING,
          channel             STRING,
          status              STRING,
          ingest_batch_id     INT,
          ingested_at         TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(transaction_ts))
        TBLPROPERTIES ({p})""",
    "bronze.events_raw": """
        CREATE TABLE IF NOT EXISTS {c}.bronze.events_raw (
          event_id            BIGINT,
          event_ts            TIMESTAMP,
          customer_id         BIGINT,
          session_id          STRING,
          event_type          STRING,
          channel             STRING,
          device              STRING,
          page                STRING,
          product_category_id INT,
          ingest_batch_id     INT
        ) USING iceberg
        PARTITIONED BY (days(event_ts))
        TBLPROPERTIES ({p})""",
    "bronze.customers_cdc": """
        CREATE TABLE IF NOT EXISTS {c}.bronze.customers_cdc (
          op                  STRING,          -- I / U / D
          op_ts               TIMESTAMP,
          customer_id         BIGINT,
          first_name          STRING,
          last_name           STRING,
          segment             STRING,
          country             STRING,
          city                STRING,
          loyalty_tier        STRING,
          signup_date         DATE,
          marketing_opt_in    BOOLEAN,
          email               STRING,
          phone               STRING,
          national_id         STRING,
          date_of_birth       DATE,
          street_address      STRING,
          ingest_batch_id     INT
        ) USING iceberg
        PARTITIONED BY (ingest_batch_id)
        TBLPROPERTIES ({p})""",
    # ------------------------------------------------------------------ silver (cleansed, current state)
    "silver.transactions": """
        CREATE TABLE IF NOT EXISTS {c}.silver.transactions (
          transaction_id      BIGINT,
          transaction_ts      TIMESTAMP,
          transaction_date    DATE,
          store_id            INT,
          customer_id         BIGINT,
          product_category_id INT,
          quantity            INT,
          unit_price          DECIMAL(10,2),
          amount              DECIMAL(12,2),
          payment_method      STRING,
          channel             STRING,
          status              STRING
        ) USING iceberg
        PARTITIONED BY (days(transaction_ts))
        TBLPROPERTIES ({p})""",
    "silver.events": """
        CREATE TABLE IF NOT EXISTS {c}.silver.events (
          event_id            BIGINT,
          event_ts            TIMESTAMP,
          event_date          DATE,
          customer_id         BIGINT,
          session_id          STRING,
          event_type          STRING,
          channel             STRING,
          device              STRING,
          page                STRING,
          product_category_id INT
        ) USING iceberg
        PARTITIONED BY (days(event_ts))
        TBLPROPERTIES ({p})""",
    "silver.customers": """
        CREATE TABLE IF NOT EXISTS {c}.silver.customers (
          customer_id         BIGINT,
          first_name          STRING,
          last_name           STRING,
          segment             STRING,
          country             STRING,
          city                STRING,
          loyalty_tier        STRING,
          signup_date         DATE,
          marketing_opt_in    BOOLEAN,
          updated_at          TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ({p})""",
    "silver.customer_pii": """
        CREATE TABLE IF NOT EXISTS {c}.silver.customer_pii (
          customer_id         BIGINT,
          email               STRING,
          phone               STRING,
          national_id         STRING,
          date_of_birth       DATE,
          street_address      STRING,
          country             STRING,
          marketing_opt_in    BOOLEAN,
          updated_at          TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ({p})""",
    "silver.stores": """
        CREATE TABLE IF NOT EXISTS {c}.silver.stores (
          store_id            INT,
          store_name          STRING,
          region              STRING,
          country             STRING,
          city                STRING,
          store_type          STRING,
          opened_date         DATE
        ) USING iceberg
        TBLPROPERTIES ({p})""",
    "silver.product_categories": """
        CREATE TABLE IF NOT EXISTS {c}.silver.product_categories (
          category_id         INT,
          category_name       STRING,
          department          STRING,
          margin_pct          DECIMAL(5,2)
        ) USING iceberg
        TBLPROPERTIES ({p})""",
    # ------------------------------------------------------------------ gold (serving marts, P5)
    "gold.daily_sales_by_store": """
        CREATE TABLE IF NOT EXISTS {c}.gold.daily_sales_by_store (
          sales_date          DATE,
          store_id            INT,
          store_name          STRING,
          region              STRING,
          channel             STRING,
          txn_count           BIGINT,
          units               BIGINT,
          revenue             DECIMAL(18,2),
          distinct_customers  BIGINT
        ) USING iceberg
        PARTITIONED BY (months(sales_date))
        TBLPROPERTIES ({p})""",
    "gold.category_sales_daily": """
        CREATE TABLE IF NOT EXISTS {c}.gold.category_sales_daily (
          sales_date          DATE,
          category_id         INT,
          category_name       STRING,
          department          STRING,
          txn_count           BIGINT,
          units               BIGINT,
          revenue             DECIMAL(18,2)
        ) USING iceberg
        PARTITIONED BY (months(sales_date))
        TBLPROPERTIES ({p})""",
    "gold.customer_monthly_summary": """
        CREATE TABLE IF NOT EXISTS {c}.gold.customer_monthly_summary (
          sales_month         DATE,
          customer_id         BIGINT,
          segment             STRING,
          loyalty_tier        STRING,
          country             STRING,
          txn_count           BIGINT,
          revenue             DECIMAL(18,2),
          last_purchase_date  DATE
        ) USING iceberg
        PARTITIONED BY (sales_month)
        TBLPROPERTIES ({p})""",
    "gold.event_funnel_daily": """
        CREATE TABLE IF NOT EXISTS {c}.gold.event_funnel_daily (
          event_date          DATE,
          channel             STRING,
          device              STRING,
          sessions            BIGINT,
          page_views          BIGINT,
          searches            BIGINT,
          add_to_carts        BIGINT,
          checkouts           BIGINT,
          purchases           BIGINT
        ) USING iceberg
        PARTITIONED BY (months(event_date))
        TBLPROPERTIES ({p})""",
}


def main():
    p = arg_parser(__doc__)
    p.add_argument("--reset", action="store_true", help="drop and recreate all tables (purges data files)")
    a = p.parse_args()
    spark = session("create_tables")
    c = a.catalog

    for ns in NAMESPACES:
        sql(spark, f"CREATE NAMESPACE IF NOT EXISTS {c}.{ns}")
    if a.reset:
        for name in DDL:
            sql(spark, f"DROP TABLE IF EXISTS {c}.{name} PURGE")
            print(f"dropped {name}")
    for name, ddl in DDL.items():
        layer = name.split(".")[0]
        sql(spark, ddl.format(c=c, p=props(layer)))
        print(f"created {name}")

    for ns in NAMESPACES:
        print(f"-- tables in {ns}:")
        sql(spark, f"SHOW TABLES IN {c}.{ns}", show=True)


if __name__ == "__main__":
    main()
