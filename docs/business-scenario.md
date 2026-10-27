# Business scenario (hypothetical client)

> Everything in this file is a **hypothetical** scenario constructed for the POC. Numbers are starting assumptions,
> not measurements. Where the design revises an assumption the revision is recorded in `assumptions.md`.

## The client: Meridian Retail Group (fictional)

Meridian Retail Group is an omnichannel retailer (about 1,200 stores in five countries plus web and mobile apps).
Two years ago its data engineering team built an Apache Iceberg lakehouse on an on-premises Spark + YARN + HDFS
cluster to replace nightly extracts. The warehouse was designed for ETL, not for people: it works, but the analysts
who were promised "self-service SQL" now share the ETL cluster, and every analyst who opens a notebook starts a
private Spark application.

## People and workload

| Item | Assumption |
|---|---|
| Analysts | 100 (finance, merchandising, marketing, store operations, data science) |
| Peak concurrency | 40 concurrent analysts (assumed; the benchmark plan measures it) |
| Access style | SQL only: BI tool (Power BI / Tableau style, live connection), SQL client (DBeaver), notebooks with JDBC |
| Typical query | aggregate last 1-90 days of sales by store / category / channel; funnel analysis; customer look-ups |
| Time travel | not required for ordinary analysts; the platform team uses it for incident analysis |
| Data sensitivity | ordinary business data plus customer PII (email, phone, national id, date of birth, address) |

## Existing ETL pipeline

* Medallion layout `bronze -> silver -> gold`, Spark SQL jobs orchestrated by Airflow (assumed).
* Change data capture from the customer master (Debezium-style `I/U/D` records) merged into `silver.customers`.
* End-to-end ETL runtime ~8 hours/day today (02:00-10:00 when uncontended), writing 200-300 GB/day.
* Warehouse: ~20 TB in ~300 Iceberg tables (Parquet, zstd), ~1.1 billion transactions/year.

## Existing cluster (shared by ETL and analysts today)

| Resource | Value |
|---|---|
| Resource manager | **YARN** (chosen; the client's Spark jobs are already submitted to YARN queues) |
| Workers combined | 96 cores, 128 GB RAM, 9 TB local disk |
| RAM per core | **1.33 GB** - RAM is the binding constraint, not CPU |
| Storage | HDFS, colocated on the same nodes (DataNodes share the 9 TB) |

Today's failure mode: analyst Spark sessions take executors from the ETL queue, the 8-hour ETL sometimes finishes
at 13:00 instead of 10:00, and the business sees stale gold tables plus unpredictable query latency.

## Representative tables (local POC sizes vs. production estimates)

| Table | Role | Production (est.) | POC rows | Partitioning |
|---|---|---|---|---|
| `silver.transactions` | large fact | ~1.1 B rows/yr, ~300 GB/yr | 1,000,000 | `days(transaction_ts)` |
| `silver.events` | medium event table | ~4 B rows/yr | 600,000 | `days(event_ts)` |
| `silver.customers` | small dimension (CDC) | ~12 M rows | 50,500 | none |
| `silver.customer_pii` | restricted PII | ~12 M rows | 50,500 | none |
| `silver.stores`, `silver.product_categories` | lookups | 1,200 / 400 rows | 200 / 40 | none |
| `gold.*` | serving marts | tens of millions | 540 - 147,723 | `months(date)` |

Full schemas: `sql/schema.sql`; generation logic: `spark/generate_data.py`.

## Success criteria for the POC

1. 100 analysts can run SQL without opening Spark sessions, on either engine, with one authorization model.
2. ETL keeps a protected share of the cluster and a completion-time objective (`problem-analysis.md`, P2).
3. The restricted PII table is masked and row-filtered for restricted analysts and denied to everyone else.
4. The same workload runs on Trino and on Kyuubi, with measured numbers rather than claims (`benchmarks/`).
5. Another engineer can start, seed, validate, benchmark, stop and restart the whole thing from the README.
