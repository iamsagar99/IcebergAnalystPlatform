# Iceberg analytics platform POC: Trino and Kyuubi on one lakehouse

A runnable proof of concept for serving ~100 analysts (40 concurrent) on an Apache Iceberg warehouse without
per-analyst Spark sessions. It ships **two complete serving paths** - **Trino 484** (primary) and
**Apache Kyuubi 1.12.0 on Spark 3.5.8** (secondary) - behind the **same Iceberg REST catalog (Lakekeeper)** and
the **same object storage**, with one authorization model enforced on both, resource management on both,
tests on both and measured benchmarks on both.

* **Runbook (single HTML page: setup, walkthrough, every test case, operations, troubleshooting): [`docs/runbook.html`](docs/runbook.html)**
* Problem decomposition: [`docs/problem-analysis.md`](docs/problem-analysis.md)
* Architecture (Mermaid, as deployed): [`docs/architecture.md`](docs/architecture.md)
* Sizing + benchmark plan: [`docs/infrastructure-sizing.md`](docs/infrastructure-sizing.md), [`benchmarks/benchmark-plan.md`](benchmarks/benchmark-plan.md)
* Security: [`docs/security-model.md`](docs/security-model.md), [`security/`](security/)
* Engine comparison with measured results: [`docs/engine-comparison.md`](docs/engine-comparison.md)
* Decisions, verified versions, research notes: [`docs/implementation-decisions.md`](docs/implementation-decisions.md)
* Final report + traceability matrix: [`docs/final-report.md`](docs/final-report.md)

## Prerequisites

* Docker Desktop / Docker Engine with Compose v2, **>= 8 GB** memory for one engine path, **>= 11 GB** for both at once
  (the POC was validated on a 7.5 GB Docker VM with both paths up and three Kyuubi engines; tight but works).
* ~8 GB of disk for images. Internet access for the first `build` (Maven Central jars) and image pulls.
* Linux/macOS shell or WSL2. All commands are run from the repository root.

```
iceberg-analytics-platform/
├── README.md                     this file (end-to-end demonstration)
├── docs/                         problem analysis, architecture, sizing, security, decisions, final report
├── infrastructure/               docker-compose.yml (core) + .trino.yml + .kyuubi.yml overlays, all configs
├── spark/                        create_tables.py, generate_data.py (seeded), seed_warehouse.py (CDC), maintenance.py
├── sql/                          schema.sql, sample_queries.sql, validation_queries.sql, kyuubi/ dialect notes
├── security/                     roles-and-groups.md, access-policy-examples/ (Ranger, OPA)
├── tests/                        pytest: data quality, catalog, Trino access, Kyuubi access
├── benchmarks/                   workload-generator.py, benchmark-plan.md, results/ (real runs only)
├── notebooks/                    JupyterLab notebook with a SparkSession for manual verification
└── scripts/                      start.sh, seed.sh, validate.sh, benchmark.sh, stop.sh, catalog-info.sh
```

## End-to-end demonstration

Every step states the exact command, what to expect, and which components talk to each other.

### 1. Start the core infrastructure (storage, catalog, Spark, monitoring)

```bash
cp infrastructure/.env.example infrastructure/.env     # local demo credentials; edit if you like, never commit
scripts/start.sh --engine core
```
Builds three images (`iap/tools`, `iap/spark`, `iap/kyuubi`), generates the local credential files
(`infrastructure/trino/generated/`, git-ignored), then starts `minio` (pgsty/silo), `postgres`, `lakekeeper-migrate`
-> `lakekeeper`, `minio-init` (bucket), `catalog-init` (bootstrap + warehouse), `spark` (JupyterLab), `prometheus`,
`grafana`. Expected tail:
```
lakekeeper: healthy
  catalog-init-1  | bootstrap: HTTP 204
  catalog-init-1  | warehouse 'lakehouse' created: {... 'sts-enabled': False, ... 'remote-signing-enabled': False ...}
spark: healthy
== endpoints ==
```
Talking: `lakekeeper -> postgres`; `catalog-init -> lakekeeper (management API) -> minio (storage validation)`.

### 2. Initialize the bucket and the catalog namespace

Done by step 1 (`minio-init` creates bucket `lakehouse`; `catalog-init` creates warehouse `lakehouse`).
Namespaces `bronze`, `silver`, `gold` are created by Spark in step 5. To re-run the bootstrap idempotently:
```bash
cd infrastructure && docker compose run --rm catalog-init && cd ..
# -> warehouse 'lakehouse' already exists: id=...
```

### 3. Confirm the Spark configuration (catalog reachable, storage credentials loaded)

```bash
cd infrastructure && docker compose exec spark bash -c 'env | grep -E "AWS_(ACCESS_KEY_ID|REGION)"; \
  spark-sql -e "SHOW NAMESPACES IN lakehouse" 2>/dev/null' && cd ..
```
Expected: `AWS_ACCESS_KEY_ID=...`, `AWS_REGION=local-01` and an empty (or `bronze/silver/gold`) namespace list.
`spark-defaults.conf` points `lakehouse` at `http://lakekeeper:8181/catalog` and `s3.endpoint=http://minio:9000`.

### 4. Generate synthetic data  and  5. Write bronze, merge silver (CDC), build gold, verify catalog metadata

```bash
scripts/seed.sh          # ~2-3 min: create_tables --reset, generate_data, seed_warehouse, catalog-info
```
Expected (abridged, real output of this repository):
```
silver.transactions                    1000000
silver.customers                         50500
gold.daily_sales_by_store                54000
|n      |sum_amount  |sum_qty|
|1000000|287866767.34|2998315|
|silver.customers|<snapshot_id>|<committed_at>|append   |
table                                  snapshots   current snapshot id  format  location
silver.customers                               1  1675475797641187707       2  s3://lakehouse/warehouse/<uuid>
```
Talking: `spark -> lakekeeper (REST commits)`, `spark -> minio (data + metadata files)`. Re-running `scripts/seed.sh`
reproduces exactly the same counts and checksums (`tests/expected.json`).

### 6. Start the Trino path and validate it

```bash
scripts/start.sh --engine trino --no-build
scripts/validate.sh --engine trino
```
Expected: `trino: healthy`, then pytest `29 passed` (catalog + data quality via Trino + role tests).
Talking: `trino -> lakekeeper (load table) -> minio (Parquet)`; clients `-> trino:8443 (HTTPS)`.

### 7. Start the Kyuubi path and validate it

```bash
scripts/start.sh --engine kyuubi --no-build
scripts/validate.sh --engine kyuubi
```
Expected: `kyuubi: healthy`, pytest `29 passed, 2 xfailed` (the two xfails document a verified AuthZ gap, see
`docs/security-model.md` section 4). First connections take ~15-20 s while an engine starts.
Talking: client `-> kyuubi:10009 -> postgres (JDBC auth)`; `kyuubi -> spark-submit engine -> lakekeeper/minio`;
engine `-> ranger-admin-stub:6080 (policies)`.

### 8. Apply analyst roles and workload policies on each engine

Policies are applied at start-up and hot-reloaded:
* Trino: `infrastructure/trino/rules.json` (10 s refresh), `resource-groups.json`, password/group files (10 s).
* Kyuubi: `infrastructure/kyuubi/authz/policies.json` (bump `policyVersion`; engines poll every 30 s),
  `kyuubi-defaults.conf` (restart `kyuubi`).
```bash
cd infrastructure && docker compose -f docker-compose.yml -f docker-compose.kyuubi.yml logs ranger-admin-stub | tail -3 && cd ..
# [ranger-admin-stub] GET /service/plugins/policies/download/lakehouse_hive?... -> 200   (one line per engine poll)
```

### 9. Connect as an analyst

Trino (CLI in the tools container, or any JDBC client with `jdbc:trino://localhost:8443/lakehouse?SSL=true&SSLVerification=NONE`):
```bash
cd infrastructure
docker compose run --rm tools python -c "
import os, trino
from trino.auth import BasicAuthentication
c = trino.dbapi.connect(host='trino', port=8443, user='analyst1', auth=BasicAuthentication('analyst1', os.environ['ANALYST_PASSWORD']), http_scheme='https', verify=False, catalog='lakehouse').cursor()
c.execute('SELECT region, SUM(revenue) FROM lakehouse.gold.daily_sales_by_store GROUP BY region ORDER BY 1'); print(c.fetchall())"
```
Kyuubi (Beeline inside the container; same JDBC URL works from DBeaver with the Hive driver):
```bash
docker compose -f docker-compose.yml -f docker-compose.kyuubi.yml exec kyuubi kyuubi-beeline \
  -u "jdbc:hive2://localhost:10009/gold" -n analyst1 -p analyst-pass \
  -e "SELECT region, SUM(revenue) FROM lakehouse.gold.daily_sales_by_store GROUP BY region ORDER BY 1"
cd ..
```
Both return five regions with identical sums. The Kyuubi session is served by the **shared `analysts` engine**
(`SET spark.app.id` returns the same application id for `analyst1` and `analyst2`), not by a private Spark app.

### 10. Run the representative queries on each engine

`sql/sample_queries.sql` runs unchanged on both engines; the test suites execute all of them per role:
```bash
cd infrastructure && docker compose run --rm -e ENGINE=both tools pytest -q tests/test_trino_query_access.py tests/test_kyuubi_query_access.py -k sample_queries && cd ..
```

### 11. Show the restricted role being denied or masked on each engine

```bash
cd infrastructure && docker compose run --rm -e ENGINE=both tools pytest -q -k "pii or masked or street_address or bronze" tests && cd ..
```
Or by hand: run q9 from `sql/sample_queries.sql` as `admin` (raw), `restricted1` (masked e-mail/phone/national id,
year-only birth date, only opted-in rows) and `analyst1` (`Access Denied` on Trino, `Permission denied: user
[analyst1] does not have [select] privilege on [silver/customer_pii/email]` on Kyuubi).

### 12. Benchmark at low concurrency and record real numbers

```bash
scripts/benchmark.sh --engine trino  --concurrency "5 10"
scripts/benchmark.sh --engine kyuubi --concurrency "5 10"
ls benchmarks/results/
```
Each run writes `<timestamp>-<engine>-c<N>.json/.md`. Measured numbers from this repository are summarised in
`docs/engine-comparison.md`.

### 13. Inspect query performance and resource use

| UI | URL | Login |
|---|---|---|
| Trino Web UI | https://localhost:8443/ui | any user from `.env` (self-signed cert) |
| Kyuubi Web UI / REST | http://localhost:10099 | basic auth, same users |
| Spark UI of Kyuubi engines | http://localhost:4050 .. 4053 | - |
| Spark UI of the ETL / notebook | http://localhost:4040 (while a job runs) | - |
| Grafana (dashboard "Lakehouse POC") | http://localhost:3000 | admin / `GRAFANA_ADMIN_PASSWORD` |
| Prometheus targets | http://localhost:9090/targets | - |
| Lakekeeper UI | http://localhost:8181 | - |
| Object browser | http://localhost:9001 | `S3_ROOT_USER` / password |
| JupyterLab (SparkSession for manual verification) | http://localhost:8888/?token=lakehouse | token from `.env` |

### 14. Run the scheduled maintenance job; analysts can still query the current snapshot

```bash
spark/spark-submit.sh maintenance.py --dry-run                 # shows what would expire
spark/spark-submit.sh maintenance.py --older-than-minutes 1    # demo: expire everything but the current snapshot
scripts/catalog-info.sh                                        # snapshot counts drop to 1, current snapshot id unchanged
cd infrastructure && docker compose run --rm -e ENGINE=both tools pytest -q tests/test_data_quality.py && cd ..   # both engines still read the current state
```
Observed in this repository: `bronze.transactions_raw` went from 2 snapshots to 1 (the re-delivered duplicate batch's
snapshot expired; `deleted_manifest_lists_count=1`), every current snapshot id stayed the same, bronze tables keep their
`min-snapshots-to-keep`, and the data-quality suite reported `18 passed` on Trino + Kyuubi afterwards.

### 15. Stop and restart everything; data, policies and the seed persist

```bash
scripts/stop.sh                       # containers down, volumes kept
scripts/start.sh --engine both --no-build
scripts/validate.sh --engine both     # same results without re-seeding
scripts/seed.sh && scripts/validate.sh --engine both   # re-seed: identical checksums (tests/expected.json)
scripts/stop.sh --purge               # full reset (volumes deleted)
```
Observed: after `stop.sh` + `start.sh --engine both --no-build` the catalog still lists all 13 tables with the same
current snapshot ids (Postgres + object-store volumes), Trino rules/resource groups and Kyuubi/Ranger policies are files
and reload unchanged, and `validate.sh --engine both` reports `54 passed, 2 xfailed` without re-seeding.

## Users (local demo)

| User | Password (`.env`) | Groups | Trino | Kyuubi |
|---|---|---|---|---|
| admin | `ADMIN_PASSWORD` | admins | all (read-only engine) | all |
| analyst1 / analyst2 | `ANALYST_PASSWORD` | analysts | gold + non-PII silver | same (shared GROUP engine) |
| restricted1 | `RESTRICTED_PASSWORD` | restricted_analysts | + masked/filtered PII | same |
| etl | `ETL_PASSWORD` | etl | all | CONNECTION-level engine |
| bench | `BENCH_PASSWORD` | analysts | own resource group | own 2-engine pool |

## Troubleshooting

* `scripts/start.sh` hangs on `kyuubi: starting`: check `docker compose ... logs kyuubi`; the first engine launch
  needs ~700 MB free memory.
* Trino `401`: the password file is regenerated from `.env` by `start.sh`; wait 10 s for the refresh.
* `docker: permission denied` on WSL right after enabling Docker Desktop integration: open a new terminal.
