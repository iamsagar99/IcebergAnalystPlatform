# Problem decomposition (P1-P10)

Each entry: problem statement, business impact, technical root cause, requirements and constraints, candidate
solutions with advantages/disadvantages, recommended solution, technical justification, implementation approach,
validation criteria, implementing files. Facts verified against documentation or observed on the pinned versions
are marked **[verified]**; sizing is marked **[assumption]**; POC measurements **[measured]** live in
`engine-comparison.md` and `benchmarks/results/`.

---

## P1. Concurrent analytical query execution

**Problem.** 100 analysts (40 concurrent) need interactive SQL on the Iceberg warehouse without each opening a Spark session.

**Business impact.** Today every notebook/BI connection is a private Spark application: ~1.5-4 GB of driver + executor
memory each. 20 of them consume the whole 128 GB cluster, starve the ETL and give analysts 30 s - 3 min latencies
for queries that should take seconds. The 10:00 gold-refresh objective is missed on busy days.

**Root cause.** Spark is a batch engine with a per-application driver and JVM warm-up; it has no multi-tenant SQL
front door, no admission control across applications and no fine-grained authorization. The cluster has 1.33 GB
RAM per core, so memory, not CPU, is exhausted first.

**Requirements and constraints.** SQL over JDBC/ODBC for BI tools; sub-10 s latency for gold-mart queries; one
authorization model with masking/row filters; no per-analyst Spark application; must run next to the ETL on
YARN; team knows Spark SQL well, Trino not at all (assumption).

**Candidate solutions (exactly three evaluated).**

| | Trino + Iceberg connector | Spark Thrift Server (STS) | Apache Kyuubi (multi-tenant Spark SQL gateway) |
|---|---|---|---|
| Concurrency model | one long-running cluster; every query is split into tasks over all workers; admission by resource groups | one SparkContext, one driver, FAIR scheduler pools; all users share one driver JVM | one server, N Spark engines (share level CONNECTION/USER/GROUP/SERVER), engine pools, idle timeout; each engine is a Spark app on YARN |
| Per-query latency / warm-up | no warm-up: JVM is hot, planning in ms, no Spark job scheduling overhead | hot after the first query, but every query goes through Spark job/stage scheduling (seconds) | first connection of a user/group pays engine launch (30-90 s on YARN); afterwards like STS |
| Fault isolation | a query failure never affects other queries; a worker loss fails only queries on it; coordinator is a SPOF (HA via Trino Gateway) | none: one OOM kills every user's session | per engine: an analyst group's OOM kills that group's engine only; the Kyuubi server itself holds no query state beyond session bookkeeping and can be made HA with ZooKeeper |
| SQL dialect vs. existing ETL | ANSI SQL, **not** Spark SQL: ETL code is not reusable; functions/types differ (see `sql/kyuubi/dialect-notes.md`) | identical to ETL | identical to ETL: views, UDFs and SQL written for the pipeline run unchanged |
| Fine-grained authorization | file-based rules (POC), Ranger plugin or OPA (prod): table, column mask, row filter; metadata tables governed by table rules **[verified]** | none (storage-level only) | Kyuubi AuthZ (Ranger) plugin: table/column/mask/row filter **[verified]**; gaps on Iceberg metadata tables and column-less scans **[verified, see security-model.md]** |
| Operational complexity | new engine to learn; separate memory budget; simple config; excellent UI and metrics | trivial, already there | Kyuubi server + ZooKeeper (HA) + N Spark engines on YARN + Ranger; more moving parts but all Spark-familiar |
| Team skills | new | existing | existing (Spark) + Kyuubi |

**Why STS is not sufficient on its own.** One driver means one failure domain and one memory budget for all 100
analysts, no per-user isolation, no authorization, and the FAIR scheduler only shares executors inside that single
application. It is the baseline that explains what Kyuubi adds.

**Recommended solution: Trino as the primary engine, Kyuubi as the secondary (both shipped).** Trino's
warm cluster and admission control fit the interactive/BI profile (80% gold-mart reads, assumption A6); it also
keeps the analyst memory budget physically separate from YARN, which is the simplest way to protect the ETL (P2).
Kyuubi remains the right tool for analysts who need Spark SQL compatibility (data scientists re-using ETL views,
notebooks with JDBC) and is the fallback if the client refuses to operate a second engine technology.

**Technical justification.** Trino 484 [verified]: Iceberg REST catalog support, file/Ranger/OPA access control with
column masks and row filters, resource groups, spill. Kyuubi 1.12.0 [verified]: Spark 3.5 engines, share levels,
pools, idle timeout, JDBC/LDAP/Kerberos auth, Ranger AuthZ plugin. Measured POC numbers: `engine-comparison.md`.

**Implementation approach.** Both paths run against the same catalog/storage/tables; same `sql/sample_queries.sql`.
**Validation.** `scripts/validate.sh --engine both`; benchmarks at 5/10 concurrent on both engines.
**Files.** Trino: `infrastructure/docker-compose.trino.yml`, `infrastructure/trino/*`. Kyuubi: `infrastructure/docker-compose.kyuubi.yml`,
`infrastructure/kyuubi/*`, `infrastructure/spark/spark-defaults.kyuubi.conf`, `infrastructure/images/Dockerfile` (target `kyuubi`).

---

## P2. ETL and analytical workload isolation

**Problem.** Decide whether ETL and analytics share compute, and keep the 8-hour ETL inside its window.
**Impact.** Every hour of ETL overrun delays the gold refresh the business runs on; today's overruns reach 13:00.
**Root cause.** Unmanaged analyst Spark sessions take executors from the same YARN capacity as the ETL.

**Candidates.**

| Option | Advantages | Disadvantages |
|---|---|---|
| Separate clusters | perfect isolation; independent sizing | doubles hardware on a cluster that is already RAM-bound; two HDFS clients, data copies or remote reads |
| One cluster, YARN queue isolation | no new hardware; guarantees + preemption; works for Kyuubi engines | queues share disks/network; preemption is slow (kills executors); Trino is not a YARN application |
| Time-window scheduling (ETL at night) | zero contention for most of the ETL | ETL overlaps analysts 08:00-10:00; late source deliveries break the assumption |
| Combination | each mechanism covers the others' gaps | more policy to maintain |

**Recommendation: combination.** ETL at 02:00-10:00 in a guaranteed YARN queue (60%, can grow to 100% at night,
preempts above-guarantee usage), analyst engines (Kyuubi) in a 30% `interactive` queue, and Trino with its own
memory budget on the same nodes (`infrastructure-sizing.md`), reserved out of the 128 GB. Quantification of
analyst load vs. ETL finish time is in `resource-management.md` section 4: unmanaged 40% contention = 11:40,
managed overlap <= 15% = 10:20; objective **gold by 10:00, SLO 99%**. Protected capacity: 60% of YARN during
overlap, 100% from 02:00-08:00.

**Validation.** Production: ETL stage-duration alert; POC: resource-group and queue settings are present and
tests show per-user admission (`test_resource_group_assignment`). **Files.** `infrastructure/trino/resource-groups.json`,
`infrastructure/kyuubi/kyuubi-defaults.conf` (queues per user), `docs/resource-management.md`.

---

## P3. Iceberg catalog integration

**Problem.** Spark (ETL), Trino and Kyuubi must share one metadata source and one storage layout.
**Impact.** Without a shared catalog each engine would re-register tables; snapshots would diverge; authorization
could not be centralised.
**Root cause.** Iceberg stores table state in metadata files; a catalog is the pointer to the current metadata and
the coordination point for commits.

**Candidates (REST catalogs).**

| Catalog | Verified facts | Advantages | Disadvantages |
|---|---|---|---|
| **Lakekeeper v0.14.2** (chosen) | Rust binary (~100 MB RSS here), Postgres backend, OpenID auth + OpenFGA/Cedar authz, credential vending or none, examples with Trino/Spark/MinIO-fork **[verified: runs in this POC]** | tiny footprint, simple bootstrap (2 API calls), view endpoints, per-warehouse storage profile with vending switched off | younger project than Nessie; non-Apache governance |
| Apache Polaris 1.x | Apache (incubating) REST catalog with OAuth2 principals/roles, Postgres persistence | built-in catalog-level RBAC per engine principal | heavier (JVM), bootstrap via admin tool, more steps for MinIO |
| Project Nessie | git-like branches/tags, REST catalog support (Iceberg REST API) | branching for ETL isolation | branches add a concept analysts don't need; the requirements exclude time-travel-style access for analysts |
| Apache Gravitino | metalake over multiple catalogs/engines | useful when many catalogs exist | heaviest; over-scope for one warehouse |
| Iceberg reference REST image (`apache/iceberg-rest-fixture`) | reference implementation | simplest | no auth, in-memory by default (fails the restart/persistence requirement) |

**Design.** Spark **owns** metadata: it creates tables, commits every write and runs maintenance. Trino and
Kyuubi **execute queries** (Trino read-only; Kyuubi engines read-only for analysts by policy). The catalog holds
pointers + namespaces + warehouse storage profile; data and metadata files live in object storage (MinIO-compatible
locally) under `s3://lakehouse/warehouse/<table-uuid>/`. Authentication to the catalog: none locally (Docker
network only) **[LOCAL]**; production: OAuth2 client credentials per engine principal (`spark-defaults.conf`
and `lakehouse.properties` show the exact keys) with Lakekeeper OpenID + OpenFGA so only engine principals may
load tables. Storage credentials are never vended (`sts-enabled=false`, `remote-signing-enabled=false`) - each engine
brings its own identity, exactly as with Kerberos on HDFS.

**Verified version matrix.** Spark 3.5.8 + Iceberg runtime 1.11.0 (Java 17) <-> Lakekeeper 0.14.2 <-> Trino 484
(Iceberg lib 1.12.0) <-> Kyuubi 1.12.0 (built against Spark 3.5.8 / Iceberg 1.11.0 per its pom). All combinations
exercised by `tests/test_catalog_access.py` and both engine suites.

**Files.** `infrastructure/catalog/bootstrap.py`, `infrastructure/docker-compose.yml` (lakekeeper*), `infrastructure/spark/spark-defaults*.conf`,
`infrastructure/trino/catalog/lakehouse.properties`.

---

## P4. Snapshot access

**Problem.** Analysts query the current snapshot only; time-travel syntax and metadata tables must be unavailable
to them on both engines while snapshots, manifests and commit semantics stay intact.
**Impact.** Metadata tables expose file paths and snapshot ids; time travel lets a user read data from before a
GDPR deletion or before a CDC correction.
**Root cause.** Iceberg exposes history as first-class SQL (`FOR VERSION AS OF`, `$snapshots`, `.history`), and
engines treat those reads like any other.

**Candidates.** (a) authorization rules on metadata-table names; (b) short snapshot retention; (c) serving views
instead of tables; (d) engine switches (none exist in either engine **[verified]**); (e) custom Spark extension
(code to build and maintain).

**Recommendation.** (a) + a retention window chosen for operations, not for access control, + documented residual
risk. Trino: table rule `.*\$.*` with no privileges for non-admins hides every metadata table **[verified]**;
`FOR VERSION AS OF <guessed id>` fails; `FOR TIMESTAMP AS OF` inside the 7-day window remains syntactically possible
but reads through the same masks and row filters (not an escalation). Kyuubi: Ranger policies deny everything
not granted, but the AuthZ plugin **does not authorize Iceberg metadata tables** (`.snapshots`, `.history`, `.files`
readable; **[verified gap]**) and skips the column check on time-travel relations; compensating controls: row
filters and masks still apply to time-travel reads **[verified]**, analysts hold no storage credentials, and the gap
is pinned by a strict `xfail` test so an upgrade that fixes it is noticed. Snapshot expiry (3/7/7 days, min 2/3
kept) and orphan-file removal are scheduled nightly from Spark (`spark/maintenance.py`), owned by the platform
team: Trino cannot run them (`read_only`), Kyuubi analysts have no `alter` privilege **[verified]**.

**Validation.** `test_metadata_tables_blocked` (Trino pass; Kyuubi documented xfail), `test_time_travel_posture*`,
`test_maintenance_procedure*`, README step 14 (maintenance run, analysts still query).
**Files.** `infrastructure/trino/rules.json`, `infrastructure/kyuubi/authz/policies.json`, `spark/create_tables.py` (retention properties), `spark/maintenance.py`.

---

## P5. Serving-layer data modeling and pre-aggregation

**Problem.** Analysts should rarely scan silver.
**Impact.** A single silver scan reads ~0.8 GB/day of transactions (production); 40 analysts doing that at once is
the I/O profile that hurts the ETL.
**Root cause.** The warehouse was modelled for ETL correctness (normalized silver), not for the questions analysts ask.

**Gold marts and what they answer.**

| Mart | Answers (sample queries) | Rows (POC) vs. silver rows scanned | Refresh |
|---|---|---|---|
| `gold.daily_sales_by_store` | q1 revenue by region/day, q8 store lookups | 54,000 vs. 1,000,000 (**18x fewer rows**; in bytes ~25x because the mart is narrow) | rebuilt after each silver load (INSERT OVERWRITE); incremental by day in production |
| `gold.category_sales_daily` | q2 top categories | 3,600 vs. 1,000,000 (278x) | same |
| `gold.customer_monthly_summary` | q7 top spenders, customer 360 | 147,723 vs. 1,000,000 x 50,500 join | same |
| `gold.event_funnel_daily` | q4 funnel conversion | 540 vs. 600,000 (1,111x) | same |

q3 (fact x dimension join) and q5 (last-7-days events) deliberately stay on silver to represent the 20% of
"unanswered" questions and to give the benchmark a heavy class.

**BI-side result cache.** Recommended: Power BI/Tableau extracts or cached live connections for dashboards that
refresh hourly; it removes repeated identical queries (the dominant pattern for dashboards) from both engines. Trino
can add a coordinator-side result cache in future versions; not assumed.

**Semantic layer (Cube).** Not added. The query patterns are a handful of marts with stable grains; a semantic layer
would add a service, a cache tier and a modelling language for a problem the marts already solve. Trigger to
revisit: more than ~5 BI tools needing shared metric definitions, or row-level security that must be defined once
for tools that bypass SQL authorization.

**Files.** `spark/seed_warehouse.py` (`build_gold`), `sql/sample_queries.sql`, `benchmarks/workload-generator.py` (`MIX`).

---

## P6. DocDB evaluation

**Problem.** Does a document database belong in the architecture?
**Use cases that could benefit:** application-style look-ups (one customer, one order) at high QPS with
millisecond latency; serving a customer-facing app; storing semi-structured event payloads.
**Assessment.** None of the stated users is an application: 100 analysts with SQL clients and BI tools. Point
look-ups (q6, q8) are rare and served in well under a second by the gold marts on both engines (measured in
`engine-comparison.md`). Caching/pre-aggregation (P5) covers the "fast repeated question" case. A DocDB would
require a sync pipeline (Iceberg -> DocDB) with its own freshness lag, a second PII copy with its own masking and
deletion handling, and a new operational system (backups, HA, upgrades).
**Decision: excluded.** Copying data to analyst laptops is explicitly not an alternative either (no laptop tier).
**Trigger to revisit.** A customer-facing or operational application needs > 100 look-ups/s with p99 < 50 ms, or
analysts need sub-second response on single-key queries during ETL I/O peaks. Then: MongoDB or PostgreSQL JSONB fed
by an incremental Iceberg CDC read (`spark.read.format("iceberg").option("start-snapshot-id", ...)`) through the
extension point described in `architecture.md`.
**Files.** none (documented exclusion); extension point in `architecture.md`.

---

## P7. Role-based access control

**Problem.** One centralised authorization model enforced on both engines.
**Model.** Identities from AD/LDAP (locally: password file + Postgres auth table); groups = roles (`admins`, `etl`,
`analysts`, `restricted_analysts`, `monitors`); table-level grants; column masks; row filter; PII policy; admin access
audited; quarterly access review (`security/roles-and-groups.md`).
**Enforcement.** Trino: system access control, file-based in the POC (`rules.json`: table denial, masks, row filter,
metadata-table denial), Ranger Trino plugin or OPA in production (`security/access-policy-examples/`). Kyuubi: Kyuubi
Spark AuthZ plugin with Ranger policies (real plugin, stubbed Ranger Admin locally; real Ranger Admin in production).
**Why nothing can be bypassed.** Direct Spark access is restricted to the ETL/platform service accounts (the `spark`
container identity holds the storage keys; analysts never receive them). Storage: anonymous access is refused and the
catalog never vends credentials (`test_catalog_does_not_vend_storage_credentials`, `test_storage_requires_engine_identity`).
Catalog API: production requires OAuth2 principals (engines only); locally the API is reachable only on the Docker
network. The other engine: both enforce the same model, so switching engines does not change what a user can see
(`test_trino_query_access.py` and `test_kyuubi_query_access.py` run the same expectations). Known Kyuubi gaps are
listed with compensating controls in `security-model.md`.
**Files.** `infrastructure/trino/rules.json`, `infrastructure/trino/generated/*` (generated), `infrastructure/kyuubi/authz/*`,
`infrastructure/catalog/init-postgres.sh`, `infrastructure/hadoop/core-site.xml`, `security/`.

---

## P8. Resource groups and workload management

See `resource-management.md` for the full model. Summary: groups `etl`, `interactive` (per-user sub-groups),
`scheduled_reports`, `adhoc_heavy`, `admin`, `bench`; Trino `resource-groups.json` (concurrency, queue depth, soft
memory, weights, selectors by group/source/tag); per-query caps in `config.properties`; Kyuubi share levels, pool,
idle timeout, queue mapping, result/row/time caps and conf restriction lists; YARN capacity queues in production.
Every percentage is an assumption explained next to the number.
**Files.** `infrastructure/trino/resource-groups.json`, `infrastructure/trino/config.properties`, `infrastructure/kyuubi/kyuubi-defaults.conf`,
`infrastructure/spark/spark-defaults.kyuubi.conf`.

---

## P9. Table characteristics and query patterns

Five sample tables (plus one extra lookup) with schemas, production row estimates, partitioning and file sizes
are defined in `business-scenario.md` and `sql/schema.sql`; representative queries in `sql/sample_queries.sql` (q1-q9).
Evaluation: daily partitions on the two big tables match the date-range predicates of every representative query;
monthly partitions on marts keep them at a few files; the dimension and PII tables are unpartitioned (CDC merge
touches random keys, partitions would not prune). Small-file remediation is limited to the weekly rewrite schedule
in `storage-and-retention.md`.
**Files.** `spark/create_tables.py`, `spark/generate_data.py`, `sql/schema.sql`, `sql/sample_queries.sql`.

---

## P10. Storage growth and retention

See `storage-and-retention.md`: the 9 TB is scratch, not warehouse storage (revision D6); growth model with and
without expiry/compaction; retention 3/7/7 days of snapshots; bronze deleted after 30 days; silver tiered to cold
HDFS storage policy after 24 months; triggers for adding object storage.
**Files.** `spark/create_tables.py` (table properties), `spark/maintenance.py`, `docs/storage-and-retention.md`.
