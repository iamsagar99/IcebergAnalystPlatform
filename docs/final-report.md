# Final technical report: Scalable Apache Iceberg analytics platform (Trino + Kyuubi POC)

Assembled from the working documents of this repository; each section links to the file that owns the detail
rather than repeating it. Labels: **[hypothetical]** sizing/scenario, **[verified]** documentation or observed on the
pinned versions, **[measured]** POC results.

## 1. Executive summary

Meridian Retail Group [hypothetical] runs a 20 TB Iceberg warehouse on a Spark/YARN/HDFS cluster built for ETL.
100 analysts (40 concurrent) now need interactive SQL, and their private Spark sessions push the 8-hour ETL past
its window. This POC decomposes the problem (P1-P10), selects technologies against official documentation, and
delivers a runnable repository with **two complete serving paths - Trino 484 (primary) and Apache Kyuubi 1.12.0 on
Spark 3.5.8 (secondary)** - behind one Iceberg REST catalog (Lakekeeper 0.14.2) and one object store, with one
authorization model enforced on both, workload management on both, and measured benchmarks on both.

Recommendation: **Trino as the analyst-facing engine**, Kyuubi for Spark-SQL-dialect users and as the fallback
if a second engine technology is unacceptable. Reasons, evidence and the conditions under which Kyuubi wins are in
`engine-comparison.md`. ETL isolation comes from YARN guarantees + a night window + Trino's separate memory budget
(`resource-management.md`). DocDB and a semantic layer are excluded with explicit revisit triggers.

Validation [measured]: `scripts/validate.sh --engine both` -> **54 passed, 2 xfailed** (the xfails pin a verified
Kyuubi AuthZ gap with compensating controls). Seed reproducibility: identical checksums across two full re-seeds and
across both engines.

## 2. Client background and business requirements

`business-scenario.md` (people, workload, pipeline, cluster, success criteria). Core requirements: SQL for 100
analysts without per-analyst Spark sessions; ETL completion objective (gold by 10:00); PII masking/row filtering;
one authorization model; evidence-based engine comparison; runnable POC that evolves to production on HDFS.

## 3. Assumptions and constraints

`assumptions.md`: starting assumptions (A), decisions (B), POC environment (C) and the visible revisions made
while building (D), including the finding that the 9 TB of worker disk cannot hold the 20 TB warehouse (D6).

## 4. Infrastructure baseline (both variants)

`infrastructure-sizing.md`: baseline 96 cores / 128 GB / 9 TB (1.33 GB/core: RAM-bound) and the 128 vCPU / 512 GB /
8 TB variant, with per-layer allocations, how concurrency drives memory in Trino (`query.max-memory*`, spill) and in
Kyuubi (engines x executors x executor memory), disk budgets, and saturation metrics. Conclusion: hardware removes
the symptom, architecture removes the cause.

## 5. Problem statements, root causes, alternatives and trade-offs

`problem-analysis.md` P1-P10, each with business impact, root cause, candidates, recommendation, justification,
implementation approach, validation criteria and implementing files.

## 6. Final technology decisions

`implementation-decisions.md`: pinned versions with evidence; verified configuration facts with the documentation
page for each; major decisions (engine, catalog, storage, authorization, workload management, DocDB, maintenance)
with alternatives, limitations, operational cost and revisit conditions; a list of things that did not work as first
assumed.

## 7. Architecture diagram

`architecture.md` (Mermaid diagram of the deployed POC, component responsibilities, data/query/catalog/auth/resource
flows, failure scenarios, trust boundaries, deployment topology, endpoints and configuration locations).

## 8. Infrastructure and capacity plan

`infrastructure-sizing.md` + `benchmarks/benchmark-plan.md` (workload mix, ramp 1-5-10-20-40, pass/fail criteria tied
to the ETL objective, scaling caveats).

## 9. Security and authorization model

`security-model.md` + `security/roles-and-groups.md` + `security/access-policy-examples/` (Ranger Hive, Ranger
Trino, OPA). Enforcement verified by `tests/test_trino_query_access.py` and `tests/test_kyuubi_query_access.py`;
storage/catalog bypass tests in `tests/test_catalog_access.py`.

## 10. Resource-management strategy

`resource-management.md`: groups, Trino `resource-groups.json` with the assumption behind every number, Kyuubi share
levels/pools/timeouts/queues, YARN queues, ETL protection quantified, limits of each mechanism.

## 11. DocDB decision

Excluded (P6 in `problem-analysis.md`); extension point in `architecture.md`.

## 12. Schemas and data model

`sql/schema.sql`, `spark/create_tables.py` (authoritative DDL with partitioning, file size and retention properties),
`business-scenario.md` (production vs. POC sizes), `sql/sample_queries.sql` (q1-q9).

## 13. Repository structure and implementation walkthrough

`README.md` (structure, 15-step demonstration). Trino path: `infrastructure/docker-compose.trino.yml`,
`infrastructure/trino/` (config, catalog, rules, resource groups, generated credentials). Kyuubi path:
`infrastructure/docker-compose.kyuubi.yml`, `infrastructure/kyuubi/` (server config, env, AuthZ policies, Ranger stub),
`infrastructure/spark/spark-defaults.kyuubi.conf`, `infrastructure/images/Dockerfile`. Shared: core compose, Lakekeeper
bootstrap, Postgres init, Hadoop group mapping, monitoring, Spark jobs, SQL, tests, benchmark generator, notebook.

## 14. Startup and validation commands

`README.md` steps 1-15; `scripts/start.sh --engine trino|kyuubi|both`, `scripts/seed.sh`, `scripts/validate.sh
--engine both`, `scripts/benchmark.sh`, `scripts/stop.sh [--purge]`.

## 15. Benchmark methodology and measured results

Methodology: `benchmarks/benchmark-plan.md`. Measured results [measured] (laptop Docker VM 7.5 GiB, 1 M-row data
set, one engine at a time, 2026-10-26; raw runs in `benchmarks/results/`, interpretation in `engine-comparison.md`):

| engine | concurrency | ok / failed | throughput q/s | p50 s | p95 s | connect p95 s |
|---|---|---|---|---|---|---|
| Trino 484 | 5 | 170 / 0 | 5.40 | 0.54 | 2.22 | 0.43 |
| Trino 484 | 10 | 340 / 0 | 9.83 | 0.68 | 3.68 | 0.59 |
| Kyuubi 1.12.0 | 5 | 170 / 0 | 1.55 | 1.17 | 8.03 | 37.98 (engine launch) |
| Kyuubi 1.12.0 | 10 | 340 / 0 | 6.09 | 1.09 | 5.14 | 5.29 (warm engines) |

Zero failures on both; Trino ~1.6-2x lower steady-state latency; Kyuubi's cost is engine warm-up, which the idle
timeout trades against memory. Concurrency 20/40 and the ETL-overlap test remain for the staging cluster.

## 16. Known limitations

* Laptop-scale measurements only; concurrency 40 and the ETL-overlap test (benchmark steps 4-6) are specified for a
  staging cluster, not executed here.
* Kyuubi AuthZ 1.12.0 does not authorize Iceberg metadata tables (compensating controls in `security-model.md`).
* Trino `FOR TIMESTAMP AS OF` remains syntactically available inside the 7-day retention window (same masks apply).
* The Ranger Admin server is simulated locally (the plugin and policies are real).
* Kyuubi engines run in `local[2]` mode; YARN queue mapping is configured but not enforced locally.
* MinIO itself is no longer distributed as an image; the POC uses its maintained fork.

## 17. Production-readiness gaps

| Gap | What it takes |
|---|---|
| HDFS migration | decide between (a) S3-compatible gateway in front of HDFS (Apache Ozone / on-prem object store) keeping Lakekeeper, or (b) a catalog with HDFS-native locations; switch FileIO to `HadoopFileIO` / Trino `fs.hadoop.enabled`, Kerberos keytabs per engine, HDFS ACLs and encryption zones for PII |
| Identity integration | Trino LDAP/OIDC + LDAP group provider; Kyuubi LDAP/Kerberos; Hadoop LDAP group mapping; Ranger usersync; remove password file / Postgres auth table |
| Ranger / OPA | deploy Ranger Admin (or OPA), import `security/access-policy-examples/`, enable audit to Solr/HDFS; retire the stub |
| HA for catalog and coordinator | 2+ Lakekeeper replicas + Postgres HA; Trino Gateway with standby coordinator; Kyuubi servers behind ZooKeeper |
| Workload enforcement | YARN capacity scheduler queues as in `resource-management.md`; Trino resource groups tuned from benchmark steps 4-6 |
| Operations | Spark History Server, alerting on the saturation metrics, nightly maintenance in the scheduler, quarterly access review |

## 18. Implementation roadmap

1. **Weeks 1-2**: staging cluster (3 workers), HDFS decision (gap 1), identity integration, run benchmark steps 1-6.
2. **Weeks 3-4**: Ranger/OPA with AD groups, PII policies reviewed by the data-protection officer, audit pipeline.
3. **Weeks 5-6**: production rollout of the primary engine for one analyst team (BI tool connection), YARN queues,
   maintenance job in the scheduler, dashboards and alerts.
4. **Weeks 7-8**: remaining teams, HA (Trino Gateway, catalog replicas), decommission per-analyst Spark sessions.
5. **Later**: revisit triggers (DocDB, semantic layer, Kyuubi as primary) as documented.

## 19. Traceability matrix

| Problem | Recommended solution | Architectural component | Trino path files | Kyuubi path files | Validating test |
|---|---|---|---|---|---|
| P1 concurrent execution | Trino primary, Kyuubi secondary, no per-analyst Spark | engines, catalog | `infrastructure/docker-compose.trino.yml`, `infrastructure/trino/config.properties` | `infrastructure/docker-compose.kyuubi.yml`, `infrastructure/kyuubi/kyuubi-defaults.conf`, `infrastructure/spark/spark-defaults.kyuubi.conf` | `test_*_query_access.py::test_admin_runs_all_sample_queries*`, `test_group_share_level_two_analysts_share_one_engine`, benchmarks |
| P2 ETL isolation | YARN guarantees + night window + Trino separate budget; SLO gold by 10:00 | resource management | `infrastructure/trino/resource-groups.json` | `kyuubi-defaults.conf` (queues, share levels) | `test_resource_group_assignment`; benchmark step 6 (staging) |
| P3 catalog integration | Lakekeeper REST, Spark owns metadata, engines read, no credential vending | catalog, storage | `infrastructure/trino/catalog/lakehouse.properties` | `spark-defaults.kyuubi.conf` (catalog block) | `tests/test_catalog_access.py` |
| P4 snapshot access | metadata tables denied, retention 3/7/7 d, maintenance from Spark only | authorization + maintenance job | `infrastructure/trino/rules.json` (`.*\$.*`), `lakehouse.properties` (`read_only`) | `infrastructure/kyuubi/authz/policies.json`, `spark/maintenance.py`, `spark/create_tables.py` | `test_metadata_tables_blocked`, `test_time_travel_posture*`, `test_maintenance_procedure*` |
| P5 serving layer | gold marts, BI cache, no semantic layer | ETL gold step | `sql/sample_queries.sql` (q1,q2,q4,q7,q8) | same | `test_data_quality.py::test_gold_*`, benchmark mix |
| P6 DocDB | excluded, trigger documented | - | - | - | - (documented decision) |
| P7 RBAC | one model, file rules / Ranger plugin | authorization | `rules.json`, `generated/groups.txt`, `password-authenticator.properties` | `authz/policies.json`, `authz/ranger-spark-security.xml`, `hadoop/core-site.xml`, `catalog/init-postgres.sh` | `test_trino_query_access.py`, `test_kyuubi_query_access.py`, `test_catalog_access.py::test_storage_requires_engine_identity` |
| P8 workload management | groups etl/interactive/reports/heavy/admin | resource management | `resource-groups.json`, `config.properties` | `kyuubi-defaults.conf`, `spark-defaults.kyuubi.conf` | `test_resource_group_assignment`, `test_analyst_cannot_disable_authz_extension`, benchmarks |
| P9 tables and queries | 5 tables + marts, daily/monthly partitions | data model | `sql/schema.sql`, `sql/sample_queries.sql` | `sql/kyuubi/dialect-notes.md` | `test_data_quality.py` on both engines |
| P10 storage growth | 9 TB = scratch; retention + tiering policy | maintenance job, table properties | - | `spark/create_tables.py`, `spark/maintenance.py` | README step 14 (maintenance run, analysts still query) |
