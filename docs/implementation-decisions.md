# Implementation decisions and technical research notes

Three kinds of statements are kept apart: **[doc]** = verified against the official documentation page cited;
**[observed]** = verified by running the pinned versions in this repository; **[assumption]** = design choice
without external evidence. Research date: 2026-10-26.

## 1. Pinned versions (mutually compatible, all exercised by `scripts/validate.sh --engine both`)

| Component | Version / image | Evidence |
|---|---|---|
| Trino | `trinodb/trino:484` (released 15 Oct 2026; JDK 25 inside the image) | [doc] https://trino.io/docs/current/release.html ; [observed] `openjdk 25.0.4` in the image |
| Trino's Iceberg library | 1.12.0 (`dep.iceberg.version`) | [doc] https://raw.githubusercontent.com/trinodb/trino/484/pom.xml |
| Apache Spark | 3.5.8, Scala 2.12, Java 17 (`apache/spark:3.5.8-scala2.12-java17-python3-ubuntu`) | [doc] Docker Hub tag list; [observed] `Spark 3.5.8 built for Hadoop 3.3.4` |
| Apache Iceberg (Spark side) | 1.11.0 runtime + aws-bundle (Maven Central, sha1 verified at build) | [doc] https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/ ; Iceberg 1.11.0 released 19 May 2026, 1.12.0 on 29 Sep 2026 and "Remove Spark 3.4 support" https://iceberg.apache.org/releases/ |
| Apache Kyuubi | 1.12.0 (`apache/kyuubi:1.12.0`, released 28 Jul 2026); built against Spark 3.5.8, Iceberg 1.11.0, Hadoop 3.4.3, Ranger 2.6.0 | [doc] https://raw.githubusercontent.com/apache/kyuubi/v1.12.0/pom.xml (and `extensions/spark/kyuubi-spark-authz/pom.xml` for `ranger.version`); [observed] `/opt/kyuubi/RELEASE` |
| Kyuubi Spark AuthZ | `kyuubi-spark-authz-shaded_2.12-1.12.0.jar` | [doc] https://repo1.maven.org/maven2/org/apache/kyuubi/kyuubi-spark-authz-shaded_2.12/ |
| Lakekeeper | `quay.io/lakekeeper/catalog:v0.14.2` (released 25 Oct 2026) | [doc] https://api.github.com/repos/lakekeeper/lakekeeper/releases ; config keys https://docs.lakekeeper.io/docs/nightly/configuration/ |
| PostgreSQL | `postgres:17` | [observed] |
| Object storage | `pgsty/silo:RELEASE.2026-09-03T13-18-01Z` (MinIO fork) | [observed] `minio/minio` repository returns 404 on Docker Hub and `docker manifest inspect minio/minio:latest` fails; Lakekeeper's own examples use this image |
| Prometheus / Grafana | `prom/prometheus:v3.5.0`, `grafana/grafana:12.1.0` | [observed] |
| Clients | `trino==0.337.0`, `pyhive[hive_pure_sasl]==0.7.0`, `pytest==8.4.1` | [observed] |

Why Iceberg 1.11.0 and not 1.12.0 on the Spark side: Kyuubi 1.12.0's build and test matrix pins 1.11.0 [doc];
Trino reads the same spec-v2 tables with its embedded 1.12.0 [observed: all tests]. Why Java 17: Iceberg 1.11
dropped Java 11 ("Drop support for Java 11 (#14400)" in the 1.11.0 notes [doc]) and the official Kyuubi image is
Java 8, so the POC copies the Kyuubi distribution onto the Spark/Java 17 image (`infrastructure/images/Dockerfile`).

## 2. Verified configuration facts (with the page each came from)

| Claim | Source |
|---|---|
| Trino REST catalog properties `iceberg.rest-catalog.uri/warehouse/security/oauth2.*/vended-credentials-enabled/view-endpoints-enabled` | https://trino.io/docs/current/object-storage/metastores.html |
| Trino native S3 file system is enabled with `fs.s3.enabled=true` (the legacy `fs.native-s3.enabled` naming is gone); `s3.endpoint`, `s3.region`, `s3.path-style-access`, `s3.aws-access-key`, `s3.aws-secret-key`; MinIO is a tested target | https://trino.io/docs/current/object-storage/file-system-s3.html |
| Trino file-based access control: rules evaluated top-down, first match wins, no match = deny; column `mask`, `filter`, user/group/role regex matching | https://trino.io/docs/current/security/file-system-access-control.html |
| Trino resource groups: `resource-groups.configuration-manager=file`, fields `hardConcurrencyLimit`, `maxQueued`, `softMemoryLimit`, `schedulingPolicy`, `schedulingWeight`, selectors by `user`, `userGroup`, `source`, `clientTags`; exhausting a resource queues new queries instead of failing running ones | https://trino.io/docs/current/admin/resource-groups.html |
| Trino password file: bcrypt via `htpasswd -B -C 10`; TLS + shared secret required; `http-server.authentication.allow-insecure-over-http` is a testing-only switch and passwords are rejected over HTTP | https://trino.io/docs/current/security/password-file.html , https://trino.io/docs/current/admin/properties-http-server.html ; [observed] `401 Password not allowed for insecure authentication` |
| Trino file group provider: `group-provider.name=file`, `file.group-file`, `group:user1,user2` lines | https://trino.io/docs/current/security/group-file.html |
| Trino `${ENV:VAR}` substitution in any properties file | https://trino.io/docs/current/security/secrets.html |
| Trino OpenMetrics `/metrics` on the coordinator, `openmetrics.jmx-object-names`, needs authentication | https://trino.io/docs/current/admin/openmetrics.html |
| Trino time travel `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`, metadata tables `$snapshots`, `$history`, `$files`...; `expire_snapshots` / `remove_orphan_files` with 7d minimum retention | https://trino.io/docs/current/connector/iceberg.html |
| Trino file-based rules do not govern `ALTER TABLE ... EXECUTE` table procedures; `iceberg.security=read_only` denies them | [observed] on 484 (`docs/security-model.md`) |
| Kyuubi JDBC authentication keys and `${user}` / `${password}` placeholders; query must start with SELECT | https://kyuubi.readthedocs.io/en/v1.12.0/security/jdbc.html |
| Kyuubi share levels CONNECTION/USER/GROUP/SERVER, URL override, subdomain | https://kyuubi.readthedocs.io/en/v1.12.0/deployment/engine_share_level.html |
| Kyuubi `kyuubi.session.engine.idle.timeout` (PT30M default), `kyuubi.engine.pool.size`, `pool.size.threshold`, `pool.selectPolicy`, `kyuubi.session.group.provider=hadoop`, `kyuubi.session.conf.restrict.list` / `ignore.list`, metrics reporter PROMETHEUS on 10019, user defaults `___user___.key` | https://kyuubi.readthedocs.io/en/v1.12.0/configuration/settings.html , https://kyuubi.readthedocs.io/en/v1.12.0/deployment/engine_lifecycle.html |
| Kyuubi AuthZ installation: shaded jar on the Spark classpath, `ranger-spark-security.xml` keys, `spark.sql.extensions=...RangerSparkExtension`, Hive service definition | https://kyuubi.readthedocs.io/en/v1.12.0/security/authorization/spark/install.html |
| Kyuubi AuthZ cache file name `sparkSql_<service>.json`; Ranger-UI date format in service definitions is not parseable by the shaded Jackson; `kyuubi.session.user.sign.enabled=true` breaks engine bootstrap with AuthZ | [observed] engine logs |
| Hadoop `hadoop.user.group.static.mapping.overrides` for static user->group mapping; Kyuubi `HadoopGroupProvider` uses `UserGroupInformation.getGroupNames` and falls back to the user name | https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/core-default.xml ; Kyuubi source `HadoopGroupProvider.scala` |
| Iceberg Spark catalog keys `type=rest`, `uri`, `warehouse`, `io-impl`; `spark.sql.extensions=IcebergSparkSessionExtensions`; `expire_snapshots(table, older_than, retain_last, ...)` default 5 days / 1; `remove_orphan_files(older_than default 3 days, dry_run)` | https://iceberg.apache.org/docs/latest/spark-configuration/ , https://iceberg.apache.org/docs/latest/spark-procedures/ |
| Lakekeeper S3 profile: `sts-enabled` controls vending, `remote-signing-enabled` controls signing; "a profile that disables both returns no credentials, whatever the header says"; MinIO should use vended credentials rather than remote signing | https://docs.lakekeeper.io/docs/nightly/storage/ ; [observed] `test_catalog_does_not_vend_storage_credentials` |
| Lakekeeper `migrate` then `serve`, `/management/v1/bootstrap`, `/management/v1/warehouse`, `/catalog` prefix is the warehouse id (`defaults.prefix` in `/v1/config`) | https://raw.githubusercontent.com/lakekeeper/lakekeeper/main/examples/minimal/docker-compose.yaml ; [observed] |
| Apache Ranger official images exist (`apache/ranger:2.8.0` + `ranger-db` + `ranger-solr`, "development only") | https://hub.docker.com/r/apache/ranger |
| Docker Compose: `depends_on` with `service_healthy` / `service_completed_successfully`, `$$` escaping in healthcheck shell strings | [observed] (the Trino healthcheck needed `$$out`) |

## 3. Major decisions

### Serving engine (P1)
* **Alternatives:** Trino, Spark Thrift Server, Kyuubi (plus Presto/Starburst and Dremio, excluded: not asked, commercial/overlapping).
* **Choice:** Trino primary, Kyuubi secondary; both implemented end to end.
* **Why:** no warm-up, admission control, authorization covering metadata tables, separate memory budget from YARN (`engine-comparison.md` for the measured side).
* **Known limitations:** Trino SQL differs from Spark SQL (ETL views not reusable); coordinator SPOF without Trino Gateway; Trino cannot write/maintain here by design.
* **Operational cost:** one more engine technology, ~3 GB per worker reserved, Trino Gateway for HA, policy service (Ranger/OPA).
* **Revisit when:** most analyst work is Spark-SQL-dialect notebooks reusing ETL code; or the client standardises on YARN-only operations; or Trino's memory reservation starves the ETL on the baseline cluster (benchmark step 6).

### Catalog (P3)
* **Alternatives:** Polaris, Nessie, Gravitino, Iceberg REST fixture, Lakekeeper.
* **Choice:** Lakekeeper 0.14.2. **Why:** smallest footprint, Postgres persistence (restart test), per-warehouse control over credential vending, OpenID/OpenFGA for production, worked first time with all three engines.
* **Limitations:** no HDFS storage profile (production must put the warehouse behind an S3-compatible gateway such as Apache Ozone or keep HDFS with a catalog that supports it); non-Apache governance.
* **Cost:** Postgres HA + 2 stateless replicas. **Revisit when:** HDFS-native locations are mandatory (then Polaris or a Hive-metastore-backed REST catalog) or multi-catalog federation is needed (Gravitino).

### Storage (POC)
* `pgsty/silo` because upstream MinIO images are gone; S3 API unchanged. Production is HDFS (requirements). **Revisit:** if the client wants object storage on-prem, Ozone or Ceph RGW.

### Authorization mechanism (P7)
* **Alternatives:** Trino file rules / Ranger Trino plugin / OPA; Kyuubi AuthZ+Ranger / storage-only.
* **Choice:** file rules (POC) -> Ranger or OPA (prod) for Trino; Kyuubi AuthZ with Ranger (real plugin, stubbed Admin locally).
* **Limitations:** Kyuubi AuthZ gaps (metadata tables, column-less scans) with compensating controls; Trino `FOR TIMESTAMP AS OF` within retention.
* **Cost:** Ranger Admin + usersync (or OPA + policy repo). **Revisit:** Kyuubi release notes closing the metadata-table gap (strict xfail test will flag it).

### Workload management (P8)
* Trino resource groups + per-query caps; Kyuubi share level/pool/timeouts + YARN queues. **Limitations:** admission-only (Trino), per-engine FAIR scheduler (Kyuubi). **Revisit:** when queueing p95 > 2 s at the measured peak.

### DocDB (P6)
* Excluded; trigger documented in `problem-analysis.md`.

### Maintenance policy (P4/P10)
* Nightly `expire_snapshots` + `remove_orphan_files` from Spark with per-layer retention 3/7/7 days, weekly rewrites; analysts cannot run maintenance on either engine. **Revisit:** when snapshot counts or planning times rise (metrics in `storage-and-retention.md`).

## 4. Things that did not work as first assumed (kept for the next engineer)

| Attempt | What happened | Fix |
|---|---|---|
| Trino password auth over HTTP with `allow-insecure-over-http=true` | server answers `401 Password not allowed for insecure authentication` | HTTPS 8443 with generated PEM; HTTP port unpublished |
| Trino spill path on a fresh named volume | `spill path /tmp/trino-spill is not accessible` (root-owned) | spill under `/data/trino/spill` (image-owned dir) |
| `group` field in `system_information` rules | Jackson `UnrecognizedPropertyException` (only `user`, `role`, `allow`) | user-based rule |
| Trino image has no `grep` | healthcheck failed silently | shell `case` pattern on `/v1/info` |
| `$out` in a Compose healthcheck | Compose interpolated it to empty | `$$out` |
| Ranger service definition from a UI export | `Cannot deserialize value of type java.util.Date from "20190401-20:14:36.000-+0800"` | stub strips `createTime`/`updateTime` |
| `kyuubi.session.user.sign.enabled=true` with AuthZ | `Invalid user identifier [null]` at engine bootstrap | left at default |
| Kyuubi frontends bound to the container IP | `localhost:10009` refused inside the container; healthcheck stuck | `kyuubi.frontend.*.bind.host=0.0.0.0` |
| PyHive issues `USE default` | `SCHEMA_NOT_FOUND lakehouse.default` | client connects to database `gold` |
| Ranger row filter `1 = 0` | constant-folded: denials became empty results | `key < 0 AND key > 0` |
| Kyuubi REST `/sessions` as admin | lists only the caller's sessions | engine sharing verified via `SET spark.app.id` inside the sessions |
| Module-level `F.lit()` in a PySpark job | `AssertionError` before the session exists | build columns lazily |
| CDC rows updated and deleted in the same batch with equal timestamps | non-deterministic merge (50,520 vs 50,500) | deletes get a later timestamp |
