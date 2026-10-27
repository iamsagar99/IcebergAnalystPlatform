# Security and authorization model

One model (`security/roles-and-groups.md`), two enforcement points, and a storage/catalog layer that no analyst
can reach directly. This document states what is enforced **in the POC** (verified by `tests/`), what changes **in
production**, and the **known gaps** with compensating controls.

## 1. Identities and groups

| Layer | POC | Production |
|---|---|---|
| Trino authentication | file password authenticator (bcrypt `password.db`, generated from `.env`), HTTPS only (self-signed) | `PASSWORD` + LDAP or `OAUTH2` (OIDC), CA certificate |
| Trino groups | file group provider (`groups.txt`) | LDAP group provider |
| Kyuubi authentication | `kyuubi.authentication=JDBC` -> Postgres `auth_users` (sha256), REST uses the same provider (basic auth) | `LDAP` or `KERBEROS` |
| Kyuubi / Ranger groups | Hadoop static group mapping (`core-site.xml`) | Hadoop `LdapGroupsMapping` / SSSD; Ranger usersync from AD |
| Engine identities | one S3 key pair (`ENGINE_S3_*`) injected into Spark, Trino and Kyuubi containers; analysts never see it | per-engine Kerberos keytabs (HDFS) and catalog OAuth2 clients |

## 2. What each role can do (enforced and tested)

| Check | Trino (rules.json) | Kyuubi (Ranger policies via AuthZ plugin) | Test |
|---|---|---|---|
| admin reads raw PII, all layers | all privileges | `admins` group: all | `test_admin_runs_all_sample_queries_and_sees_raw_pii` |
| analyst: gold + non-PII silver | SELECT grants | select policies | `test_analyst_runs_sample_queries_but_pii_is_denied` |
| analyst: PII table | **denied** (empty privileges rule) | column reads **denied**; `COUNT(*)` returns 0 (row filter `customer_id < 0 AND customer_id > 0`) | same + `test_pii_row_count_hidden_from_analyst` |
| restricted analyst: PII | masked columns + `marketing_opt_in = true` filter, `street_address` blocked | Ranger masks (`MASK_SHOW_FIRST_4`, `MASK_SHOW_LAST_4`, `MASK_HASH`, `MASK_DATE_SHOW_YEAR`) + row filter + column deny | `test_restricted_analyst_gets_masked_and_filtered_pii`, `test_street_address_blocked_for_restricted` |
| bronze | denied | column reads denied, counts 0 | `test_bronze_blocked` |
| writes (INSERT/DELETE) | denied (read-only connector) | denied (no update/create/drop privilege) | `test_writes_blocked` |
| maintenance (expire snapshots, optimize, rollback) | denied for everyone (`iceberg.security=read_only`) | denied for analysts (`CALL` needs `alter`) | `test_maintenance_procedure*` |
| metadata tables (`$snapshots`, `.snapshots`) | denied for non-admins | **not enforced - known gap** | `test_metadata_tables_blocked` (Trino pass, Kyuubi strict xfail) |
| bad password | 401 | SASL failure | `test_bad_password_rejected` |
| disabling the AuthZ extension from a session | n/a | rejected (`kyuubi.session.conf.restrict.list`) | `test_analyst_cannot_disable_authz_extension` |
| storage read-only via file paths | n/a | `spark.sql.runSQLOnFiles=false`; no credentials | `test_storage_requires_engine_identity` |

## 3. Why neither endpoint can be bypassed

1. **Storage.** The bucket refuses anonymous requests; the only credentials exist inside the engine containers
   (environment of `spark`, `trino`, `kyuubi`). On HDFS this becomes: only engine keytabs are authorized by HDFS ACLs
   on `/warehouse`; analysts have no HDFS principal.
2. **Catalog.** The warehouse profile has `sts-enabled=false` and `remote-signing-enabled=false`: a client asking
   for `X-Iceberg-Access-Delegation` gets metadata but **no** `storage-credentials` (verified by
   `test_catalog_does_not_vend_storage_credentials`). Knowing a table's metadata location is useless without
   storage identity. Production adds OpenID on the catalog so even metadata needs an engine principal.
3. **The other engine.** Both engines enforce the same group model; the same test expectations run against both.
4. **Spark directly.** Only the ETL service account and the platform team can submit Spark jobs (YARN queue ACLs
   `etl`, `admin`); the Jupyter notebook is a platform-team tool inside the engine zone, not an analyst endpoint.
5. **Policy tampering.** Trino rules and Ranger policies are files/APIs in the engine/data zone; analysts cannot
   change session properties that matter (Trino `system_session_properties` rule; Kyuubi restrict list).

## 4. Known gaps (verified) and compensating controls

| Gap | Where | Evidence | Compensating control / production fix |
|---|---|---|---|
| Iceberg metadata tables readable by analysts | Kyuubi AuthZ 1.12.0 | `SELECT * FROM lakehouse.silver.customer_pii.snapshots` allowed for `analyst1` | no storage credentials -> file paths unusable; snapshot ids only enable time travel, which still passes masks/filters; strict `xfail` test flags the day an upgrade fixes it; production: raise with the Kyuubi project / custom Spark extension that rejects `MetadataTable` relations for non-admins |
| Column-less scans (`COUNT(*)`) not authorized | Kyuubi AuthZ 1.12.0 | `SELECT COUNT(*) FROM bronze.transactions_raw` returned 1,005,000 before mitigation | Ranger row filter with a non-foldable contradiction on every denied table -> 0 rows; column reads still denied |
| Time travel (`VERSION AS OF`) reachable with discovered snapshot ids | Kyuubi | snapshot ids readable (see above) | verified: time-travel reads are authorized like current reads (analysts denied on PII columns, `COUNT(*)` 0, restricted users masked + filtered) - `test_time_travel_posture` |
| `ALTER TABLE ... EXECUTE` not governed by file rules | Trino 484 file-based access control | `expire_snapshots`/`optimize` ran for `analyst1` with SELECT only | connector set to `iceberg.security=read_only` (denied for everyone, incl. admin); maintenance is Spark-only |
| `FOR TIMESTAMP AS OF` within retention | Trino | syntax works for analysts (same masks/filters apply) | 7-day retention bounds it; not a privilege escalation; OPA/Ranger cannot see the query period either |
| Password over HTTP | Trino | server rejects | HTTPS only, port 8080 not published |

## 5. MinIO vs. HDFS: what changes

| Topic | POC (MinIO-compatible) | Production (HDFS) |
|---|---|---|
| FileIO | `S3FileIO` (Spark/Kyuubi), native S3 (Trino) | `HadoopFileIO`; Trino `fs.hadoop.enabled=true` + `hive.config.resources` |
| Engine identity | access key / secret | Kerberos principal + keytab per engine; HDFS ACLs on `/warehouse` |
| Catalog storage profile | `s3` with endpoint, path-style, vending disabled | Lakekeeper HDFS support is not available: either keep tables on an S3-compatible gateway (Ozone/MinIO on-prem) or use a catalog with HDFS-native locations (Polaris/Nessie/Hive-metastore-backed REST). This is the largest production decision left open (`final-report.md`, production-readiness gaps) |
| "Analysts cannot read storage" argument | no credentials + bucket policy | no HDFS principal + ACLs; stronger, because HDFS has no anonymous access path at all |
| Remote signing / vending | disabled | not applicable |
| Encryption | TLS to storage optional locally | HDFS transparent encryption zones for `customer_pii` partitions |

## 6. Audit

Trino: query history in `system.runtime.queries` and the HTTP server log include user, groups and resource group;
production ships the event listener to a SIEM. Kyuubi: `OperationAuditLogger` lines (`user=... opType=... state=...`)
in the server log; Ranger audit (`ranger-spark-audit.xml`) to Solr/HDFS in production records every allow/deny with
the policy id. Admin reads of `customer_pii` are therefore attributable on both paths.
