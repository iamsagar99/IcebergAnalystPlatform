# Assumptions and revisions

Every unspecified item was decided here and carried into the design. **Hypothetical sizing**, **verified facts**
(documentation / observed on the pinned versions) and **measured results** are labelled so they are never confused.

## A. Client, workload and cluster (hypothetical unless noted)

| # | Assumption | Basis | Status |
|---|---|---|---|
| A1 | 100 analysts, 40 peak concurrent | given by the requirements | hypothetical; benchmark plan tests concurrency 1-40 |
| A2 | Resource manager is **YARN**, not Kubernetes | existing Spark/HDFS cluster; YARN capacity scheduler gives queue guarantees and preemption needed for P2 | decision |
| A3 | HDFS DataNodes are colocated on the 9 TB worker disks | requirements say "if colocated" | **revised** (D6): impossible at 20 TB; the 9 TB is scratch |
| A4 | ETL runs 02:00-10:00 when uncontended; the business objective is **gold refreshed by 10:00** | requirements: ~8 h/day | hypothetical SLO used in P2 |
| A5 | Daily writes 250 GB (midpoint of 200-300) and Parquet/zstd compresses raw input ~3x | requirements + common ratio | hypothetical, P10 |
| A6 | Analyst queries are 80% gold-mart reads, 20% silver scans/joins | the workload mix encoded in `benchmarks/workload-generator.py` (`MIX`) | hypothetical, P5/P8 |
| A7 | Analysts authenticate with corporate AD/LDAP (OIDC available through Keycloak/ADFS) | requirements say "identities, groups" | hypothetical, P7 |
| A8 | Time travel is a platform-team tool only | requirements | given |

## B. Technology choices (decisions, each verified against documentation - see `implementation-decisions.md`)

| # | Decision | Note |
|---|---|---|
| B1 | REST catalog = **Lakekeeper v0.14.2** (Postgres-backed) | evaluated against Polaris, Nessie, Gravitino, Iceberg reference image |
| B2 | Primary serving engine = **Trino 484**; secondary = **Kyuubi 1.12.0 on Spark 3.5.8** | both fully implemented and tested; evidence in `engine-comparison.md` |
| B3 | Iceberg runtime = **1.11.0** on the Spark side (the version Kyuubi 1.12.0 is built and tested against); Trino 484 embeds Iceberg 1.12.0 - both read/write spec v2 | verified from `pom.xml` of both projects |
| B4 | Spark runs on **Java 17** (Iceberg >= 1.11 dropped Java 11; the official Kyuubi image is Java 8, so the POC builds its own Kyuubi image on the Spark 3.5.8/Java 17 base) | verified at build time |
| B5 | Object storage image = **pgsty/silo** (MinIO fork) because `minio/minio` images were withdrawn from Docker Hub in 2025/26 and the upstream repository is archived | verified: `docker manifest inspect minio/minio:latest` fails; Docker Hub returns 404 for the repository |
| B6 | DocDB: **excluded** (P6) | revisit trigger documented |
| B7 | Semantic layer (Cube): **excluded**; BI-side result cache: **recommended** (P5) | |
| B8 | Trino is a **read-only** engine (`iceberg.security=read_only`); Spark owns every write and all maintenance | also closes a verified gap in Trino's file-based rules (`ALTER TABLE ... EXECUTE` not governed) |
| B9 | Catalog vends **no storage credentials** (`sts-enabled=false`, `remote-signing-enabled=false`); each engine brings its own storage identity | mirrors Kerberos/HDFS semantics in production |

## C. POC environment (local only)

| # | Assumption | Note |
|---|---|---|
| C1 | Developer machine: Docker Desktop with **>= 8 GB** for one engine path, **>= 11 GB** for both at once | measured here: core 1.2 GB, Trino ~1.7 GB, Kyuubi server 0.5 GB + ~0.75 GB per engine |
| C2 | Everything runs on one Docker network; no TLS except Trino (self-signed) | production differences listed per component in `security-model.md` |
| C3 | Demo passwords live in `infrastructure/.env` (git-ignored); `.env.example` holds placeholders only | |
| C4 | The POC uses `local[N]` Spark masters inside containers; YARN queue settings are carried in configuration but not enforced locally | marked `[LOCAL]` / `[PROD]` in every config file |
| C5 | Row counts are scaled down ~1,000x (1 M transactions) so a full seed takes ~2 minutes | reproducibility is verified by checksums in `tests/expected.json` |

## D. Revisions made while building (visible changes to the starting assumptions)

| # | Original | Revision | Why |
|---|---|---|---|
| D1 | "Trino can use HTTP with password auth locally" | Trino refuses passwords over HTTP by design; the POC runs **HTTPS 8443** with a generated self-signed certificate and does not publish port 8080 | observed: `401 Password not allowed for insecure authentication` |
| D2 | "Kyuubi AuthZ with Ranger if Ranger runs locally" | Ranger Admin (2.8.0 + DB + Solr) does not fit the 7.5 GB Docker budget next to two engines; the POC runs the **real Ranger plugin** inside the Kyuubi engines against a **clearly labelled Ranger Admin simulation** (`ranger-admin-stub.py`) serving real Ranger policy JSON | production config documented in `security-model.md` |
| D3 | "The authz plugin blocks metadata tables and column-less scans" | **Verified gaps** on Kyuubi 1.12.0: Iceberg metadata tables and `COUNT(*)`-style (column-less) scans are not authorized. Compensating Ranger row filters with a non-foldable contradiction (`key < 0 AND key > 0`) added so counts return 0 while column checks still run (a plain `1 = 0` is constant-folded away and turns denials into empty results - verified); metadata-table exposure documented and pinned by a strict xfail test | `tests/test_kyuubi_query_access.py` |
| D4 | "Kyuubi can verify signed session users" | `kyuubi.session.user.sign.enabled=true` breaks engine bootstrap with the AuthZ extension (`Invalid user identifier [null]`); left at default | observed |
| D6 | "HDFS is colocated on the 9 TB worker disks" (A3) | 20 TB of warehouse data cannot fit on 9 TB (60 TB raw at 3x); the 9 TB is treated as **scratch** (shuffle/spill/landing) and HDFS as a separate storage tier | `storage-and-retention.md` section 1 |
| D5 | 20 customers were both updated and deleted in the same CDC batch with equal timestamps | deletes carry a later timestamp so the merge is deterministic (50,500 customers, not 50,520) | reproducibility |
