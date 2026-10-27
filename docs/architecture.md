# Architecture

One end-to-end architecture, drawn as the repository deploys it. Solid lines are the primary path (Trino);
the Kyuubi path is the alternative serving path and is drawn as such. The Mermaid diagram is the deployed POC;
production substitutions (HDFS, YARN, Ranger Admin, LDAP/OIDC) are listed per component below.

```mermaid
flowchart LR
  subgraph SRC["Source systems (hypothetical)"]
    POS["POS / order systems"]
    WEB["Web & app clickstream"]
    CRM["Customer master (CDC: I/U/D)"]
  end

  subgraph ETL["ETL (Spark 3.5.8 + Iceberg 1.11.0, service account etl)"]
    GEN["generate_data.py -> bronze"]
    MERGE["seed_warehouse.py: bronze -> silver MERGE (CDC) -> gold"]
    MAINT["maintenance.py: expire_snapshots / remove_orphan_files (nightly)"]
    NB["JupyterLab (platform team only)"]
  end

  subgraph STORE["Object storage (MinIO-compatible: pgsty/silo) - PROD: HDFS"]
    B["bronze/*"]
    S["silver/* (incl. customer_pii)"]
    G["gold/* marts"]
  end

  CAT["Iceberg REST catalog: Lakekeeper 0.14.2 (Postgres) - no credential vending"]

  subgraph TRINO["Primary query engine: Trino 484 (HTTPS 8443)"]
    TC["coordinator+worker, Iceberg connector read-only"]
    TAC["file-based access control: rules.json (PROD: Ranger/OPA)"]
    TRG["resource groups: resource-groups.json"]
    TPW["password file + group file (PROD: LDAP/OIDC)"]
  end

  subgraph KYUUBI["Alternative path: Kyuubi 1.12.0 (Thrift 10009 / REST 10099)"]
    KS["Kyuubi server: JDBC auth (Postgres) - PROD: LDAP/Kerberos"]
    E1["Spark engine: group analysts"]
    E2["Spark engine: group restricted_analysts"]
    E3["Spark engine: user admin"]
    RP["Kyuubi Spark AuthZ (Ranger plugin) in every engine"]
    RA["ranger-admin-stub (SIMULATION) - PROD: Ranger Admin 2.8"]
  end

  subgraph ANALYSTS["Analysts (100, 40 peak)"]
    CLI["Trino CLI / JDBC / BI tool"]
    BEE["Beeline / JDBC (hive2) / PyHive"]
  end

  subgraph MON["Monitoring"]
    PROM["Prometheus"]
    GRAF["Grafana"]
  end

  POS --> GEN
  WEB --> GEN
  CRM --> GEN
  GEN --> B
  B --> MERGE --> S --> MERGE --> G
  MAINT --> STORE
  ETL <-->|commit / load table| CAT
  NB <--> CAT
  CLI ==>|HTTPS + password| TC
  TC <-->|load table (REST)| CAT
  TC ==>|engine identity| STORE
  TAC -.- TC
  TRG -.- TC
  TPW -.- TC
  BEE -->|PLAIN SASL| KS
  KS --> E1
  KS --> E2
  KS --> E3
  E1 & E2 & E3 <-->|load table (REST)| CAT
  E1 & E2 & E3 -->|engine identity| STORE
  RP -.->|policies (poll)| RA
  TC -.->|/metrics| PROM
  KS -.->|/metrics| PROM
  CAT -.->|/metrics| PROM
  STORE -.->|/metrics| PROM
  PROM --> GRAF
```

DocDB is absent by decision (P6). Optional components (Grafana/Prometheus, Jupyter) are not production
dependencies: the engines run without them.

## Component responsibilities

| Component | Responsibility | POC implementation | Production |
|---|---|---|---|
| Source systems | emit transactions, events, CDC records | deterministic generator | Kafka/Debezium, file drops |
| Spark ETL | owns table creation, writes, CDC merge, gold rebuilds, maintenance | `spark/*.py` in the `spark` container, `local[4]` | YARN queue `etl`, Airflow-scheduled |
| Object storage | data + metadata files | pgsty/silo (MinIO lineage), bucket `lakehouse` | HDFS (`HadoopFileIO`, Kerberos) |
| REST catalog | namespaces, table pointers, warehouse storage profile, commits | Lakekeeper + Postgres, no auth, no vending | Lakekeeper with OpenID + OpenFGA, HA behind a load balancer, Postgres HA |
| Trino | interactive SQL, admission control, authorization, read-only | single node | 1 coordinator + N workers, Trino Gateway for HA |
| Kyuubi | multi-tenant Spark SQL gateway; engines per group/user | server + local-mode engines in one container | server HA (ZooKeeper) + engines on YARN |
| Ranger | policy store for the Kyuubi/Spark path (and Trino in prod) | stub serving `policies.json` | Ranger Admin 2.8 + usersync from AD |
| Identity | users and groups | password file / Postgres table / static group map | AD/LDAP (+ OIDC) |
| Monitoring | metrics, dashboards | Prometheus + Grafana | same, plus alerting |

## Data flow

1. Sources land in **bronze** (append-only, raw, with `ingest_batch_id`).
2. The ETL merges bronze into **silver**: de-duplicated facts (`MERGE ... WHEN NOT MATCHED`), CDC-merged dimensions
   (`WHEN MATCHED AND op='D' THEN DELETE`), PII split into `silver.customer_pii`.
3. **gold** marts are rebuilt (`INSERT OVERWRITE`) from silver.
4. Analysts read gold (mostly) and silver (sometimes) through Trino or Kyuubi; never bronze.
5. Nightly maintenance expires snapshots and removes orphan files; analysts always read the current snapshot.

## Query execution flow

**Trino.** Client -> HTTPS 8443 with username/password -> password authenticator (file / LDAP) -> group provider
resolves groups -> system access control evaluates `rules.json` (catalog, table, columns, masks, filters) ->
resource-group selector admits or queues the query -> coordinator loads table metadata from the REST catalog ->
plans splits over Parquet files -> workers read files from storage with the engine identity -> results stream
back; `/metrics` exposes counters.

**Kyuubi.** Client -> Thrift 10009 (PLAIN SASL) -> JDBC authentication against Postgres -> session is created
for user U with primary group G -> share level decides the engine key (`GROUP` -> G; admin `USER`) -> if no
engine is registered for that key, the server launches `spark-submit` (proxy-user G) with
`spark-defaults.kyuubi.conf` and waits for it to register (embedded ZooKeeper locally, external in prod) ->
statements are forwarded to the engine; the engine sets the session user as a Spark local property; the AuthZ
extension injects row filters/masks at analysis and checks privileges against Ranger policies at optimization ->
Spark plans the Iceberg scan via the REST catalog and reads storage with the engine identity -> results return
through the server. Idle engines exit after `PT10M`.

## Catalog metadata flow

Spark commits: write data files -> write new metadata.json -> `POST /v1/{prefix}/namespaces/{ns}/tables/{t}` (commit
with requirements) -> catalog swaps the pointer atomically. Readers (Trino, Kyuubi engines, notebook) call
`GET /v1/{prefix}/namespaces/{ns}/tables/{t}` and receive the current metadata location + config (endpoint,
path-style) but **no credentials**. Snapshot history stays in metadata.json; expiry rewrites it.

## Authentication and authorization flow

See `security-model.md`. In short: identity at the engine front door (Trino password/LDAP/OIDC; Kyuubi
JDBC/LDAP/Kerberos), groups from one source, authorization at the engine (file rules / Ranger plugin), storage
and catalog reachable only with engine identities.

## Resource-management flow

Trino: selector -> resource group -> admission (concurrency, queue) -> per-query memory/time caps -> spill.
Kyuubi: user defaults -> share level/pool/queue -> engine on YARN queue -> Spark dynamic allocation within
queue limits -> per-operation caps. Details and numbers: `resource-management.md`.

## Failure scenarios and recovery

| Failure | Effect | Recovery / design |
|---|---|---|
| Catalog down | new queries fail to load tables (both engines); running queries continue; ETL commits fail and retry | Lakekeeper is stateless over Postgres: run 2+ replicas behind a load balancer; Postgres HA; engines retry (`iceberg.rest-catalog.max-retries`) |
| Storage down | all reads fail; ETL fails | HDFS HA (NameNode pair), DataNode replication; Trino/Spark retry at split level; alert |
| Trino coordinator down | all Trino queries fail; Kyuubi path unaffected | restart (stateless); production: standby coordinator behind Trino Gateway; clients retry |
| Kyuubi server down | new sessions fail; running engines keep running and are re-discovered via ZooKeeper when the server returns | 2+ servers registered in ZooKeeper, clients use the ZK connection string |
| One Spark engine OOMs | only that group's sessions fail; others unaffected | server relaunches on next session; `maxResultSize`, broadcast thresholds and row caps reduce the chance |
| Runaway query | Trino: capped by `query.max-memory`, execution time, spill; Kyuubi: `kyuubi.operation.query.timeout`, result caps | admins can kill from the UI/REST on both |
| ETL overrun | gold stale | YARN guarantee + preemption, stage-duration alert, `adhoc_heavy` cap (P2) |
| Policy file / Ranger unreachable | Trino keeps last loaded rules; Ranger plugin keeps last policy cache (`policycache/`) and keeps enforcing | plugin logs refresh failures; alert on `PolicyRefresher` errors |

## Network and trust boundaries

* **Analyst zone**: may reach only Trino 8443 (TLS) and Kyuubi 10009/10099. Nothing else is routable.
* **Engine zone**: Trino, Kyuubi server + engines, Spark ETL. Holds storage identities (keys/keytabs) and catalog
  principals. Reaches catalog, storage, Postgres (Kyuubi auth), Ranger.
* **Data zone**: storage, catalog, Postgres, Ranger. No analyst route. Locally this is the Docker network
  `iap_lakehouse`; only the ports listed in the README are published to the host, and those are for the demo.
* Secrets: `.env` (local only), production: Vault/Kubernetes secrets, keytabs on engine hosts only.

## Deployment topology

POC: one Docker host, Compose project `iap`, core + two overlays. Production (baseline cluster): 12 worker
nodes (8 cores / ~10.7 GB RAM each on average), YARN for ETL + Kyuubi engines, Trino co-located on the same
nodes with a reserved memory budget (`infrastructure-sizing.md`), HDFS DataNodes on dedicated storage nodes,
catalog + Postgres + Ranger + ZooKeeper on 3 small service nodes.

**Where configuration lives and how analysts connect.**

| | Trino | Kyuubi |
|---|---|---|
| Config | `infrastructure/trino/*` mounted at `/etc/trino` | `infrastructure/kyuubi/*` at `/opt/kyuubi/conf`; engine Spark config `infrastructure/spark/spark-defaults.kyuubi.conf` at `/opt/spark/conf/spark-defaults.conf`; Ranger plugin XML next to it |
| Host / port / protocol | `localhost:8443`, HTTPS (self-signed), HTTP basic/password | `localhost:10009`, Thrift binary (hive2), PLAIN SASL; REST/UI `localhost:10099` |
| Clients | Trino CLI (`--insecure`), JDBC `jdbc:trino://localhost:8443?SSL=true&SSLVerification=NONE`, Python `trino`, DBeaver, Power BI/Tableau connectors | `kyuubi-beeline -u jdbc:hive2://localhost:10009/gold -n analyst1 -p ...`, Hive JDBC driver, PyHive, DBeaver (Hive), BI tools with Hive/Spark connectors |

## Extension point for a DocDB (not implemented)

If P6's trigger fires: a Spark structured-streaming or incremental job reads changed rows from an Iceberg table
between snapshots (`start-snapshot-id`) and upserts into the document store; masking is applied in that job with the
same Ranger policies (Spark AuthZ) so the copy never contains unmasked PII for the serving role.
