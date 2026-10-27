# Storage growth, retention and tiering (P4, P9, P10)

> All numbers in this file are **hypothetical sizing** derived from the requirements' starting assumptions
> (`assumptions.md` A3-A5). The POC does not measure production growth; it demonstrates the mechanisms
> (snapshot retention properties, `spark/maintenance.py`) on a 1 M-row data set.

## 1. What the 9 TB actually is

The requirements give 20 TB of warehouse data and 9 TB of combined worker disk. 20 TB cannot live on 9 TB (and HDFS at
its default 3x replication would need ~60 TB raw), so the only consistent reading is:

* the **warehouse lives on a separate HDFS storage tier** (DataNodes with their own disks), and
* the **9 TB of worker disk is scratch**: OS, logs, YARN local dirs (shuffle, spill), Spark/Trino temp, caches.

This revises assumption A3 (recorded as D6 in `assumptions.md`). If the client confirms HDFS *is* colocated on the
9 TB, the warehouse is at least 2x over capacity today and tiering (section 5) becomes the first project, not the last.

### Usable scratch budget on the baseline cluster

| Reservation | Share | TB | Reason |
|---|---|---|---|
| OS, Hadoop/Spark/Trino binaries, logs, container images | 10% | 0.9 | NodeManager and DataNode logs grow fast; log rotation 7 days |
| Headroom (disk-full = NodeManager marks disk unhealthy at 90%) | 10% | 0.9 | `yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage` |
| YARN local dirs: ETL shuffle + spill | 45% | 4.05 | the daily ETL shuffles far more than it writes (joins, CDC merge, gold rebuilds) |
| Kyuubi engines: shuffle/spill (interactive) | 15% | 1.35 | engine `spark.local.dir` |
| Trino spill (`spiller-spill-path`) | 10% | 0.9 | spill is a safety valve, not a design point |
| Landing zone for bronze source files before ingestion | 10% | 0.9 | 250 GB/day => ~3.6 days of raw input |
| **Total** | 100% | 9.0 | |

If the 9 TB were the HDFS data disks instead: 9 TB x 0.75 usable / (250 GB/day x 3 replicas) = **9 days** of
ingestion at 3x replication, **27 days** at 1x. Either way, retention is forced, not optional.

## 2. Growth model

Assumptions (A5): 250 GB/day written; of that, ~60 GB/day is **net-new** data (transactions, events) and ~190 GB/day
is **rewrites** (CDC merges into silver rewrite touched data files; gold marts are rebuilt). Parquet/zstd already applied.

| Scenario | Retained per day | 20 TB warehouse after 30 days | after 365 days |
|---|---|---|---|
| no snapshot expiry, no compaction (every rewritten file kept) | 250 GB | 27.5 TB | 111 TB |
| snapshot expiry only (7 d silver/gold, 3 d bronze): rewrites fall out after retention | 60 GB net + ~1.3 TB rolling window | 22.2 TB (+1.3 TB window) | 43.2 TB |
| expiry + compaction + bronze tiering (section 5) | 60 GB net, bronze leaves hot tier after 30 d | 21.8 TB | ~40 TB (needs the 128/512 variant or more HDFS by Q3) |

Compaction does not reduce bytes much (Parquet is already compressed) but it keeps file counts and manifest sizes
under control; compaction tuning is out of scope, so the plan only schedules it.

## 3. Snapshot and orphan-file policy (owned by the platform team)

| Layer | `history.expire.max-snapshot-age-ms` | `history.expire.min-snapshots-to-keep` | Orphan files older than | Schedule |
|---|---|---|---|---|
| bronze | 3 days | 2 | 3 days | nightly 11:00, after ETL + validation |
| silver | 7 days | 3 | 3 days | nightly 11:00 |
| gold | 7 days | 3 | 3 days | nightly 11:00 |

Implemented as table properties in `spark/create_tables.py` and executed by `spark/maintenance.py`
(`CALL lakehouse.system.expire_snapshots(table => ...)` uses the table properties; `remove_orphan_files` with
`older_than` 3 days). Analysts cannot run either: Trino is read-only (`iceberg.security=read_only`) and the Kyuubi
Ranger policies grant no `alter`/`all`. Snapshots are **not** expired to prevent querying; they are expired for
storage and metadata hygiene. Rewrites (`rewrite_data_files`, `rewrite_manifests`) run weekly on silver facts
(out of scope beyond scheduling).

Why 7 days: it covers one weekly reporting cycle for incident replays by the platform team and bounds the
`FOR TIMESTAMP AS OF` window that remains syntactically open on Trino (see `security-model.md`).

## 4. Partitioning and file sizing (P9)

| Table | Partition | Target file | Why |
|---|---|---|---|
| `silver.transactions` | `days(transaction_ts)` | 128 MB | queries filter on date ranges; ~0.8 GB/day of data => 6-7 files per partition |
| `silver.events` | `days(event_ts)` | 128 MB | same; events are 4x the row count but narrow rows |
| `silver.customers`, `customer_pii` | none | 128 MB | 12 M rows ~ 1 GB; CDC merge rewrites touched files (copy-on-write) |
| `gold.*` | `months(date)` | 128 MB | marts are small; monthly partitions keep file counts low |
| lookups | none | - | single file |

`write.distribution-mode=hash` makes Spark cluster rows by partition before writing (fewer, larger files per
partition). Local POC files are far smaller than 128 MB because the data is 1,000x smaller.

## 5. Retention and tiering policy

| Layer | Hot (HDFS, queryable) | Warm | Cold / delete |
|---|---|---|---|
| bronze | 30 days (replay window for the CDC merge) | - | delete after 30 days; the source systems keep their own history |
| silver | current state + 7 days of snapshots | yearly partitions older than 24 months to a cheaper HDFS storage policy (`COLD`, erasure coding 1.5x instead of 3x) | legal hold decides deletion |
| gold | current + 7 days of snapshots; rebuilt from silver | - | - |

Trigger to add object storage (S3-compatible) as the warm/cold tier: HDFS above 75% or more than 24 months of
silver history; Iceberg tables can span locations, so tiering is per partition, not per table.

## 6. Saturation signals

HDFS: `dfs.datanode.du.reserved`, used % > 75%, block count growth. YARN local dirs: `LocalDirsHandler` disk
unhealthy events, shuffle fetch failures. Trino: `trino.memory` spilled bytes, `spiller` disk usage.
Metadata: snapshot count per table (> 50 means expiry is not running), manifest list size, planning time in both
engines (grows with file count: the symptom of missing compaction).
