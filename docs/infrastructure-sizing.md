# Infrastructure and capacity plan

> **Hypothetical sizing.** Everything here derives from the requirements' cluster figures and the assumptions in
> `assumptions.md`. The POC measures a 1 M-row data set on a laptop (`benchmarks/results/`); it does **not** prove that
> either engine handles 40 concurrent analysts on the baseline cluster. `benchmarks/benchmark-plan.md` describes
> how to establish that number.

## 1. Layers

| Layer | Components | Dedicated / shared |
|---|---|---|
| ETL compute | Spark on YARN, queue `etl` | shared nodes, dedicated YARN capacity (guarantee 60%, preempts) |
| Analytical compute, Trino path | Trino coordinator + workers | dedicated memory budget reserved outside YARN on the same nodes |
| Analytical compute, Kyuubi path | Kyuubi server(s) + Spark engines, queue `interactive` / `adhoc_heavy` | shared YARN capacity (30% + 5%) |
| Object storage | HDFS DataNodes (prod) / MinIO-compatible (POC) | dedicated storage nodes (see `storage-and-retention.md` section 1) |
| Catalog and metadata | Lakekeeper x2, Postgres (HA pair), ZooKeeper x3 (Kyuubi HA), Ranger Admin | 3 small service nodes (4 cores / 16 GB each) outside the worker pool |
| Optional serving | BI result cache (in the BI tool) | none on the cluster |
| Monitoring | Prometheus, Grafana, Spark History Server | 1 service node |

Only **one** analytical path runs in production; the other's budget returns to YARN. Both are sized here so the
comparison in `engine-comparison.md` is like for like.

## 2. Baseline cluster: 96 cores / 128 GB / 9 TB (12 workers of 8 cores, 10.7 GB)

RAM per core is 1.33 GB; every allocation below is memory-first and treats CPU as plentiful.

### 2a. Per-node budget (10.7 GB)

| Reservation | GB | Note |
|---|---|---|
| OS, DataNode/NodeManager daemons, page cache floor | 1.7 | non-negotiable |
| YARN NodeManager capacity (`yarn.nodemanager.resource.memory-mb`) | 6.0 | ETL + Kyuubi engines |
| Trino worker JVM (Trino path only) | 3.0 | `-Xmx3G`; on the Kyuubi path this 3 GB goes to YARN (9 GB) |

Cluster totals: YARN 72 GB (Trino path) or 108 GB (Kyuubi path); Trino 36 GB heap across 12 workers; 1 of the 12
nodes also hosts the coordinator (`node-scheduler.include-coordinator=false` in prod, coordinator JVM 3 GB taken
from that node's YARN share).

### 2b. ETL compute (YARN queue `etl`, 60% guaranteed = 43 GB of 72 GB; up to 100% at night)

Spark executor = 2 cores, 4 GB heap + 1 GB overhead (5 GB container) -> 8 executors at the guarantee (16 cores),
14 executors (28 cores) at night. Driver 4 GB. Why 2-core executors: at 1.33 GB/core, a 4-core executor would get
only ~5 GB and spill constantly on the CDC merge and gold aggregations; 2 cores x 2.5 GB/core is the smallest
executor that keeps shuffle pages in memory. Assumption to validate: today's 8-hour run uses roughly the whole
cluster; with 14 executors overnight and 8 during the 08:00-10:00 overlap the run finishes within the window
(`resource-management.md` section 4).

### 2c. Trino path (36 GB total heap, 12 workers x 3 GB; coordinator 3 GB)

| Setting | Value | Derivation |
|---|---|---|
| `query.max-memory-per-node` | 900 MB | 30% of a 3 GB worker heap (Trino's headroom default is 30%; the rest is reserved for system + other queries) |
| `query.max-memory` | 8 GB | 12 x 900 MB, rounded down: the largest single query may use ~22% of the cluster memory |
| `memory.heap-headroom-per-node` | 900 MB | 30% |
| general pool per node | ~2.1 GB | heap - headroom |
| concurrency sizing | 12 running interactive queries, each <= 900 MB/node | 12 x 900 MB = 10.8 GB > 2.1 GB general pool per node: **the memory pool, not the concurrency limit, is the ceiling** |

How concurrency drives memory: a gold-mart aggregation on 54,000 rows needs a few MB per node; the silver join
(q3) with a 50,500-row broadcast dimension needs ~100-300 MB per node on the POC data and scales with the dimension
(12 M rows -> ~1-2 GB broadcast side, above the per-node limit -> Trino switches to a partitioned join, which is
slower but fits). The `interactive` group at 12 concurrent assumes <= 170 MB/node per query on average (2.1 GB /
12); heavier queries queue (`QueuedQueries` metric) or spill. Spill: `spiller-spill-path` on the YARN local disks,
`max-spill-per-node=100GB` out of the 0.9 TB budget (`storage-and-retention.md`), `query-max-spill-per-node=20GB`.
Spill protects the ETL by failing fast rather than consuming disk the ETL needs.

### 2d. Kyuubi path (YARN queue `interactive`, 30% of 108 GB = 32 GB; `adhoc_heavy` 5% = 5 GB)

Engines = groups x pool size. With groups `analysts`, `restricted_analysts` and a pool of 2 for the busy group:
3 engines. Each engine: driver 2 GB (client mode on an edge node or AM in cluster mode) + executors 2 cores /
4 GB + 1 GB overhead, `minExecutors=1`, `maxExecutors=8`, idle timeout 2 min for executors, 30 min for the engine.

| Engines | Executors each (peak) | Memory at peak | Fits 32 GB? |
|---|---|---|---|
| 3 | 2 | 3 x (2 + 2 x 5) = 36 GB | no - peak is capped by the queue to ~6 executors in total |
| 3 | 1 | 3 x (2 + 5) = 21 GB | yes, with 11 GB for bursts |

Engines x executors x executor memory is the Kyuubi sizing identity; the YARN queue, not Kyuubi, stops it from
exceeding 32 GB. 40 concurrent analysts on 3 engines means ~13 sessions per Spark application sharing one
driver and 2-6 executors: fine for gold-mart reads, saturating for silver scans (one scan can occupy all executor
slots; others wait in the Spark FAIR scheduler). That is the structural reason Trino is primary (P1).

### 2e. Disk for shuffle, spill and temp (see `storage-and-retention.md` section 1)

Of 9 TB: 4.05 TB YARN local dirs (ETL), 1.35 TB Kyuubi engine shuffle, 0.9 TB Trino spill, 0.9 TB landing,
1.8 TB OS/logs/headroom. Alert at 75% utilisation of any local dir.

### 2f. Saturation metrics (what tells you this plan is wrong)

| Metric | Source | Threshold (assumption) |
|---|---|---|
| queued queries (`QueuedQueries`, resource group queued) | Trino | > 5 for > 2 min during business hours |
| spilled bytes / spill failures | Trino | any sustained spill on gold-mart queries |
| GC time share | Trino, Spark driver | > 10% |
| executor pending / YARN pending containers | Spark UI, ResourceManager | pending > 0 for > 1 min in `interactive` |
| ETL stage duration vs. trailing p95 | Spark History / Airflow | +25% |
| YARN queue wait time | ResourceManager | > 60 s in `interactive` |
| memory pool exhausted errors | Trino | any |

## 3. Variant: 128 vCPU / 512 GB / 8 TB (16 workers of 8 vCPU, 32 GB)

RAM per core becomes 4 GB: memory stops being the binding constraint and the architecture, not the hardware, decides.

| Allocation | Baseline (96/128) | Variant (128/512) | What changes |
|---|---|---|---|
| per-node budget | OS 1.7 / YARN 6 / Trino 3 GB | OS 2 / YARN 18 / Trino 12 GB | Trino heap 192 GB total; YARN 288 GB |
| ETL executors (queue `etl`, 60%) | 8 (guarantee) - 14 (night), 2 cores / 4 GB | 12 - 24, 4 cores / 12 GB | fewer spills, ETL finishes faster (fewer, larger executors) |
| Trino `query.max-memory-per-node` / `query.max-memory` | 900 MB / 8 GB | 3.6 GB / 50 GB | q3-style joins broadcast comfortably; `interactive` concurrency can be raised to 24 with the same per-query share |
| Kyuubi engines (queue `interactive`, 30% = 86 GB) | 3 engines, 1-2 executors | 3-6 engines (pools of 2 per group), 4 executors of 12 GB each | 40 concurrent analysts across 6 engines = ~7 sessions per driver |
| disk (8 TB) | scratch only | scratch only, 1 TB less: shrink the landing zone and rely on tiering | growth plan unchanged (warehouse is on HDFS) |
| monitoring/catalog | separate service nodes | same | unchanged |

Conclusion of the two variants: on the baseline, memory forces low concurrency per query and makes the
engine choice (warm Trino cluster with admission control vs. shared Spark drivers) the main lever; on the variant,
both engines have room, and the decision rests on latency, isolation and operations (`engine-comparison.md`).
Hardware removes the symptom; architecture removes the cause (unmanaged per-analyst Spark sessions and no
admission control).

## 4. POC machine (what was actually measured here)

Docker Desktop VM with 7.5 GiB, 14 vCPU. Observed resident memory: core services ~1.2 GB, Trino ~1.7 GB (2 GB heap),
Kyuubi server ~0.5 GB, each Spark engine ~0.7 GB (`-Xmx768m` driver, `local[2]`), Spark ETL job ~1.5 GB while seeding.
Both engine paths plus three Kyuubi engines fit, with ~1 GB spare; the benchmark runs engines one at a time to keep
results comparable.
