# Resource groups and workload management (P2, P8)

The POC ships one workload model, implemented twice: Trino resource groups (`infrastructure/trino/resource-groups.json`)
and Kyuubi/Spark settings (`infrastructure/kyuubi/kyuubi-defaults.conf`, `infrastructure/spark/spark-defaults.kyuubi.conf`).
Production adds YARN queues underneath Kyuubi. Every percentage below is a **sizing assumption**; the benchmark plan
exists to replace them with measurements.

## 1. Logical workload groups

| Group | Who | Characteristics | Protection goal |
|---|---|---|---|
| `etl` | the Spark ETL (service account `etl`) | long, heavy, must finish by 10:00 | guaranteed capacity, may preempt others |
| `interactive` | analysts via BI tool / SQL client | short (< 10 s target), bursty, 40 concurrent at peak | low latency, fair share per user |
| `scheduled_reports` | report server, Airflow extracts (source `scheduled-*`) | predictable, medium | bounded concurrency, never starve interactive |
| `adhoc_heavy` | analysts who tag a query `heavy` (silver scans, big joins) | rare, large | strictly capped, queue instead of fail |
| `admin` | platform team | diagnostics, kills | always admitted |
| `bench` | benchmark identity | load generation | isolated so results don't distort other groups |

## 2. Trino implementation (`resource-groups.json`)

| Group | hardConcurrencyLimit | maxQueued | softMemoryLimit | weight | Assumption behind the number |
|---|---|---|---|---|---|
| `global` | 24 | 200 | 80% | - | leaves 20% of cluster memory for system/admin; 24 = 2 x worker count (12 on the baseline) so no single wave over-subscribes |
| `global.etl` | 4 | 20 | 40% | 4 | Trino is not the ETL engine; this group serves scheduled Trino extracts and gets priority when they run |
| `global.interactive` | 12 | 60 | 50% | 3 | 40 peak analysts, ~30% actively executing at any instant => 12 running, the rest queue briefly |
| `global.interactive.user-${USER}` | 3 | 10 | 15% | - | a BI dashboard fires 3-6 queries at once; 3 running + 10 queued per user stops one dashboard from monopolising |
| `global.scheduled_reports` | 3 | 30 | 30% | 2 | reports are embarrassingly parallel; 3 at a time finishes a nightly batch of ~100 in time |
| `global.adhoc_heavy` | 2 | 6 | 35% | 1 | a silver scan can take most of the memory pool; 2 running + 6 queued, further ones are rejected (fail fast) |
| `admin` | 6 | 20 | 90% | query_priority | always admitted |

Selectors (first match wins): admins -> `admin`; user `etl`/group `etl` -> `global.etl`; user `bench` ->
`global.bench`; source `scheduled-.*` -> reports; client tag `heavy` -> `adhoc_heavy`; groups analysts ->
per-user interactive; default -> per-user interactive.

Per-query guard-rails (config.properties): `query.max-memory=1GB`, `query.max-memory-per-node=700MB`,
`query.max-execution-time=30m`, spill enabled with a 1 GB per-query cap, `query.max-planning-time=2m`.
Users may lower (not raise) `query_max_memory` / `query_max_execution_time` through session properties (rules.json).

Limits of the mechanism (verified in the Trino docs): resource groups only *admit* queries; a running query is
not slowed or killed when its group exceeds a soft limit - only new queries queue. CPU limits are quotas over a
period, not real-time caps. Memory is enforced per query, not per group. There is no I/O or network isolation.

## 3. Kyuubi implementation

| Lever | Setting | Effect |
|---|---|---|
| Share level | `kyuubi.engine.share.level=GROUP` (default), admin `USER`, etl `CONNECTION` | analysts of one AD group share one Spark engine (one JVM, one SparkContext, warm); admins get a private engine; ETL gets a throw-away engine per connection |
| Engine pool | `___bench___.kyuubi.engine.pool.size=2`, threshold 4 | a busy group can be spread over N engines; sessions round-robin (`POLLING`) |
| Idle timeout | `kyuubi.session.engine.idle.timeout=PT10M` (prod PT30M) | idle engines release executors/driver memory |
| Queue mapping | `___analyst1___.spark.yarn.queue=interactive`, `___etl___.spark.yarn.queue=etl`, admin -> `admin` | engines land in YARN queues; the queue, not Kyuubi, enforces capacity |
| Per-engine caps | `spark.dynamicAllocation.maxExecutors=8`, `spark.driver.maxResultSize=512m`, `kyuubi.operation.result.max.rows=100000`, `kyuubi.operation.query.timeout=PT15M` | runaway queries and `SELECT *` are bounded |
| Conf hardening | `kyuubi.session.conf.restrict.list`, `kyuubi.session.conf.ignore.list` | analysts cannot change extensions, jars, memory, master |

Production YARN capacity scheduler (the actual enforcement point):

| Queue | Capacity (guaranteed) | Max | Preemption | Users |
|---|---|---|---|---|
| `etl` | 60% | 100% (off-hours) | can preempt `interactive`/`adhoc_heavy` beyond their guarantee | ETL service account |
| `interactive` | 30% | 60% | preemptable above guarantee | Kyuubi engines of analyst groups |
| `adhoc_heavy` | 5% | 30% | preemptable | heavy analysts |
| `admin` | 5% | 20% | not preempted | platform team |

Why 60/30/5/5: the ETL needs ~8 h of the whole cluster today; with 60% guaranteed and the right to grow to 100% at
night (02:00-08:00 analysts are absent) it keeps the 10:00 objective (`problem-analysis.md`, P2 quantifies it).

Limits of the mechanism: a Spark engine shared by a group has one driver; one analyst's huge result set or
broadcast can still hurt group mates (mitigated by `maxResultSize`, `autoBroadcastJoinThreshold`, result row cap).
YARN preemption kills executors, not drivers, and takes seconds to minutes. Kyuubi has no cluster-wide query
queue comparable to Trino resource groups: queueing happens inside Spark's FIFO/FAIR scheduler per engine.

## 4. ETL protection and the completion-time objective (P2)

Objective: gold refreshed by **10:00** every day (SLO 99% of days/month). ETL runs 02:00-10:00 uncontended.

| Analyst load taken from the ETL share | ETL slowdown (linear model) | Finish time |
|---|---|---|
| 0% (separate cluster or night window) | 8.0 h | 10:00 |
| 15% (interactive queue at its 30% guarantee, half used during the overlap 08:00-10:00) | +0.3 h | 10:20 |
| 40% (today: unmanaged analyst Spark sessions from 08:00) | +1.6 h | 11:40 |
| 60% (dashboards at 09:00 + a heavy ad-hoc scan) | +3.0 h | 13:00 |

The architecture answer: (1) time-window scheduling moves most of the ETL out of the overlap, (2) YARN guarantees
give ETL 60% during the overlap, (3) Trino serves the interactive load from its *own* memory budget and only
contends for HDFS I/O, (4) the platform alerts when stage durations of the ETL exceed the trailing-7-day p95 by
25% (`ETL stage duration` metric), and (5) `adhoc_heavy` is capped at 2 concurrent so one scan cannot blow the window.

## 5. Saturation metrics

| Signal | Where | Meaning |
|---|---|---|
| `QueuedQueries` > 0 for minutes | Trino `/metrics` (dashboard panel 1) | admission limit reached: add workers or lower concurrency |
| spilled bytes, `query.max-memory` failures | Trino | memory per query too small or queries too big |
| GC time / heap used | Trino, Spark driver | heap too small for the concurrency |
| executor pending / `kyuubi_engine_total` rising | Spark UI, Kyuubi metrics | YARN queue full; engines waiting |
| YARN queue wait, pending containers | ResourceManager | capacity, not Kyuubi, is the bottleneck |
| ETL stage duration vs. trailing p95 | Spark History / Airflow | ETL window at risk (P2) |
