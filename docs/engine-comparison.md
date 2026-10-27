# Engine comparison: Trino vs. Apache Kyuubi (Spark SQL)

Both paths run in this repository against the same catalog, storage, tables, users, policies and queries.
Columns marked **[verified]** come from documentation or from running the pinned versions; **[measured]** rows are
the real POC runs in `benchmarks/results/` (laptop, Docker VM 7.5 GiB, 1 M-row data set, one engine at a time).

## Side by side

| Dimension | Trino 484 | Kyuubi 1.12.0 + Spark 3.5.8 |
|---|---|---|
| Concurrency model [verified] | one always-on cluster; every query split into tasks across workers; resource groups admit/queue per group and per user | gateway + engines per group/user (share level), engine pools; inside an engine Spark's FAIR scheduler; YARN queues outside |
| Latency / warm-up [measured] | no warm-up; connect p95 0.4-0.6 s; overall p50 0.54-0.68 s at 5-10 concurrent | first session of a group pays the engine launch; subsequent sessions reuse the warm engine (see table below) |
| Fault isolation [verified] | query-level; coordinator SPOF (Trino Gateway for HA) | engine-level (one group's OOM kills only that engine); server HA via ZooKeeper |
| Authorization [verified] | file rules (POC) / Ranger / OPA; table, column mask, row filter; metadata tables governed by name rules; `ALTER TABLE EXECUTE` needs connector `read_only` or Ranger/OPA | Ranger via Kyuubi AuthZ: table, column, mask, row filter; **gaps**: metadata tables and column-less scans (compensating row filters), see `security-model.md` |
| Resource control [verified] | resource groups (concurrency, queue, soft memory, weights), per-query memory/time caps, spill | share level, pool size, idle timeout, YARN queue per user, executor caps, result/row/time caps; no cluster-wide query queue |
| SQL dialect vs. the ETL [verified] | ANSI SQL; ETL views/UDFs must be ported (`sql/kyuubi/dialect-notes.md`) | identical to the ETL's Spark SQL |
| Operations [verified] | new engine for the team; simple config; coordinator + workers; separate memory budget | Spark-familiar; more parts (server, ZooKeeper, engines on YARN, Ranger); engine lifecycle to tune |
| Writes / maintenance | read-only by design here | engines are capable of writes; analysts denied by policy |
| Footprint in the POC [measured] | ~1.4-1.7 GB RSS (2 GB heap) | server 0.5 GB + ~0.7-0.85 GB per engine |

## Measured POC results

Workload: `sql/sample_queries.sql` mix (80% gold marts, 20% silver), user `bench`, 2 iterations per worker,
`scripts/benchmark.sh --engine <e> --concurrency "5 10"`. Raw files: `benchmarks/results/20261026-*`.

| engine | concurrency | queries ok / failed | wall s | throughput q/s | p50 s | p95 s | max s | connect p95 s (engine warm-up for Kyuubi) | run file |
|---|---|---|---|---|---|---|---|---|---|
| trino | 5 | 170 / 0 | 31.46 | 5.404 | 0.54 | 2.22 | 5.01 | 0.43 | `20261026-071140-trino-c5` |
| trino | 10 | 340 / 0 | 34.59 | 9.83 | 0.68 | 3.68 | 5.38 | 0.59 | `20261026-071214-trino-c10` |
| kyuubi | 5 | 170 / 0 | 109.74 | 1.549 | 1.17 | 8.03 | 10.78 | 37.98 | `20261026-071339-kyuubi-c5` |
| kyuubi | 10 | 340 / 0 | 55.81 | 6.093 | 1.09 | 5.14 | 7.52 | 5.29 | `20261026-071539-kyuubi-c10` |

Per-query p50 / p95 seconds at concurrency 10:

| query | Trino p50 | Trino p95 | Kyuubi p50 | Kyuubi p95 |
|---|---|---|---|---|
| q1_daily_revenue_by_region_gold | 0.71 | 0.94 | 1.22 | 2.12 |
| q2_top_categories_month_gold | 0.62 | 0.94 | 0.82 | 2.46 |
| q4_funnel_conversion_gold | 1.00 | 1.93 | 1.27 | 2.22 |
| q7_customer_monthly_top_spenders_gold | 0.52 | 0.88 | 0.66 | 1.27 |
| q8_store_lookup_point_query | 1.08 | 1.77 | 1.15 | 2.34 |
| q6_current_state_customer_after_cdc | 0.39 | 0.60 | 0.72 | 2.19 |
| q3_fact_join_dimension_silver | 4.70 | 5.03 | 5.47 | 6.33 |
| q5_events_last_7_days_silver | 1.20 | 1.56 | 2.57 | 4.02 |

Reading the numbers (laptop caveats in `benchmarks/benchmark-plan.md`):

* **Warm-up is the dominant Kyuubi cost.** At concurrency 5 the `bench` user's first sessions had to launch the
  2-engine pool: connect p95 **38.0 s** and wall time 109.7 s, versus Trino's connect p95 **0.4-0.6 s**. At
  concurrency 10 the engines were already warm (connect p95 5.3 s = session set-up on a busy engine) and
  throughput rose to 6.1 q/s. This is exactly the behaviour `kyuubi.session.engine.idle.timeout` trades against
  memory: keep engines alive and the warm-up disappears; let them expire and the next analyst waits.
* **Steady-state latency favours Trino by ~1.6-2x on this hardware**: p50 0.68 s vs 1.09 s and p95 3.7 s vs 5.1 s at
  concurrency 10; throughput 9.8 vs 6.1 q/s. Gold-mart point queries are sub-second on both.
* **The silver join (q3) is the heavy class on both** (Trino p95 5.0 s, Kyuubi 6.3 s): with a 50,500-row dimension the
  POC does not stress either engine's join strategy; the production dimension (12 M rows) will, which is why the
  staging benchmark (plan steps 4-6) decides the final concurrency number.
* **Zero failures on both** at 5 and 10 concurrent sessions; Trino queued nothing (`QueuedQueries` stayed 0, the
  `global.bench` group allows 40); Kyuubi served 10 sessions over 2 shared engines (5 sessions per Spark driver).
* **Both engines are memory-starved here** (2 GB Trino heap, 768 MB Spark drivers in `local[2]`), so absolute numbers
  are smoke-test values; the *shape* (warm-up, steady-state ratio, zero errors, heavy-query class) is what scales.

## Recommendation and when the other engine is better

**Trino is the primary engine** for the analyst/BI workload: no warm-up, admission control that queues instead of
failing, authorization that covers metadata tables by name, and a memory budget that does not sit inside YARN
next to the ETL.

**Kyuubi is the better choice when:**
* analysts must run Spark-SQL-dialect code (ETL views, UDFs, notebooks) unchanged;
* the organisation standardises on one engine technology (Spark) and YARN-only operations;
* the workload is few, long, heavy queries (big joins over silver) rather than many short ones - Spark's shuffle and
  spill handling with dynamic allocation scales those better on the 512 GB variant;
* the client already runs Ranger for Hive/Spark and wants a single plugin family.

Conditions that would flip the decision back to Kyuubi during the staging benchmark: Trino's reserved memory
pushes the ETL past 10:00 (benchmark step 6) with no room to add workers, or dialect porting of existing
analyst assets exceeds the budget.
