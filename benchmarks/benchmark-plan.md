# Benchmark plan: establishing an evidence-based capacity number

Goal: replace the assumption "the baseline cluster supports 40 concurrent analysts" with a measurement, tied to the
ETL completion objective (gold by 10:00), for both serving paths on identical data and identical queries.

## 1. Workload mix (from P9)

The generator (`workload-generator.py`, `MIX`) weights the representative queries as an interactive session would
issue them: 80% gold-mart queries (q1, q2, q4, q7, q8, q6 - point look-ups and aggregates), 20% silver queries
(q3 fact x dimension join, q5 event scan). Each worker is one analyst identity (`bench`, separate resource group /
engine pool so the run is isolated from other users), runs the mix `--iterations` times, and records per-query
latency, row count and errors. Connection time (engine warm-up) is recorded separately.

## 2. Concurrency ramp and runs

| Step | Concurrency | Iterations | Purpose |
|---|---|---|---|
| 1 | 1 | 3 | baseline latency per query, no contention |
| 2 | 5 | 2 | light load (POC runs this) |
| 3 | 10 | 2 | moderate load (POC runs this) |
| 4 | 20 | 2 | half of the assumed peak |
| 5 | 40 | 2 | the assumed peak: pass/fail point |
| 6 | 40 + ETL | 2 | run step 5 while the nightly ETL (or a representative merge + gold rebuild) executes |

Each step runs against Trino and then against Kyuubi (never both at once), three repetitions, discard the first
(cache/warm-up), report median of the remaining two. `scripts/benchmark.sh --engine trino --concurrency "1 5 10 20 40"`.

## 3. Metrics to capture

Per run: wall time, throughput (queries/s), latency p50/p95/max per query class, error count and types,
connect/warm-up time. Engine side: Trino `QueuedQueries`, memory pool reserved bytes, spilled bytes, GC time;
Kyuubi engines started, executor pending, Spark stage durations; YARN queue wait (prod). System: CPU, memory,
disk I/O on workers; HDFS/S3 request rate. ETL side (step 6): stage durations and total runtime vs. an uncontended run.

## 4. Pass / fail criteria

| Criterion | Pass |
|---|---|
| interactive latency | p95 of gold-mart queries <= 5 s and of silver queries <= 30 s at concurrency 40 |
| errors | 0 failed queries at concurrency <= 20; <= 1% at 40 (queued, not failed, is acceptable) |
| queueing | median queue wait <= 2 s at 40 |
| ETL objective (step 6) | ETL runtime with concurrent analyst load <= 1.1 x uncontended runtime **and** finishes before 10:00 |
| isolation | one deliberately runaway query (`SELECT * FROM silver.transactions ORDER BY amount`) is killed by limits without failing other sessions |

If step 5 fails but step 4 passes, the evidence-based number is 20 and the roadmap says what to change (memory per
node, workers, resource-group limits) before re-running.

## 5. Scaling POC results to production (caveats)

The POC data is ~1,000x smaller, runs single-node, and both engines are memory-starved on a laptop. Results scale
qualitatively (relative latency, warm-up cost, failure modes, admission behaviour), not quantitatively. To scale:
run the same generator from an edge node against a staging deployment of the chosen engine on 2-3 production
workers with a 1/4 sample of silver (date-bounded) and multiply concurrency by the worker ratio; keep the ETL step
(6) because it is the only test that answers P2. Treat any number from the laptop as a smoke test.

## 6. Results

Real runs only, in `results/<timestamp>-<engine>-c<N>.{json,md}`; the summary table is in
`docs/engine-comparison.md`. The POC ran concurrency 5 and 10 on both engines (steps 2-3); steps 1, 4-6 are
documented for the staging environment.
