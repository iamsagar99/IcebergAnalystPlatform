# Benchmark results (real runs only)

Every file here was produced by `scripts/benchmark.sh` on 2026-10-26 on the POC machine (Docker VM 7.5 GiB / 14 vCPU,
1 M-row data set, engines benchmarked one after the other, Kyuubi restarted before its run so engine warm-up is measured).

| run | engine | concurrency | ok / failed | throughput q/s | p50 s | p95 s | connect p95 s |
|---|---|---|---|---|---|---|---|
| `20261026-071140-trino-c5` | Trino 484 | 5 | 170 / 0 | 5.40 | 0.54 | 2.22 | 0.43 |
| `20261026-071214-trino-c10` | Trino 484 | 10 | 340 / 0 | 9.83 | 0.68 | 3.68 | 0.59 |
| `20261026-071339-kyuubi-c5` | Kyuubi 1.12.0 / Spark 3.5.8 | 5 | 170 / 0 | 1.55 | 1.17 | 8.03 | 37.98 (engine launch) |
| `20261026-071539-kyuubi-c10` | Kyuubi 1.12.0 / Spark 3.5.8 | 10 | 340 / 0 | 6.09 | 1.09 | 5.14 | 5.29 (warm engines) |

`*.json` holds every individual query record (worker, query, latency, rows, error); `*.md` the per-query summary.
Interpretation and caveats: `docs/engine-comparison.md`, `benchmarks/benchmark-plan.md`.
