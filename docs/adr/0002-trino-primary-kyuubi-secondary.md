# 0002. Trino is the primary serving engine; Kyuubi is shipped as the secondary path

Date: 2026-10-26. Status: accepted.

## Context

100 analysts (40 concurrent) need interactive SQL without private Spark sessions. Three options were evaluated:
Trino, Spark Thrift Server and Apache Kyuubi. The decision had to rest on runs, not on documentation.

## Decision

Trino 484 is the analyst-facing engine. Kyuubi 1.12.0 on Spark 3.5.8 is implemented end to end (auth, authz,
resource management, tests, benchmarks) and kept as the alternative path.

## Consequences

* Measured at concurrency 10 on the POC machine: Trino p50 0.68 s / 9.8 q/s, Kyuubi p50 1.09 s / 6.1 q/s, with
  Kyuubi paying 38 s of engine warm-up when engines are cold.
* Trino's memory budget sits outside YARN, which simplifies ETL protection (P2).
* Trino SQL differs from the ETL's Spark SQL; existing views and UDFs are not reusable there.
* Kyuubi wins if dialect compatibility or a single-engine (Spark/YARN) operations model matters more than latency.

Evidence: docs/engine-comparison.md, benchmarks/results/.
