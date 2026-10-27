# 0001. Lakekeeper as the Iceberg REST catalog

Date: 2026-10-26. Status: accepted.

## Context

Spark (ETL), Trino and Kyuubi must share one catalog. Candidates: Apache Polaris, Lakekeeper, Project Nessie,
Apache Gravitino, the Iceberg reference REST image. The POC must survive a stop/restart with its metadata intact and
must let the catalog refuse to hand out storage credentials.

## Decision

Lakekeeper 0.14.2 backed by Postgres 17.

## Consequences

* Smallest footprint of the candidates (~100 MB RSS), two API calls to bootstrap, persistent across restarts.
* Per-warehouse `sts-enabled` / `remote-signing-enabled` switches give exactly the "no vending" posture we want.
* No HDFS storage profile: production needs an S3-compatible gateway in front of HDFS or a different catalog.
  This is the largest open production decision (docs/final-report.md, production-readiness gaps).
* Governance is not Apache; revisit if that matters to the client.

Evidence: docs/problem-analysis.md P3, docs/implementation-decisions.md.
