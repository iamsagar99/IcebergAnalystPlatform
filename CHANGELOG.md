# Changelog

## 0.1.0 (2026-10-26)

First runnable version.

* Core stack: MinIO-compatible storage (pgsty/silo), Postgres 17, Lakekeeper 0.14.2, Spark 3.5.8 + Iceberg 1.11.0
  (Java 17), JupyterLab, Prometheus, Grafana.
* Trino 484 path: HTTPS + password file, file-based access control with masks and row filters, resource groups,
  read-only Iceberg connector.
* Kyuubi 1.12.0 path: JDBC authentication, GROUP/USER/CONNECTION share levels, engine pool, Kyuubi Spark AuthZ with
  Ranger policies served by a local Ranger Admin stub.
* Deterministic seed (1 M transactions, CDC merge, four gold marts), 55 pytest checks across both engines,
  benchmark generator with recorded runs at concurrency 5 and 10.
* Known gaps documented: Kyuubi AuthZ does not authorize Iceberg metadata tables or column-less scans
  (compensating row filters); Trino file rules do not govern `ALTER TABLE ... EXECUTE` (connector read-only).
