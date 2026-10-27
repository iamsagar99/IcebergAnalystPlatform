# Monitoring

Prometheus (http://localhost:9090) scrapes:

| Job | Target | Mechanism |
|---|---|---|
| trino | `trino:8080/metrics` | Trino OpenMetrics endpoint; JMX beans selected by `openmetrics.jmx-object-names` in `trino/config.properties`; authenticated as user `monitor` |
| kyuubi | `kyuubi:10019/metrics` | `kyuubi.metrics.reporters=PROMETHEUS` |
| lakekeeper | `lakekeeper:9000/metrics` | built-in metrics port |
| minio | `minio:9000/minio/v2/metrics/cluster` | `MINIO_PROMETHEUS_AUTH_TYPE=public` (LOCAL-ONLY) |

Grafana (http://localhost:3000, admin / `GRAFANA_ADMIN_PASSWORD`) auto-provisions the Prometheus datasource and the
dashboard `Lakehouse POC` (`grafana/dashboards/lakehouse-poc.json`). Metric names in the dashboard were taken from the
live `/metrics` output of the pinned versions during validation; if a panel is empty after an upgrade, check the names
with `curl -u monitor:<pw> localhost:8080/metrics | grep -i queued`.

Saturation signals to watch (docs/infrastructure-sizing.md): Trino `QueuedQueries`, spilled bytes, GC time;
Kyuubi engines started vs. idle, operations failed; Spark UI executor pending / stage duration; YARN queue wait (production only).
