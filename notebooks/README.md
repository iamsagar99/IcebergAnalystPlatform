# Notebooks (manual verification with a SparkSession)

Open http://localhost:8888/?token=<JUPYTER_TOKEN from infrastructure/.env> (default token `lakehouse`) after
`scripts/start.sh`. The kernel runs inside the `spark` container, so `SparkSession.builder.getOrCreate()` picks up
`infrastructure/spark/spark-defaults.conf` (catalog `lakehouse`, Iceberg extensions, S3 settings) automatically.

* `explore_warehouse.ipynb` - browse namespaces/tables, inspect snapshots and files, time travel, pandas hand-off.

This path runs as the **ETL / platform identity**; it is a maintenance and verification tool, not an analyst endpoint.
