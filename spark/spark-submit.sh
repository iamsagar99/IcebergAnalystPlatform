#!/usr/bin/env bash
# Run a job from this directory inside the running "spark" container (same Spark/Iceberg binaries as the Kyuubi engines).
#   spark/spark-submit.sh create_tables.py --reset
#   spark/spark-submit.sh maintenance.py --dry-run
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOB="${1:?usage: spark-submit.sh <job.py> [args...]}"; shift
cd "$ROOT/infrastructure"
exec docker compose exec -T spark spark-submit \
  --master "${SPARK_MASTER:-local[4]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  "/workspace/spark/${JOB}" "$@"
