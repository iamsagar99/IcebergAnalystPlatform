#!/usr/bin/env bash
# Start the platform.   scripts/start.sh --engine trino|kyuubi|both|core   [--no-build]
# core  = storage + postgres + catalog + spark/jupyter + monitoring (always started)
# Memory: core ≈ 2.5 GB, Trino ≈ 2.5 GB, Kyuubi server + 2 engines ≈ 3 GB. Give Docker ≥ 8 GB for one engine, ≥ 11 GB for both.
# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
ENGINE=both; BUILD=1
while [[ $# -gt 0 ]]; do case "$1" in --engine) ENGINE="$2"; shift 2;; --no-build) BUILD=0; shift;; *) echo "unknown arg $1"; exit 2;; esac; done
parse_engine "$ENGINE"

log "building images (tools, spark, kyuubi) - cached after the first run"
(( BUILD )) && dc build tools spark kyuubi

log "generating local credential files"
generate_local_credentials

log "starting core: minio, postgres, lakekeeper (+migrate, bootstrap), spark/jupyter, prometheus, grafana"
dc up -d minio postgres lakekeeper-migrate lakekeeper minio-init catalog-init spark prometheus grafana
wait_healthy lakekeeper 180
dc logs catalog-init | sed 's/^/  /'
wait_healthy spark 300

for e in "${ENGINES[@]}"; do
  log "starting engine path: $e"
  case "$e" in
    trino)  dc up -d trino;  wait_healthy trino 300 ;;
    kyuubi) dc up -d ranger-admin-stub kyuubi; wait_healthy kyuubi 300 ;;
  esac
done

log "endpoints"
cat <<EP
  Lakekeeper REST catalog  http://localhost:8181/catalog      UI: http://localhost:8181
  Object storage (S3 API)  http://localhost:9000               browser: http://localhost:9001  (${S3_ROOT_USER})
  JupyterLab (PySpark)     http://localhost:8888/?token=${JUPYTER_TOKEN}
  Spark UI (ETL/notebook)  http://localhost:4040  (while a job runs)
  Prometheus / Grafana     http://localhost:9090   http://localhost:3000  (admin / \$GRAFANA_ADMIN_PASSWORD)
EP
[[ " ${ENGINES[*]} " == *" trino "* ]]  && echo "  Trino  (HTTPS, self-signed) https://localhost:8443   users: admin / analyst1 / restricted1 (Web UI = same logins)"
[[ " ${ENGINES[*]} " == *" kyuubi "* ]] && echo "  Kyuubi (Thrift/JDBC)     jdbc:hive2://localhost:10009/   REST+UI: http://localhost:10099   Ranger stub: http://localhost:6080"
echo; echo "next: scripts/seed.sh   then   scripts/validate.sh --engine $ENGINE"
