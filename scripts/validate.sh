#!/usr/bin/env bash
# End-to-end validation.  scripts/validate.sh --engine trino|kyuubi|both
# Proves: infrastructure up, Spark wrote Iceberg via the catalog, engines discover + query tables, sample queries agree
# with recorded expectations, current-state reads, role restrictions on each engine, reproducible seed.
# Nothing is marked passed unless pytest actually executed it (exit code is pytest's).
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
ENGINE=both
while [[ $# -gt 0 ]]; do case "$1" in --engine) ENGINE="$2"; shift 2;; *) echo "unknown arg $1"; exit 2;; esac; done
parse_engine "$ENGINE"

log "1. infrastructure health"
for s in minio postgres lakekeeper spark "${ENGINES[@]}"; do
  cid=$(dc ps -q "$s" 2>/dev/null || true)
  [[ -n "$cid" ]] || { echo "  $s: NOT RUNNING -> run scripts/start.sh --engine $ENGINE"; exit 1; }
  st=$(docker inspect -f '{{.State.Health.Status}}' "$cid"); echo "  $s: $st"; [[ "$st" == "healthy" ]] || exit 1
done

log "2. catalog metadata written by Spark (tables, snapshots, current snapshot id)"
"$ROOT/scripts/catalog-info.sh"

log "3. pytest: catalog/storage, data quality on each engine, role restrictions on each engine (ENGINE=$ENGINE)"
dc run --rm --no-deps -T -e ENGINE="$ENGINE" tools pytest -q --engine "$ENGINE" tests
