#!/usr/bin/env bash
# Low-concurrency benchmark on each running engine.  scripts/benchmark.sh [--engine trino|kyuubi|both] [--concurrency "5 10"] [--iterations 2]
# Results (real runs only) land in benchmarks/results/<timestamp>-<engine>-c<N>.{json,md}
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
ENGINE=both; CONC="5 10"; ITER=2; LABEL=""
while [[ $# -gt 0 ]]; do case "$1" in
  --engine) ENGINE="$2"; shift 2;; --concurrency) CONC="$2"; shift 2;; --iterations) ITER="$2"; shift 2;; --label) LABEL="$2"; shift 2;;
  *) echo "unknown arg $1"; exit 2;; esac; done
parse_engine "$ENGINE"
for e in "${ENGINES[@]}"; do
  for c in $CONC; do
    log "benchmark: engine=$e concurrency=$c iterations=$ITER"
    dc run --rm --no-deps -T tools python benchmarks/workload-generator.py --engine "$e" --concurrency "$c" --iterations "$ITER" ${LABEL:+--label "$LABEL"}
  done
done
