#!/usr/bin/env bash
# Seed the warehouse deterministically:  scripts/seed.sh [--no-reset]
#   1. create namespaces/tables (drop + recreate unless --no-reset)   2. generate bronze data   3. bronze->silver (CDC)->gold
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
parse_engine core
RESET="--reset"; [[ "${1:-}" == "--no-reset" ]] && RESET=""
wait_healthy spark 120
log "1/3 create tables ${RESET}"
"$ROOT/spark/spark-submit.sh" create_tables.py $RESET
log "2/3 generate deterministic bronze data"
"$ROOT/spark/spark-submit.sh" generate_data.py
log "3/3 silver CDC merge + gold marts"
"$ROOT/spark/spark-submit.sh" seed_warehouse.py
log "catalog metadata after seeding"
"$ROOT/scripts/catalog-info.sh"
