#!/usr/bin/env bash
# Stop everything.  scripts/stop.sh            -> containers stopped, DATA KEPT (volumes persist: catalog, storage, policies)
#                   scripts/stop.sh --purge    -> also delete volumes (full reset; the seed recreates identical data)
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
if [[ "${1:-}" == "--purge" ]]; then
  log "stopping and removing containers AND volumes"
  dc down --volumes --remove-orphans
else
  log "stopping containers (volumes kept)"
  dc down --remove-orphans
fi
