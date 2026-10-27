#!/usr/bin/env bash
# Show namespaces, tables, snapshot counts and the current snapshot id straight from the REST catalog (engine-independent).
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
dc run --rm --no-deps -T tools python - <<'PY'
import json, os, urllib.request
base = os.environ["LAKEKEEPER_URL"] + "/catalog/v1"
def get(path):
    with urllib.request.urlopen(base + path, timeout=30) as r: return json.load(r)
cfg = urllib.request.urlopen(base + "/config?warehouse=lakehouse", timeout=30)
cfg = json.load(cfg)
prefix = cfg.get("defaults", {}).get("prefix") or cfg.get("overrides", {}).get("prefix", "")
print(f"warehouse prefix: {prefix}")
print(f"{'table':38s} {'snapshots':>9s}  {'current snapshot id':>20s}  {'format':>6s}  location")
for ns in get(f"/{prefix}/namespaces")["namespaces"]:
    n = ns[0]
    for t in get(f"/{prefix}/namespaces/{n}/tables")["identifiers"]:
        md = get(f"/{prefix}/namespaces/{n}/tables/{t['name']}")["metadata"]
        print(f"{n+'.'+t['name']:38s} {len(md.get('snapshots', [])):>9d}  {str(md.get('current-snapshot-id')):>20s}  {md.get('format-version'):>6}  {md['location']}")
PY
