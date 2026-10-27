#!/usr/bin/env python3
"""Concurrency workload generator for the Trino and Kyuubi paths (same workload, same tables, same users).

  workload-generator.py --engine trino|kyuubi --concurrency N [--iterations M] [--duration S] [--out DIR]

Each worker is one analyst session (user 'bench', Trino source 'bench-<i>'), running the P9 workload mix drawn from
sql/sample_queries.sql in a loop. Captured per query: latency, rows, error. Output: JSON + Markdown summary under
benchmarks/results/ (only real runs are committed there). Runs inside the tools container (scripts/benchmark.sh).
"""
import argparse
import datetime as dt
import json
import os
import re
import statistics
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))
from conftest import load_named_queries  # noqa: E402

# Workload mix (P9): weights = how often an interactive analyst session issues each query type.
MIX = {
    "q1_daily_revenue_by_region_gold": 3,
    "q2_top_categories_month_gold": 3,
    "q4_funnel_conversion_gold": 2,
    "q7_customer_monthly_top_spenders_gold": 2,
    "q8_store_lookup_point_query": 3,
    "q6_current_state_customer_after_cdc": 2,
    "q3_fact_join_dimension_silver": 1,
    "q5_events_last_7_days_silver": 1,
}


def make_client(engine, worker):
    user, pw = "bench", os.environ.get("BENCH_PASSWORD", "")
    if engine == "trino":
        import trino
        from trino.auth import BasicAuthentication
        return trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"), port=int(os.environ.get("TRINO_PORT", "8443")),
                                   user=user, auth=BasicAuthentication(user, pw), http_scheme="https", verify=False, catalog="lakehouse",
                                   source=f"bench-{worker}")
    from pyhive import hive
    return hive.connect(host=os.environ.get("KYUUBI_HOST", "kyuubi"), port=int(os.environ.get("KYUUBI_PORT", "10009")),
                        username=user, password=pw, auth="CUSTOM", database="gold")


def worker(engine, wid, queries, plan, results, stop_at, lock):
    t0 = time.time()
    try:
        conn = make_client(engine, wid)
    except Exception as e:  # connection failure is a result too
        with lock:
            results.append({"worker": wid, "query": "connect", "ok": False, "latency_s": time.time() - t0, "error": str(e)[:300]})
        return
    with lock:
        results.append({"worker": wid, "query": "connect", "ok": True, "latency_s": time.time() - t0, "rows": 0})
    i = 0
    while i < len(plan) and time.time() < stop_at:
        name = plan[i]
        i += 1
        cur = conn.cursor()
        t = time.time()
        try:
            cur.execute(queries[name])
            rows = cur.fetchall()
            rec = {"worker": wid, "query": name, "ok": True, "latency_s": time.time() - t, "rows": len(rows)}
        except Exception as e:
            rec = {"worker": wid, "query": name, "ok": False, "latency_s": time.time() - t, "error": str(e)[:300]}
        finally:
            try:
                cur.close()
            except Exception:
                pass
        with lock:
            results.append(rec)
    try:
        conn.close()
    except Exception:
        pass


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1)))))
    return xs[k]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, choices=["trino", "kyuubi"])
    ap.add_argument("--concurrency", type=int, required=True)
    ap.add_argument("--iterations", type=int, default=2, help="passes over the workload mix per worker")
    ap.add_argument("--duration", type=int, default=600, help="hard stop in seconds")
    ap.add_argument("--out", default=str(ROOT / "benchmarks" / "results"))
    ap.add_argument("--label", default="")
    a = ap.parse_args()

    queries = load_named_queries(ROOT / "sql" / "sample_queries.sql")
    plan = [q for q, w in MIX.items() for _ in range(w)] * a.iterations
    results, lock = [], threading.Lock()
    stop_at = time.time() + a.duration
    started = dt.datetime.now(dt.timezone.utc)
    threads = [threading.Thread(target=worker, args=(a.engine, i, queries, plan, results, stop_at, lock), daemon=True)
               for i in range(a.concurrency)]
    t0 = time.time()
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    wall = time.time() - t0

    qres = [r for r in results if r["query"] != "connect"]
    conn = [r for r in results if r["query"] == "connect"]
    ok = [r["latency_s"] for r in qres if r["ok"]]
    per_query = {}
    for name in MIX:
        lat = [r["latency_s"] for r in qres if r["query"] == name and r["ok"]]
        err = sum(1 for r in qres if r["query"] == name and not r["ok"])
        if lat or err:
            per_query[name] = {"n": len(lat), "errors": err, "p50_s": pct(lat, 50), "p95_s": pct(lat, 95), "max_s": max(lat) if lat else None}
    summary = {
        "engine": a.engine, "concurrency": a.concurrency, "iterations": a.iterations, "label": a.label,
        "started_utc": started.isoformat(), "wall_s": round(wall, 2),
        "queries_total": len(qres), "queries_ok": len(ok), "queries_failed": len(qres) - len(ok),
        "throughput_qps": round(len(ok) / wall, 3) if wall else None,
        "latency_p50_s": pct(ok, 50), "latency_p95_s": pct(ok, 95), "latency_max_s": max(ok) if ok else None,
        "latency_mean_s": statistics.fmean(ok) if ok else None,
        "connect_p95_s": pct([c["latency_s"] for c in conn if c["ok"]], 95), "connect_failed": sum(1 for c in conn if not c["ok"]),
        "per_query": per_query,
        "errors": sorted({r.get("error", "") for r in qres if not r["ok"]})[:10],
    }
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    stem = f"{started.strftime('%Y%m%d-%H%M%S')}-{a.engine}-c{a.concurrency}{('-' + a.label) if a.label else ''}"
    (out / f"{stem}.json").write_text(json.dumps({"summary": summary, "results": results}, indent=1, default=str))
    md = [f"# {a.engine} @ concurrency {a.concurrency} ({started.strftime('%Y-%m-%d %H:%M UTC')})", "",
          f"wall {wall:.1f}s | queries ok/failed {len(ok)}/{len(qres) - len(ok)} | throughput {summary['throughput_qps']} q/s | "
          f"p50 {summary['latency_p50_s']:.2f}s | p95 {summary['latency_p95_s']:.2f}s | max {summary['latency_max_s']:.2f}s | connect p95 {summary['connect_p95_s']}s" if ok else f"wall {wall:.1f}s | ALL {len(qres)} QUERIES FAILED",
          "", "| query | n | errors | p50 s | p95 s | max s |", "|---|---|---|---|---|---|"]
    for q, s in per_query.items():
        md.append(f"| {q} | {s['n']} | {s['errors']} | {s['p50_s'] and round(s['p50_s'], 2)} | {s['p95_s'] and round(s['p95_s'], 2)} | {s['max_s'] and round(s['max_s'], 2)} |")
    if summary["errors"]:
        md += ["", "errors:", *[f"- `{e}`" for e in summary["errors"]]]
    (out / f"{stem}.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))
    print(f"\nwritten: {out / stem}.json / .md")


if __name__ == "__main__":
    main()
