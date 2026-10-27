#!/usr/bin/env python3
"""Scheduled Iceberg maintenance (platform team only, never analysts): expire snapshots + remove orphan files.

Schedule (P4/P10): nightly after the ETL window. Retention windows per layer come from table properties set in
create_tables.py (bronze 3d, silver/gold 7d, min snapshots kept 2/3). Snapshots are expired for storage and
metadata hygiene, NOT to block time travel: analysts are prevented from time travel by authorization (docs/security-model.md).

Usage: maintenance.py [--older-than-minutes N] [--orphan-older-than-minutes N] [--dry-run]
  default: table-level retention properties decide (older_than omitted), orphan files older than 3 days (Iceberg default).
"""

import datetime as dt

from lakehouse import NAMESPACES, arg_parser, session, sql, timed


def main():
    p = arg_parser(__doc__)
    p.add_argument(
        "--older-than-minutes",
        type=int,
        default=None,
        help="expire snapshots older than N minutes (demo only; production relies on table properties)",
    )
    p.add_argument("--orphan-older-than-minutes", type=int, default=3 * 24 * 60)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    spark = session("maintenance")
    c = a.catalog

    tables = []
    for ns in NAMESPACES:
        for row in sql(spark, f"SHOW TABLES IN {c}.{ns}").collect():
            tables.append(f"{ns}.{row['tableName']}")

    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    for t in tables:
        before = sql(spark, f"SELECT COUNT(*) AS n FROM {c}.{t}.snapshots").collect()[0]["n"]
        with timed(f"expire_snapshots {t} (had {before} snapshots)"):
            if a.older_than_minutes is not None:
                ts = (now - dt.timedelta(minutes=a.older_than_minutes)).strftime("%Y-%m-%d %H:%M:%S")
                stmt = f"CALL {c}.system.expire_snapshots(table => '{t}', older_than => TIMESTAMP '{ts}', retain_last => 1)"
            else:
                stmt = f"CALL {c}.system.expire_snapshots(table => '{t}')"
            if a.dry_run:
                print("DRY RUN:", stmt)
            else:
                sql(spark, stmt, show=True)
        with timed(f"remove_orphan_files {t}"):
            ts = (now - dt.timedelta(minutes=a.orphan_older_than_minutes)).strftime("%Y-%m-%d %H:%M:%S")
            stmt = (
                f"CALL {c}.system.remove_orphan_files(table => '{t}', older_than => TIMESTAMP '{ts}', "
                f"dry_run => {'true' if a.dry_run else 'false'})"
            )
            sql(spark, stmt, show=True)
        after = sql(spark, f"SELECT COUNT(*) AS n FROM {c}.{t}.snapshots").collect()[0]["n"]
        cur = sql(spark, f"SELECT snapshot_id FROM {c}.{t}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
        print(f"{t}: snapshots {before} -> {after}, current snapshot {cur[0]['snapshot_id'] if cur else None}")


if __name__ == "__main__":
    main()
