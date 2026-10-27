# 0003. Trino's Iceberg connector runs with iceberg.security=read_only

Date: 2026-10-26. Status: accepted.

## Context

While testing role restrictions, an analyst with SELECT-only grants on a read-only catalog could run
`ALTER TABLE ... EXECUTE expire_snapshots`, `optimize` and `remove_orphan_files` on Trino 484. The file-based
system access control does not govern table procedures.

## Decision

Set `iceberg.security=read_only` on the `lakehouse` catalog. Trino is a serving engine; Spark owns every write and
all maintenance.

## Consequences

* Table procedures, CALLs, INSERT/DELETE/UPDATE are denied for everyone on Trino, including admins. Tests cover
  analyst, restricted analyst and admin.
* Maintenance has exactly one entry point (`spark/maintenance.py`), which also makes the retention policy auditable.
* With Ranger or OPA as the system access control in production, this can return to `iceberg.security=system`
  with explicit procedure policies.
