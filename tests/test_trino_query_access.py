"""Trino path: sample queries per role, PII masking/filtering, metadata-table and write denial (file-based rules)."""

import os

import pytest
from _access_common import (
    check_admin_sees_raw,
    check_bronze_blocked,
    check_metadata_tables_blocked,
    check_restricted_sees_masked,
    check_street_address_blocked,
    check_write_blocked,
    run_sample_queries,
)

from conftest import EngineError, connect

ENGINE = "trino"
pytestmark = pytest.mark.skipif(
    "trino" not in __import__("os").environ.get("ENGINE", "both").replace("both", "trino kyuubi"),
    reason="trino not selected",
)


def test_bad_password_rejected():
    import trino
    from trino.auth import BasicAuthentication

    conn = trino.dbapi.connect(
        host="trino",
        port=int(os.environ.get("TRINO_PORT", "8443")),
        user="analyst1",
        auth=BasicAuthentication("analyst1", "wrong"),
        http_scheme="https",
        verify=False,
        catalog="lakehouse",
    )
    with pytest.raises(Exception):
        conn.cursor().execute("SELECT 1").fetchall()


def test_admin_runs_all_sample_queries_and_sees_raw_pii(sample_queries):
    rows = run_sample_queries(ENGINE, "admin", sample_queries, expect_pii="raw")
    check_admin_sees_raw(rows)


def test_analyst_runs_sample_queries_but_pii_is_denied(sample_queries):
    run_sample_queries(ENGINE, "analyst", sample_queries, expect_pii="deny")


def test_restricted_analyst_gets_masked_and_filtered_pii(sample_queries):
    rows = run_sample_queries(ENGINE, "restricted", sample_queries, expect_pii="masked")
    check_restricted_sees_masked(rows, ENGINE)
    # the row filter hides opted-out customers: counts differ from admin
    r = connect(ENGINE, "restricted")
    a = connect(ENGINE, "admin")
    try:
        assert (
            r.query("SELECT COUNT(*) FROM lakehouse.silver.customer_pii")[0][0]
            < a.query("SELECT COUNT(*) FROM lakehouse.silver.customer_pii")[0][0]
        )
    finally:
        r.close()
        a.close()


@pytest.mark.parametrize("role", ["analyst", "restricted"])
def test_metadata_tables_blocked(role):
    check_metadata_tables_blocked(ENGINE, role)


@pytest.mark.parametrize("role", ["analyst", "restricted"])
def test_bronze_blocked(role):
    check_bronze_blocked(ENGINE, role)


def test_street_address_blocked_for_restricted():
    check_street_address_blocked(ENGINE, "restricted")


@pytest.mark.parametrize("role", ["analyst", "restricted"])
def test_writes_blocked(role):
    check_write_blocked(ENGINE, role)


@pytest.mark.parametrize("role", ["analyst", "restricted", "admin"])
def test_maintenance_procedures_blocked(role):
    """Trino is a read-only serving engine (iceberg.security=read_only): table procedures and CALLs are denied for
    everyone, including admins. Maintenance belongs to the Spark job spark/maintenance.py (P4)."""
    c = connect(ENGINE, role)
    try:
        for sql in [
            "ALTER TABLE lakehouse.gold.event_funnel_daily EXECUTE expire_snapshots(retention_threshold => '7d')",
            "ALTER TABLE lakehouse.gold.event_funnel_daily EXECUTE optimize",
            "CALL lakehouse.system.rollback_to_snapshot('gold', 'event_funnel_daily', 1)",
        ]:
            with pytest.raises(EngineError):
                c.query(sql)
    finally:
        c.close()


def test_time_travel_posture_for_analyst():
    """P4 on Trino: metadata tables are denied (no snapshot ids discoverable) and FOR VERSION AS OF with a guessed id
    fails. FOR TIMESTAMP AS OF inside the retention window remains syntactically possible (documented limitation);
    it still goes through the same column masks / row filters, so it is not a privilege escalation."""
    c = connect(ENGINE, "analyst")
    try:
        with pytest.raises(EngineError):
            c.query('SELECT snapshot_id FROM lakehouse.silver."customers$snapshots"')
        with pytest.raises(EngineError):
            c.query("SELECT * FROM lakehouse.silver.customers FOR VERSION AS OF 1234567890123 LIMIT 1")
    finally:
        c.close()


def test_resource_group_assignment():
    """Queries from analysts land in global.interactive.user-<name>; admins in 'admin' (resource-groups.json selectors).
    Only admins may read system.runtime.queries (analysts have no rule for the system catalog's tables)."""
    a = connect(ENGINE, "analyst")
    ad = connect(ENGINE, "admin")
    try:
        a.query("SELECT 1")
        rows = ad.query(
            "SELECT resource_group_id FROM system.runtime.queries WHERE user = 'analyst1' AND query LIKE 'SELECT 1%' ORDER BY created DESC LIMIT 1"
        )
        mine = ad.query(
            "SELECT resource_group_id FROM system.runtime.queries WHERE user = 'admin' AND query LIKE 'SELECT 1%' ORDER BY created DESC LIMIT 1"
        )
    finally:
        a.close()
        ad.close()
    assert rows and rows[0][0] == ["global", "interactive", "user-analyst1"], rows
    assert mine == [] or mine[0][0] == ["admin"], mine
