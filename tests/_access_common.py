"""Role-based access expectations shared by the Trino and Kyuubi access tests (one model, two enforcement points)."""
import re

import pytest

from conftest import EngineError, connect

PII_SQL = ("SELECT customer_id, email, phone, national_id, date_of_birth, marketing_opt_in "
           "FROM lakehouse.silver.customer_pii WHERE customer_id <= 20 ORDER BY customer_id")


def assert_denied(client, sql):
    with pytest.raises(EngineError) as e:
        client.query(sql)
    msg = str(e.value).lower()
    assert any(k in msg for k in ("access denied", "permission", "not allowed", "does not have", "denied", "not exist", "cannot be found", "not found")), msg


def run_sample_queries(engine, role, sample_queries, expect_pii):
    c = connect(engine, role)
    try:
        for name, sql in sample_queries.items():
            if name == "q9_pii_restricted":
                continue
            rows = c.query(sql)
            assert rows, f"{engine}/{role}: {name} returned no rows"
        if expect_pii == "deny":
            assert_denied(c, PII_SQL)
        else:
            rows = c.query(PII_SQL)
            assert rows
            return rows
    finally:
        c.close()


def check_admin_sees_raw(rows):
    cid, email, phone, nid, dob, opt = rows[0]
    assert "@example.test" in email and email.startswith(email[0].lower())
    assert phone.startswith("+1-555-") and len(nid) == 11 and "-" in nid
    # admin sees opted-in AND opted-out rows (row filter does not apply)
    assert {r[5] for r in rows} == {True, False}


def check_restricted_sees_masked(rows, engine):
    """Masks differ by engine (Trino SQL expressions vs. Ranger mask types) but the invariants are the same."""
    for cid, email, phone, nid, dob, opt in rows:
        assert opt is True, "row filter failed: opted-out customer visible"
        assert "@" not in email or "***" in email, f"email not masked: {email}"
        assert not phone.startswith("+1-555-") or phone.startswith("***"), f"phone not masked: {phone}"
        assert not re.fullmatch(r"\d{3}-\d{2}-\d{4}", nid), f"national_id not masked: {nid}"
        assert re.match(r"^\d{4}-01-01", str(dob)), f"date_of_birth not reduced to year: {dob}"  # Trino: date, Ranger MASK_DATE_SHOW_YEAR: timestamp


def check_metadata_tables_blocked(engine, role):
    c = connect(engine, role)
    try:
        if engine == "trino":
            sqls = ['SELECT * FROM lakehouse.gold."daily_sales_by_store$snapshots"',
                    'SELECT * FROM lakehouse.silver."customers$history"']
        else:
            sqls = ["SELECT * FROM lakehouse.gold.daily_sales_by_store.snapshots",
                    "SELECT * FROM lakehouse.silver.customers.history"]
        for s in sqls:
            assert_denied(c, s)
    finally:
        c.close()


def check_bronze_blocked(engine, role):
    """bronze is ETL-only. Column reads are denied on both engines. Column-less scans (COUNT(*)) are denied by Trino's
    table rule; the Kyuubi AuthZ plugin does not authorize column-less scans (verified gap), so a Ranger row filter
    '1 = 0' is the compensating control there: the count must be 0."""
    c = connect(engine, role)
    try:
        assert_denied(c, "SELECT transaction_id FROM lakehouse.bronze.transactions_raw LIMIT 1")
        assert_denied(c, "SELECT * FROM lakehouse.bronze.customers_cdc LIMIT 1")
        if engine == "trino":
            assert_denied(c, "SELECT COUNT(*) FROM lakehouse.bronze.transactions_raw")
        else:
            assert c.query("SELECT COUNT(*) FROM lakehouse.bronze.transactions_raw")[0][0] == 0
    finally:
        c.close()


def check_street_address_blocked(engine, role):
    c = connect(engine, role)
    try:
        assert_denied(c, "SELECT street_address FROM lakehouse.silver.customer_pii LIMIT 1")
    finally:
        c.close()


def check_write_blocked(engine, role):
    c = connect(engine, role)
    try:
        assert_denied(c, "DELETE FROM lakehouse.gold.event_funnel_daily WHERE event_date = DATE '2026-09-01'")
        assert_denied(c, "INSERT INTO lakehouse.silver.stores VALUES (9999, 'x', 'r', 'US', 'c', 'standard', DATE '2026-01-01')")
    finally:
        c.close()
