"""Kyuubi path: same role expectations enforced by the Kyuubi Spark AuthZ (Ranger) plugin, plus engine-sharing checks."""

import json
import os
import urllib.request

import pytest
from _access_common import (
    assert_denied,
    check_admin_sees_raw,
    check_bronze_blocked,
    check_metadata_tables_blocked,
    check_restricted_sees_masked,
    check_street_address_blocked,
    check_write_blocked,
    run_sample_queries,
)

from conftest import EngineError, connect

ENGINE = "kyuubi"
pytestmark = pytest.mark.skipif(
    "kyuubi" not in os.environ.get("ENGINE", "both").replace("both", "trino kyuubi"), reason="kyuubi not selected"
)
REST = f"http://{os.environ.get('KYUUBI_HOST', 'kyuubi')}:10099/api/v1"


def rest(path):
    """Kyuubi REST API uses the same authentication as JDBC (HTTP basic -> JDBC auth provider)."""
    import base64

    token = base64.b64encode(f"admin:{os.environ.get('ADMIN_PASSWORD', '')}".encode()).decode()
    req = urllib.request.Request(REST + path, headers={"Authorization": f"Basic {token}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def test_bad_password_rejected():
    from pyhive import hive

    with pytest.raises(Exception):
        hive.connect(
            host=os.environ.get("KYUUBI_HOST", "kyuubi"),
            port=10009,
            username="analyst1",
            password="wrong",
            auth="CUSTOM",
        )


def test_admin_runs_all_sample_queries_and_sees_raw_pii(sample_queries):
    rows = run_sample_queries(ENGINE, "admin", sample_queries, expect_pii="raw")
    check_admin_sees_raw(rows)


def test_analyst_runs_sample_queries_but_pii_is_denied(sample_queries):
    run_sample_queries(ENGINE, "analyst", sample_queries, expect_pii="deny")


def test_restricted_analyst_gets_masked_and_filtered_pii(sample_queries):
    rows = run_sample_queries(ENGINE, "restricted", sample_queries, expect_pii="masked")
    check_restricted_sees_masked(rows, ENGINE)
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


@pytest.mark.xfail(
    strict=True,
    reason="KNOWN GAP (verified on Kyuubi 1.12.0 AuthZ + Iceberg 1.11.0): Iceberg metadata tables "
    "(.snapshots/.history/.files) are not authorized by the plugin. Compensating controls: analysts hold no "
    "storage credentials (file paths are useless) and time-travel reads still pass masks/row filters "
    "(test_time_travel_posture). strict=True: if a Kyuubi upgrade closes the gap this XPASSes -> update docs.",
)
@pytest.mark.parametrize("role", ["analyst", "restricted"])
def test_metadata_tables_blocked(role):
    check_metadata_tables_blocked(ENGINE, role)


def test_pii_row_count_hidden_from_analyst():
    """Column-less scans are not authorized by the AuthZ plugin; the '1 = 0' row filter hides the PII table's row count."""
    c = connect(ENGINE, "analyst")
    try:
        assert c.query("SELECT COUNT(*) FROM lakehouse.silver.customer_pii")[0][0] == 0
        assert c.query("SELECT COUNT(*) FROM lakehouse.silver.customer_pii WHERE national_id LIKE '1%'")[0][0] == 0
    finally:
        c.close()


def test_time_travel_posture():
    """Time travel (VERSION AS OF) is authorized exactly like a current-snapshot read on Kyuubi: analysts are denied
    on PII columns, restricted analysts get masked + row-filtered rows. (Snapshot ids are discoverable - see
    test_metadata_tables_blocked - but knowing them gains nothing.)"""
    ad = connect(ENGINE, "admin")
    try:
        snap = ad.query("SELECT snapshot_id FROM lakehouse.silver.customer_pii.snapshots ORDER BY committed_at")[0][0]
    finally:
        ad.close()
    a = connect(ENGINE, "analyst")
    r = connect(ENGINE, "restricted")
    try:
        assert_denied(a, f"SELECT email FROM lakehouse.silver.customer_pii VERSION AS OF {snap} WHERE customer_id = 1")
        assert a.query(f"SELECT COUNT(*) FROM lakehouse.silver.customer_pii VERSION AS OF {snap}")[0][0] == 0
        rows = r.query(
            f"SELECT customer_id, email, phone, national_id, date_of_birth, marketing_opt_in "
            f"FROM lakehouse.silver.customer_pii VERSION AS OF {snap} WHERE customer_id <= 20 ORDER BY customer_id"
        )
        assert rows
        check_restricted_sees_masked(rows, ENGINE)
    finally:
        a.close()
        r.close()


@pytest.mark.parametrize("role", ["analyst", "restricted"])
def test_bronze_blocked(role):
    check_bronze_blocked(ENGINE, role)


def test_street_address_blocked_for_restricted():
    check_street_address_blocked(ENGINE, "restricted")


@pytest.mark.parametrize("role", ["analyst", "restricted"])
def test_writes_blocked(role):
    check_write_blocked(ENGINE, role)


def test_analyst_cannot_disable_authz_extension():
    """kyuubi.session.conf.restrict.list rejects spark.sql.extensions overrides at connection time."""
    with pytest.raises(Exception):
        c = connect(
            ENGINE,
            "analyst",
            conf={"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"},
        )
        c.query("SELECT 1")


def test_maintenance_procedure_blocked_for_analyst():
    c = connect(ENGINE, "analyst")
    try:
        with pytest.raises(EngineError):
            c.query("CALL lakehouse.system.expire_snapshots(table => 'gold.event_funnel_daily')")
    finally:
        c.close()


def app_id(client):
    """The Spark application id of the engine serving this session (SET is answered by the engine)."""
    rows = client.query("SET spark.app.id")
    return rows[0][1]


def test_group_share_level_two_analysts_share_one_engine():
    """analyst1 and analyst2 are both in group 'analysts' (GROUP share level) -> served by the SAME Spark application,
    i.e. a shared engine, not a private Spark session per analyst."""
    a1 = connect(ENGINE, "analyst")
    a2 = connect(ENGINE, "analyst2")
    try:
        id1, id2 = app_id(a1), app_id(a2)
        assert id1 and id1 == id2, (id1, id2)
    finally:
        a1.close()
        a2.close()


def test_user_share_level_admin_gets_own_engine():
    """admin has ___admin___.kyuubi.engine.share.level=USER -> a different engine than the analysts group; restricted
    analysts are another group -> another engine."""
    a = connect(ENGINE, "analyst")
    ad = connect(ENGINE, "admin")
    r = connect(ENGINE, "restricted")
    try:
        ids = {app_id(a), app_id(ad), app_id(r)}
        assert len(ids) == 3, ids
    finally:
        a.close()
        ad.close()
        r.close()


def test_second_session_reuses_warm_engine():
    """Reconnecting as the same group reuses the running engine (no JVM warm-up): the app id is unchanged."""
    a = connect(ENGINE, "analyst")
    first = app_id(a)
    a.close()
    b = connect(ENGINE, "analyst2")
    try:
        assert app_id(b) == first
    finally:
        b.close()
