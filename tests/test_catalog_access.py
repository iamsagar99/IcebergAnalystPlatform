"""Catalog + storage layer checks (engine-independent): the REST catalog serves the tables Spark created,
snapshots are intact, and the catalog never vends storage credentials (engines bring their own identity)."""

import json
import os
import urllib.request

import boto3
import pytest

BASE = os.environ.get("LAKEKEEPER_URL", "http://lakekeeper:8181") + "/catalog/v1"
TABLES = {
    "bronze": {"transactions_raw", "events_raw", "customers_cdc"},
    "silver": {"transactions", "events", "customers", "customer_pii", "stores", "product_categories"},
    "gold": {"daily_sales_by_store", "category_sales_daily", "customer_monthly_summary", "event_funnel_daily"},
}


def get(path, headers=None):
    req = urllib.request.Request(BASE + path, headers=headers or {})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


@pytest.fixture(scope="module")
def prefix():
    return (lambda c: c.get("defaults", {}).get("prefix") or c.get("overrides", {}).get("prefix", ""))(
        get("/config?warehouse=lakehouse")
    )


def test_namespaces_and_tables_registered(prefix):
    namespaces = {ns[0] for ns in get(f"/{prefix}/namespaces")["namespaces"]}
    assert set(TABLES) <= namespaces
    for ns, expected_tables in TABLES.items():
        found = {t["name"] for t in get(f"/{prefix}/namespaces/{ns}/tables")["identifiers"]}
        assert expected_tables <= found, f"{ns}: missing {expected_tables - found}"


def test_current_snapshot_and_history_intact(prefix):
    md = get(f"/{prefix}/namespaces/silver/tables/customers")["metadata"]
    assert md["format-version"] == 2
    assert md["current-snapshot-id"] is not None
    assert len(md["snapshots"]) >= 1
    assert any(s["snapshot-id"] == md["current-snapshot-id"] for s in md["snapshots"])
    assert md["location"].startswith("s3://lakehouse/warehouse/")


def test_catalog_does_not_vend_storage_credentials(prefix):
    """Credential vending and remote signing are disabled on the warehouse: a client asking for delegated access
    gets table metadata but NO storage credentials. Reading data therefore requires an engine identity."""
    resp = get(
        f"/{prefix}/namespaces/gold/tables/daily_sales_by_store",
        headers={"X-Iceberg-Access-Delegation": "vended-credentials,remote-signing"},
    )
    assert "storage-credentials" not in resp or not resp["storage-credentials"]
    cfg = resp.get("config", {})
    assert not any(k in cfg for k in ("s3.access-key-id", "s3.secret-access-key", "s3.session-token"))


def test_storage_requires_engine_identity():
    """Anonymous S3 access to the warehouse bucket is refused; the engine service identity can list it."""
    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    bucket = os.environ.get("S3_BUCKET", "lakehouse")
    from botocore import UNSIGNED
    from botocore.config import Config

    anon = boto3.client("s3", endpoint_url=endpoint, config=Config(signature_version=UNSIGNED), region_name="local-01")
    with pytest.raises(Exception):
        anon.list_objects_v2(Bucket=bucket, MaxKeys=1)
    engine = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("S3_REGION", "local-01"),
        aws_access_key_id=os.environ["ENGINE_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["ENGINE_S3_SECRET_KEY"],
    )
    assert engine.list_objects_v2(Bucket=bucket, Prefix="warehouse/", MaxKeys=1)["KeyCount"] == 1
