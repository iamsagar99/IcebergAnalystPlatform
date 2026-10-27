#!/usr/bin/env python3
"""Idempotent Lakekeeper bootstrap: accept terms, create the 'lakehouse' warehouse on the local S3 bucket.

Runs inside the tools image (service catalog-init). Safe to re-run: it exits 0 if the warehouse exists.
Storage profile decisions (see docs/security-model.md):
  * sts-enabled=false and remote-signing-enabled=false  -> the catalog NEVER hands out storage credentials.
    Every engine must bring its own storage identity (service account), exactly like Kerberos on HDFS.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("LAKEKEEPER_URL", "http://lakekeeper:8181")
WAREHOUSE = os.environ.get("LAKEKEEPER_WAREHOUSE", "lakehouse")
PROJECT_ID = "00000000-0000-0000-0000-000000000000"


def call(method, path, body=None, ok=(200, 201, 204)):
    req = urllib.request.Request(BASE + path, method=method, headers={"Content-Type": "application/json"})
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(req, data=data, timeout=30) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        if e.code in ok:
            return e.code, raw
        return e.code, raw


def wait_healthy():
    for _ in range(120):
        try:
            with urllib.request.urlopen(BASE + "/health", timeout=5) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        time.sleep(2)
    sys.exit("lakekeeper did not become healthy")


def main():
    wait_healthy()
    status, body = call("POST", "/management/v1/bootstrap", {"accept-terms-of-use": True}, ok=(200, 204, 400))
    print(f"bootstrap: HTTP {status} {'(already bootstrapped)' if status == 400 else ''}")

    status, body = call("GET", f"/management/v1/warehouse?project-id={PROJECT_ID}")
    if status != 200:
        sys.exit(f"list warehouses failed: {status} {body}")
    existing = {w["name"]: w for w in body.get("warehouses", [])}
    if WAREHOUSE in existing:
        print(f"warehouse '{WAREHOUSE}' already exists: id={existing[WAREHOUSE]['id']}")
        return

    payload = {
        "warehouse-name": WAREHOUSE,
        "project-id": PROJECT_ID,
        "storage-profile": {
            "type": "s3",
            "bucket": os.environ["S3_BUCKET"],
            "key-prefix": "warehouse",
            "endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
            "region": os.environ.get("S3_REGION", "local-01"),
            "path-style-access": True,
            "flavor": "s3-compat",
            "sts-enabled": False,
            "remote-signing-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": os.environ["S3_ROOT_USER"],
            "aws-secret-access-key": os.environ["S3_ROOT_PASSWORD"],
        },
        "delete-profile": {"type": "soft", "expiration-seconds": 3600},
    }
    status, body = call("POST", "/management/v1/warehouse", payload, ok=(201, 200))
    if status not in (200, 201):
        sys.exit(f"create warehouse failed: {status} {body}")
    print(f"warehouse '{WAREHOUSE}' created: {body}")


if __name__ == "__main__":
    main()
