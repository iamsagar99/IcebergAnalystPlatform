#!/usr/bin/env python3
"""LOCAL-ONLY SIMULATION of Apache Ranger Admin's plugin-facing REST API.

The real Ranger plugin (embedded in kyuubi-spark-authz-shaded) polls these endpoints exactly as it would poll
Ranger Admin. We serve a ServicePolicies document assembled from:
  * ranger-servicedef-hive.json  - the Ranger 'hive' service definition
  * policies.json                - our access / masking / row-filter policies (editable; hot-reloaded on poll)
Production replaces this process with Ranger Admin (apache/ranger:2.8.0); nothing changes on the engine side
except ranger.plugin.spark.policy.rest.url. No authentication is implemented here on purpose: it is a stub.
"""
import argparse
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ARGS = None


def load_service_policies():
    with open(ARGS.servicedef) as f:
        servicedef = json.load(f)
    servicedef.pop("_source", None)
    # Ranger's shaded Jackson expects ISO-8601 dates; the Ranger-UI export format ("20190401-20:14:36.000-+0800") is not
    # parseable, so timestamps are dropped recursively (they carry no authorization meaning).
    def strip_dates(o):
        if isinstance(o, dict):
            return {k: strip_dates(v) for k, v in o.items() if k not in ("createTime", "updateTime")}
        if isinstance(o, list):
            return [strip_dates(x) for x in o]
        return o
    servicedef = strip_dates(servicedef)
    with open(ARGS.policies) as f:
        doc = json.load(f)
    policies = doc["policies"]
    version = int(doc.get("policyVersion", int(os.path.getmtime(ARGS.policies))))
    for i, p in enumerate(policies, start=1):
        p.setdefault("id", i)
        p.setdefault("guid", f"00000000-0000-0000-0000-{i:012d}")
        p.setdefault("isEnabled", True)
        p.setdefault("version", 1)
        p.setdefault("service", ARGS.service)
        p.setdefault("policyType", 0)
        p.setdefault("policyPriority", 0)
        p.setdefault("isAuditEnabled", True)
        p.setdefault("isDenyAllElse", False)
        p.setdefault("serviceType", "hive")
        for r in p.get("resources", {}).values():
            r.setdefault("isExcludes", False)
            r.setdefault("isRecursive", False)
    return {
        "serviceName": ARGS.service,
        "serviceId": 1,
        "policyVersion": version,
        "policyUpdateTime": int(time.time() * 1000),
        "policies": policies,
        "serviceDef": servicedef,
        "auditMode": "audit-default",
    }


class Handler(BaseHTTPRequestHandler):
    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):  # keep the log readable
        print(f"[ranger-admin-stub] {self.command} {self.path} -> {args[1] if len(args) > 1 else ''}", flush=True)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path == "/health":
            return self._json(200, {"status": "ok"})
        if "/policies/download/" in u.path:
            sp = load_service_policies()
            last = int(q.get("lastKnownVersion", ["-1"])[0])
            if last == sp["policyVersion"]:
                self.send_response(304)
                self.end_headers()
                return
            return self._json(200, sp)
        if "/roles/download/" in u.path:
            return self._json(200, {"serviceName": ARGS.service, "roleVersion": 1,
                                    "roleUpdateTime": int(time.time() * 1000), "rangerRoles": []})
        if "/tags/download/" in u.path or "/xusers/download/" in u.path or "/userstore/download/" in u.path:
            self.send_response(304)
            self.end_headers()
            return
        if u.path == "/service/plugins/policies" or u.path.startswith("/service/public/v2/api/policy"):
            return self._json(200, load_service_policies()["policies"])
        return self._json(404, {"error": f"not simulated: {u.path}"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        if "/plugins/info" in self.path:
            self.send_response(200)
            self.end_headers()
            return
        return self._json(404, {"error": f"not simulated: {self.path}"})

    do_PUT = do_POST


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=6080)
    ap.add_argument("--policies", required=True)
    ap.add_argument("--servicedef", required=True)
    ap.add_argument("--service", default="lakehouse_hive")
    ARGS = ap.parse_args()
    load_service_policies()  # fail fast on malformed JSON
    print(f"[ranger-admin-stub] serving policies for service '{ARGS.service}' on :{ARGS.port} (SIMULATION, not Ranger Admin)", flush=True)
    ThreadingHTTPServer(("0.0.0.0", ARGS.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
