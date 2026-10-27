"""Shared fixtures: engine clients for Trino and Kyuubi, SQL file parsing, expected values.

Runs inside the tools container (docker compose run --rm tools pytest ...). Host names / passwords come from the
container environment (infrastructure/.env). Tests never mark anything as passed without executing it.
"""
import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql"

USERS = {
    "admin": ("admin", os.environ.get("ADMIN_PASSWORD", "")),
    "etl": ("etl", os.environ.get("ETL_PASSWORD", "")),
    "analyst": ("analyst1", os.environ.get("ANALYST_PASSWORD", "")),
    "analyst2": ("analyst2", os.environ.get("ANALYST_PASSWORD", "")),
    "restricted": ("restricted1", os.environ.get("RESTRICTED_PASSWORD", "")),
    "bench": ("bench", os.environ.get("BENCH_PASSWORD", "")),
}


def load_named_queries(path: Path) -> dict:
    """Parse '-- name: id' blocks from a .sql file into {id: sql}."""
    out, name, buf = {}, None, []
    for line in path.read_text().splitlines():
        m = re.match(r"^--\s*name:\s*(\S+)", line)
        if m:
            if name:
                out[name] = "\n".join(buf).strip().rstrip(";")
            name, buf = m.group(1), []
        elif name is not None and not line.strip().startswith("--"):
            buf.append(line)
    if name:
        out[name] = "\n".join(buf).strip().rstrip(";")
    return out


@pytest.fixture(scope="session")
def sample_queries():
    return load_named_queries(SQL_DIR / "sample_queries.sql")


@pytest.fixture(scope="session")
def validation_queries():
    return load_named_queries(SQL_DIR / "validation_queries.sql")


@pytest.fixture(scope="session")
def expected():
    return json.loads((ROOT / "tests" / "expected.json").read_text())


# ------------------------------------------------------------------------------------------------ engine clients
class EngineError(Exception):
    pass


class TrinoClient:
    name = "trino"

    def __init__(self, user, password):
        import trino
        from trino.auth import BasicAuthentication
        self.conn = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "trino"), port=int(os.environ.get("TRINO_PORT", "8443")),
            user=user, auth=BasicAuthentication(user, password), http_scheme="https", verify=False, catalog="lakehouse",
            source="pytest")
        self.user = user

    def query(self, sql):
        from trino.exceptions import TrinoUserError
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        except TrinoUserError as e:
            raise EngineError(str(e)) from e
        finally:
            cur.close()

    def close(self):
        self.conn.close()


class KyuubiClient:
    name = "kyuubi"

    def __init__(self, user, password, conf=None):
        from pyhive import hive
        self.conn = hive.connect(
            host=os.environ.get("KYUUBI_HOST", "kyuubi"), port=int(os.environ.get("KYUUBI_PORT", "10009")),
            username=user, password=password, auth="CUSTOM", database="gold",  # PyHive issues USE <db>; lakehouse has no "default"
            configuration=conf or {})
        self.user = user

    def query(self, sql):
        from pyhive.exc import OperationalError
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return []
        except OperationalError as e:
            raise EngineError(str(e)) from e
        finally:
            cur.close()

    def close(self):
        self.conn.close()


ENGINES = {"trino": TrinoClient, "kyuubi": KyuubiClient}


def connect(engine, role, conf=None):
    user, pw = USERS[role]
    if engine == "kyuubi":
        return KyuubiClient(user, pw, conf)
    return TrinoClient(user, pw)


def pytest_addoption(parser):
    parser.addoption("--engine", action="store", default=os.environ.get("ENGINE", "both"),
                     help="trino | kyuubi | both")


def selected_engines(config):
    e = config.getoption("--engine")
    return ["trino", "kyuubi"] if e == "both" else [e]


def pytest_generate_tests(metafunc):
    if "engine" in metafunc.fixturenames:
        metafunc.parametrize("engine", selected_engines(metafunc.config), scope="session")
