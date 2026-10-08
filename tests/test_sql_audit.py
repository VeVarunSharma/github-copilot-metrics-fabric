from __future__ import annotations

import sys
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from copilot_metrics_fabric.config import parse_config
from copilot_metrics_fabric.deployment import DeploymentError
from copilot_metrics_fabric.sql_audit import (
    SQL_COPT_SS_ACCESS_TOKEN,
    FabricSqlAuditExecutor,
)


@dataclass
class Token:
    token: str = "sql-access-token"


class Credential:
    def get_token(self, scope: str) -> Token:
        assert scope == "https://database.windows.net/.default"
        return Token()


class FabricClient:
    credential = Credential()

    def request(self, method, path, *, expected):
        assert method == "GET"
        if path == "workspaces":
            return {"value": [{"id": "workspace", "displayName": "Metrics"}]}
        if path.endswith("items?type=Lakehouse"):
            return {"value": [{"id": "lakehouse", "displayName": "Lake"}]}
        if path.endswith("/lakehouses/lakehouse"):
            return {
                "properties": {
                    "sqlEndpointProperties": {
                        "provisioningStatus": "Success",
                        "connectionString": "tcp:server.fabric.microsoft.com,1433",
                    }
                }
            }
        raise AssertionError(path)


class Cursor:
    def __init__(self, value: int) -> None:
        self.value = value
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def execute(self, statement: str, *parameters: str):
        self.calls.append((statement, parameters))
        return self

    def fetchone(self):
        return (self.value,)


class Connection:
    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self._cursor


def config():
    return parse_config(
        {
            "schema_version": 1,
            "github": {
                "mode": "enterprise",
                "enterprise": "avocado-corp",
                "organizations": [],
            },
            "fabric": {
                "workspace_name": "Metrics",
                "lakehouse_name": "Lake",
            },
        }
    )


def test_executes_parameterized_scalar_with_access_token(monkeypatch) -> None:
    cursor = Cursor(1)
    captured: dict[str, Any] = {}

    def connect(connection_string, *, attrs_before):
        captured["connection_string"] = connection_string
        captured["attrs_before"] = attrs_before
        return Connection(cursor)

    fake = SimpleNamespace(connect=connect, Error=RuntimeError)
    monkeypatch.setitem(sys.modules, "pyodbc", fake)

    result = FabricSqlAuditExecutor(
        config(), FabricClient()
    ).scalar_bool("SELECT ?", ("value",))

    assert result is True
    assert "server.fabric.microsoft.com" in captured["connection_string"]
    assert SQL_COPT_SS_ACCESS_TOKEN in captured["attrs_before"]
    assert cursor.calls == [("SELECT ?", ("value",))]


def test_missing_audit_table_returns_false(monkeypatch) -> None:
    class PyodbcError(Exception):
        pass

    def connect(*_args, **_kwargs):
        raise PyodbcError("42S02 invalid object name")

    monkeypatch.setitem(
        sys.modules,
        "pyodbc",
        SimpleNamespace(connect=connect, Error=PyodbcError),
    )

    assert (
        FabricSqlAuditExecutor(config(), FabricClient()).scalar_bool(
            "SELECT 1", ()
        )
        is False
    )


def test_non_missing_sql_error_is_explicit(monkeypatch) -> None:
    class PyodbcError(Exception):
        pass

    def connect(*_args, **_kwargs):
        raise PyodbcError("login failed")

    monkeypatch.setitem(
        sys.modules,
        "pyodbc",
        SimpleNamespace(connect=connect, Error=PyodbcError),
    )

    with pytest.raises(DeploymentError, match="Fabric SQL audit query failed"):
        FabricSqlAuditExecutor(config(), FabricClient()).scalar_bool(
            "SELECT 1", ()
        )
