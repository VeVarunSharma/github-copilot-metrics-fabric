"""Fabric Lakehouse SQL access for durable bootstrap audit checks."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any

from copilot_metrics_fabric.config import AppConfig, ConfigurationError
from copilot_metrics_fabric.deployment import DeploymentError, FabricClient

SQL_SCOPE = "https://database.windows.net/.default"
SQL_COPT_SS_ACCESS_TOKEN = 1256


@dataclass(slots=True)
class FabricSqlAuditExecutor:
    """Execute scalar queries through the Lakehouse SQL endpoint."""

    config: AppConfig
    client: FabricClient
    connect_timeout: int = 30

    def scalar_bool(
        self, statement: str, parameters: tuple[str, ...]
    ) -> bool:
        pyodbc = _load_pyodbc()
        server, database = self._connection_target()
        token = self.client.credential.get_token(SQL_SCOPE).token
        encoded = token.encode("utf-16-le")
        access_token = struct.pack(f"<I{len(encoded)}s", len(encoded), encoded)
        connection_string = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={server};Database={database};"
            "Encrypt=yes;TrustServerCertificate=no;"
            f"Connection Timeout={self.connect_timeout};"
        )
        try:
            with pyodbc.connect(
                connection_string,
                attrs_before={SQL_COPT_SS_ACCESS_TOKEN: access_token},
            ) as connection:
                row = connection.cursor().execute(
                    statement, *parameters
                ).fetchone()
        except pyodbc.Error as error:
            if _is_missing_audit_table(error):
                return False
            raise DeploymentError(
                "Fabric SQL audit query failed; verify SQL endpoint "
                "provisioning, ODBC Driver 18, and caller permissions"
            ) from error
        return bool(row and row[0])

    def _connection_target(self) -> tuple[str, str]:
        workspace_name = self.config.fabric.workspace_name
        lakehouse_name = self.config.fabric.lakehouse_name
        if not workspace_name or not lakehouse_name:
            raise ConfigurationError(
                "fabric.workspace_name and fabric.lakehouse_name are required"
            )
        workspace = _unique_named(
            _values(
                self.client.request("GET", "workspaces", expected=(200,))
            ),
            workspace_name,
            "workspace",
        )
        workspace_id = _required_id(workspace, "workspace")
        lakehouse = _unique_named(
            _values(
                self.client.request(
                    "GET",
                    f"workspaces/{workspace_id}/items?type=Lakehouse",
                    expected=(200,),
                )
            ),
            lakehouse_name,
            "lakehouse",
        )
        lakehouse_id = _required_id(lakehouse, "lakehouse")
        details = self.client.request(
            "GET",
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
            expected=(200,),
        )
        if not isinstance(details, dict):
            raise DeploymentError("invalid Fabric Lakehouse response")
        properties = details.get("properties", {})
        sql_properties = (
            properties.get("sqlEndpointProperties", {})
            if isinstance(properties, dict)
            else {}
        )
        status = str(sql_properties.get("provisioningStatus", "")).lower()
        connection_string = sql_properties.get("connectionString")
        if status not in {"success", "succeeded"}:
            raise DeploymentError(
                "Fabric Lakehouse SQL endpoint is not provisioned successfully"
            )
        if not isinstance(connection_string, str) or not connection_string:
            raise DeploymentError(
                "Fabric Lakehouse SQL endpoint has no connection string"
            )
        server = connection_string.removeprefix("tcp:").split(",", 1)[0]
        return server, lakehouse_name


def _load_pyodbc() -> Any:
    try:
        import pyodbc
    except ImportError as error:
        raise DeploymentError(
            "pyodbc and Microsoft ODBC Driver 18 for SQL Server are required "
            "for durable Fabric audit queries"
        ) from error
    return pyodbc


def _values(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
        raise DeploymentError("invalid Fabric list response")
    return [item for item in payload["value"] if isinstance(item, dict)]


def _unique_named(
    values: list[dict[str, Any]], name: str, resource_type: str
) -> dict[str, Any]:
    matches = [value for value in values if value.get("displayName") == name]
    if len(matches) != 1:
        raise DeploymentError(
            f"expected exactly one {resource_type} named {name!r}"
        )
    return matches[0]


def _required_id(value: dict[str, Any], resource_type: str) -> str:
    identifier = value.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise DeploymentError(f"Fabric {resource_type} response is missing an id")
    return identifier


def _is_missing_audit_table(error: Exception) -> bool:
    text = " ".join(str(part) for part in getattr(error, "args", (error,)))
    lowered = text.lower()
    return "42s02" in lowered or "invalid object name" in lowered
