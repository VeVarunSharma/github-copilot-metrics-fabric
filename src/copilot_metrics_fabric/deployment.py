"""Idempotent Microsoft Fabric deployment support."""

from __future__ import annotations

import base64
import copy
import csv
import hashlib
import importlib.metadata
import importlib.resources
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin

import requests
import yaml
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential

from copilot_metrics_fabric.config import AppConfig, ConfigurationError

FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1/"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
TELEMETRY_HEADER = "x-ms-fabric-skill"
TELEMETRY_VALUE = "e2e-medallion-architecture"
TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}
TERMINAL_SUCCESS = {"succeeded", "completed"}
TERMINAL_FAILURE = {"failed", "cancelled", "canceled"}
ENVIRONMENT_SUCCESS = {"success", "succeeded", "completed"}
ENVIRONMENT_FAILURE = {"failed", "cancelled", "canceled"}
ENVIRONMENT_RUNTIME_VERSION = "2.0"
SECRET_PATTERN = re.compile(
    r"(?i)(token|secret|password|credential|authorization|sig|signature)"
    r"\s*[:=]\s*\S+"
)
SIGNED_URL_PATTERN = re.compile(r"https://[^\s?]+\?[^\s]+", re.IGNORECASE)


class DeploymentError(RuntimeError):
    """Raised when validation or a Fabric operation fails."""


class FabricJobFailure(DeploymentError):
    """Raised when a Fabric item job reaches a failed terminal state."""

    def __init__(self, job_id: str, status: str, details: Any) -> None:
        self.job_id = job_id
        self.status = status
        super().__init__(
            f"Fabric job {job_id} {status}: {_safe_error(details)}"
        )


class ResponseLike(Protocol):
    status_code: int
    headers: Mapping[str, str]

    def json(self) -> Any: ...


class Transport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json: Any | None,
        timeout: float,
    ) -> ResponseLike: ...


@dataclass(frozen=True, slots=True)
class DefinitionPart:
    path: str
    content: bytes


@dataclass(frozen=True, slots=True)
class DeploymentAssets:
    notebooks: tuple[tuple[str, Path], ...]
    pipeline: Path
    semantic_model: Path
    report: Path


@dataclass(frozen=True, slots=True)
class PlanAction:
    operation: str
    item_type: str
    display_name: str

    def __str__(self) -> str:
        return f"{self.operation.upper():6} {self.item_type}: {self.display_name}"


class FabricClient:
    """Small Fabric REST client with authentication, retries, and LRO polling."""

    def __init__(
        self,
        *,
        credential: Any | None = None,
        transport: Transport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = 4,
        timeout: float = 30,
        poll_interval: float = 2,
        max_lro_polls: int = 900,
    ) -> None:
        self.credential = credential or DefaultAzureCredential()
        self.transport = transport or requests.Session()
        self.sleep = sleep
        self.max_retries = max_retries
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.max_lro_polls = max_lro_polls

    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        body: Any | None = None,
        expected: Iterable[int] = (200,),
        poll_lro: bool = True,
    ) -> Any:
        url = (
            path_or_url
            if path_or_url.startswith("https://")
            else urljoin(FABRIC_BASE_URL, path_or_url.lstrip("/"))
        )
        response = self._request_with_retry(method, url, body)
        if response.status_code not in set(expected):
            raise self._error(method, url, response)
        if response.status_code == 202 and poll_lro:
            return self._poll(response)
        if response.status_code == 204:
            return None
        return _response_json(response)

    def request_response(
        self,
        method: str,
        path_or_url: str,
        *,
        body: Any | None = None,
        expected: Iterable[int] = (200,),
    ) -> ResponseLike:
        """Return the raw response when callers need Fabric response headers."""
        url = (
            path_or_url
            if path_or_url.startswith("https://")
            else urljoin(FABRIC_BASE_URL, path_or_url.lstrip("/"))
        )
        response = self._request_with_retry(method, url, body)
        if response.status_code not in set(expected):
            raise self._error(method, url, response)
        return response

    def run_data_pipeline(
        self,
        workspace_id: str,
        pipeline_id: str,
        parameters: Iterable[Mapping[str, Any]],
        *,
        wait: bool = True,
    ) -> str | dict[str, Any]:
        """Start a Data Pipeline Execute job and optionally wait for completion."""
        response = self.request_response(
            "POST",
            (
                f"workspaces/{workspace_id}/items/{pipeline_id}/"
                "jobs/Pipeline/instances"
            ),
            body={
                "executionData": {
                    "parameters": {
                        str(parameter["name"]): parameter.get("value")
                        for parameter in parameters
                    }
                }
            },
            expected=(202,),
        )
        location = _response_location(response)
        job_id = _job_instance_id(location)
        if not wait:
            return job_id
        return self.wait_for_job(location, initial_response=response)

    def wait_for_job(
        self,
        location: str,
        *,
        initial_response: ResponseLike | None = None,
    ) -> dict[str, Any]:
        """Poll a Fabric item job instance until it reaches a terminal state."""
        delay = (
            _retry_delay(initial_response, 0)
            if initial_response is not None
            else self.poll_interval
        )
        for poll in range(self.max_lro_polls):
            self.sleep(delay)
            response = self.request_response("GET", location, expected=(200,))
            payload = _response_json(response)
            if not isinstance(payload, dict):
                raise DeploymentError(
                    f"Fabric job at {_safe_path(location)} returned invalid status"
                )
            status = _status(payload)
            if status == "completed":
                return payload
            if status in TERMINAL_FAILURE:
                details = payload.get("failureReason", payload)
                raise FabricJobFailure(
                    _job_instance_id(location), status, details
                )
            delay = _retry_delay(response, poll + 1)
        raise DeploymentError(
            f"Fabric job {_job_instance_id(location)} did not complete "
            f"after {self.max_lro_polls} polls"
        )

    def wait_for_data_pipeline_job(
        self,
        workspace_id: str,
        pipeline_id: str,
        job_id: str,
    ) -> dict[str, Any]:
        """Poll a previously submitted Data Pipeline job to completion."""
        return self.wait_for_job(
            f"workspaces/{workspace_id}/items/{pipeline_id}/"
            f"jobs/Pipeline/instances/{job_id}"
        )

    def _request_with_retry(
        self, method: str, url: str, body: Any | None
    ) -> ResponseLike:
        for attempt in range(self.max_retries + 1):
            try:
                token = self.credential.get_token(FABRIC_SCOPE).token
            except ClientAuthenticationError as error:
                raise DeploymentError(
                    "Azure Identity could not acquire a Fabric access token "
                    f"({type(error).__name__})"
                ) from error
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                TELEMETRY_HEADER: TELEMETRY_VALUE,
            }
            try:
                response = self.transport.request(
                    method,
                    url,
                    headers=headers,
                    json=body,
                    timeout=self.timeout,
                )
            except requests.RequestException as error:
                if attempt == self.max_retries:
                    raise DeploymentError(
                        f"Fabric {method.upper()} {_safe_path(url)} failed: "
                        f"{type(error).__name__}"
                    ) from error
                self.sleep(min(2**attempt, 30))
                continue
            if (
                response.status_code not in TRANSIENT_STATUS_CODES
                or attempt == self.max_retries
            ):
                return response
            self.sleep(_retry_delay(response, attempt))
        raise AssertionError("retry loop did not return")

    def _poll(self, initial: ResponseLike) -> Any:
        location = (
            initial.headers.get("Location")
            or initial.headers.get("Operation-Location")
            or initial.headers.get("operation-location")
        )
        if not location:
            payload = _response_json(initial)
            if _status(payload) in TERMINAL_SUCCESS:
                return payload
            raise DeploymentError(
                "Fabric returned 202 without an LRO polling location"
            )
        for _ in range(self.max_lro_polls):
            self.sleep(self.poll_interval)
            response = self._request_with_retry("GET", location, None)
            if response.status_code not in {200, 201, 202}:
                raise self._error("GET", location, response)
            payload = _response_json(response)
            status = _status(payload)
            if status in TERMINAL_SUCCESS:
                return payload
            if status in TERMINAL_FAILURE:
                raise DeploymentError(
                    f"Fabric operation failed at {_safe_path(location)}: "
                    f"{_safe_error(payload)}"
                )
            if response.status_code != 202 and not status:
                raise DeploymentError(
                    f"Fabric operation at {_safe_path(location)} returned "
                    "no status"
                )
        raise DeploymentError(
            f"Fabric operation at {_safe_path(location)} did not complete "
            f"after {self.max_lro_polls} polls"
        )

    @staticmethod
    def _error(method: str, url: str, response: ResponseLike) -> DeploymentError:
        request_id = (
            response.headers.get("requestId")
            or response.headers.get("x-ms-request-id")
            or "unavailable"
        )
        return DeploymentError(
            f"Fabric {method.upper()} {_safe_path(url)} returned "
            f"{response.status_code} (request {request_id}): "
            f"{_safe_error(_response_json(response))}"
        )


class FabricDeployer:
    """Validate, plan, and apply the repository's complete Fabric solution."""

    NOTEBOOKS = (
        ("ingest_bronze", Path("fabric/notebooks/ingest_bronze.ipynb")),
        ("build_silver", Path("fabric/notebooks/build_silver.ipynb")),
        ("build_gold", Path("fabric/notebooks/build_gold.ipynb")),
    )

    def __init__(
        self,
        config: AppConfig,
        *,
        root: Path,
        client: FabricClient,
        wheel_builder: Callable[[Path], Path] | None = None,
        state_root: Path | None = None,
        include_bi: bool = True,
    ) -> None:
        self.config = config
        self.root = root
        self.client = client
        self.wheel_builder = wheel_builder or build_project_wheel
        self.include_bi = include_bi
        self.assets = validate_assets(root)
        self.state_path = (state_root or root) / config.fabric.state_file
        self.state = self._load_state()

    def plan(self) -> list[PlanAction]:
        fabric = self.config.fabric
        workspace_name, lakehouse_name = self._required_names()
        workspace = self._find_workspace(workspace_name)
        if workspace is None:
            if not fabric.create_workspace:
                raise DeploymentError(
                    f"workspace {workspace_name!r} does not exist and "
                    "fabric.create_workspace is false"
                )
            actions = [
                PlanAction("create", "Workspace", workspace_name),
                PlanAction("create", "Lakehouse", lakehouse_name),
            ]
            if fabric.environment_name:
                if not fabric.create_environment:
                    raise DeploymentError(
                        "a new workspace cannot contain the configured "
                        "Environment while fabric.create_environment is false"
                    )
                actions.append(
                    PlanAction("create", "Environment", fabric.environment_name)
                )
            actions.extend(
                [
                    PlanAction("create", "Notebook", name)
                    for name, _ in self.assets.notebooks
                ]
            )
            actions.append(
                PlanAction("create", "DataPipeline", fabric.pipeline_name)
            )
            if self.include_bi:
                actions.extend(
                    [
                        PlanAction(
                            "create",
                            "SemanticModel",
                            fabric.semantic_model_name,
                        ),
                        PlanAction("create", "Report", fabric.report_name),
                    ]
                )
            return actions
        workspace_id = _id(workspace, "workspace")
        actions = [self._plan_item(workspace_id, "Lakehouse", lakehouse_name)]
        if fabric.environment_name:
            environment = self._find_item(
                workspace_id, "Environment", fabric.environment_name
            )
            if environment is None and not fabric.create_environment:
                raise DeploymentError(
                    f"Environment {fabric.environment_name!r} does not exist "
                    "and fabric.create_environment is false"
                )
            actions.append(
                PlanAction(
                    "update" if environment else "create",
                    "Environment",
                    fabric.environment_name,
                )
            )
            actions.append(
                PlanAction(
                    "update",
                    "WorkspaceSparkSettings",
                    fabric.environment_name,
                )
            )
        actions.extend(
            [
            *[
                self._plan_item(workspace_id, "Notebook", name)
                for name, _ in self.assets.notebooks
            ],
            self._plan_item(
                workspace_id, "DataPipeline", fabric.pipeline_name
            ),
            ]
        )
        if self.include_bi:
            actions.extend(
                [
                    self._plan_item(
                        workspace_id,
                        "SemanticModel",
                        fabric.semantic_model_name,
                    ),
                    self._plan_item(
                        workspace_id, "Report", fabric.report_name
                    ),
                ]
            )
        return actions

    def deploy(self) -> list[PlanAction]:
        fabric = self.config.fabric
        workspace_name, lakehouse_name = self._required_names()
        workspace = self._find_workspace(workspace_name)
        actions: list[PlanAction] = []
        if workspace is None:
            if not fabric.create_workspace:
                raise DeploymentError(
                    f"workspace {workspace_name!r} does not exist and "
                    "fabric.create_workspace is false"
                )
            body: dict[str, Any] = {"displayName": workspace_name}
            if fabric.capacity_id:
                body["capacityId"] = fabric.capacity_id
            created = self.client.request(
                "POST", "workspaces", body=body, expected=(201, 202)
            )
            workspace_id = _optional_result_id(created)
            if workspace_id is None:
                workspace = self._find_workspace(workspace_name)
                if workspace is None:
                    raise DeploymentError(
                        "Fabric completed workspace creation but the workspace "
                        "could not be discovered"
                    )
                workspace_id = _id(workspace, "workspace")
            actions.append(PlanAction("create", "Workspace", workspace_name))
        else:
            workspace_id = _id(workspace, "workspace")

        self._prepare_state(workspace_name, workspace_id)
        lakehouse, operation = self._ensure_item(
            workspace_id,
            "Lakehouse",
            lakehouse_name,
            creation_payload={"enableSchemas": True},
        )
        lakehouse_id = _id(lakehouse, "lakehouse")
        actions.append(PlanAction(operation, "Lakehouse", lakehouse_name))
        self._record("Lakehouse", lakehouse_name, lakehouse_id, None)

        if fabric.environment_name:
            actions.extend(
                self._deploy_environment(
                    workspace_id,
                    fabric.environment_name,
                    fabric.create_environment,
                )
            )

        notebook_ids: dict[str, str] = {}
        for name, path in self.assets.notebooks:
            item, operation = self._ensure_item(workspace_id, "Notebook", name)
            item_id = _id(item, f"notebook {name}")
            notebook_ids[name] = item_id
            parts = [
                DefinitionPart(
                    "notebook-content.ipynb",
                    _bind_notebook(
                        path,
                        workspace_id,
                        lakehouse_id,
                        lakehouse_name,
                    ),
                )
            ]
            operation = self._update_if_needed(
                workspace_id, "Notebook", name, item_id, parts, operation
            )
            actions.append(PlanAction(operation, "Notebook", name))

        pipeline_content = _pipeline_content(
            self.assets.pipeline, workspace_id, notebook_ids
        )
        pipeline, operation = self._ensure_item(
            workspace_id, "DataPipeline", fabric.pipeline_name
        )
        pipeline_id = _id(pipeline, "pipeline")
        operation = self._update_if_needed(
            workspace_id,
            "DataPipeline",
            fabric.pipeline_name,
            pipeline_id,
            [DefinitionPart("pipeline-content.json", pipeline_content)],
            operation,
        )
        actions.append(
            PlanAction(operation, "DataPipeline", fabric.pipeline_name)
        )

        if not self.include_bi:
            return actions

        lakehouse_details = self.client.request(
            "GET",
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
            expected=(200,),
        )
        sql_endpoint = _lakehouse_sql_endpoint(lakehouse_details)

        model, operation = self._ensure_item(
            workspace_id, "SemanticModel", fabric.semantic_model_name
        )
        model_id = _id(model, "semantic model")
        model_parts = _directory_parts(
            self.assets.semantic_model,
            replacements={
                "<WORKSPACE_NAME>": workspace_name,
                "<LAKEHOUSE_NAME>": lakehouse_name,
                "<SQL_ENDPOINT>": sql_endpoint,
                "<SQL_DATABASE>": lakehouse_name,
            },
        )
        operation = self._update_if_needed(
            workspace_id,
            "SemanticModel",
            fabric.semantic_model_name,
            model_id,
            model_parts,
            operation,
        )
        actions.append(
            PlanAction(operation, "SemanticModel", fabric.semantic_model_name)
        )

        report, operation = self._ensure_item(
            workspace_id, "Report", fabric.report_name
        )
        report_id = _id(report, "report")
        report_parts = _report_parts(
            self.assets.report,
            workspace_name,
            fabric.semantic_model_name,
            model_id,
        )
        operation = self._update_if_needed(
            workspace_id,
            "Report",
            fabric.report_name,
            report_id,
            report_parts,
            operation,
        )
        actions.append(PlanAction(operation, "Report", fabric.report_name))
        return actions

    def _deploy_environment(
        self,
        workspace_id: str,
        name: str,
        allow_create: bool,
    ) -> list[PlanAction]:
        wheel_path = self.wheel_builder(self.root)
        try:
            parts = _environment_parts(self.root, wheel_path)
        finally:
            _clean_wheel_build(wheel_path, self.root)
        digest = _parts_hash(parts)
        state_key = _state_key("Environment", name)
        previous = self.state.get("items", {}).get(state_key, {})
        environment = self._find_item(workspace_id, "Environment", name)
        operation = "update"
        if environment is None:
            if not allow_create:
                raise DeploymentError(
                    f"Environment {name!r} does not exist and "
                    "fabric.create_environment is false"
                )
            created = self.client.request(
                "POST",
                f"workspaces/{workspace_id}/environments",
                body={
                    "displayName": name,
                    "description": (
                        "GitHub Copilot metrics runtime and project package"
                    ),
                    **_definition_body(parts),
                },
                expected=(201, 202),
            )
            environment_id = _optional_result_id(created)
            if environment_id is None:
                environment = self._find_item(workspace_id, "Environment", name)
                if environment is None:
                    raise DeploymentError(
                        "Fabric completed Environment creation but "
                        f"{name!r} could not be discovered"
                    )
                environment_id = _id(environment, f"Environment {name}")
            operation = "create"
            self._record(
                "Environment",
                name,
                environment_id,
                digest,
                published_sha256=None,
            )
        else:
            environment_id = _id(environment, f"Environment {name}")
            if (
                previous.get("id") != environment_id
                or previous.get("sha256") != digest
            ):
                self.client.request(
                    "POST",
                    (
                        f"workspaces/{workspace_id}/environments/"
                        f"{environment_id}/updateDefinition"
                    ),
                    body=_definition_body(parts),
                    expected=(200, 202),
                )
                self._record(
                    "Environment",
                    name,
                    environment_id,
                    digest,
                    published_sha256=previous.get("published_sha256"),
                )
            elif previous.get("published_sha256") == digest:
                operation = "skip"

        if previous.get("published_sha256") != digest or operation == "create":
            self._publish_environment(workspace_id, environment_id)
            self._record(
                "Environment",
                name,
                environment_id,
                digest,
                published_sha256=digest,
            )
            operation = "create" if operation == "create" else "update"

        settings = self.client.request(
            "GET",
            f"workspaces/{workspace_id}/spark/settings",
            expected=(200,),
        )
        if not isinstance(settings, dict):
            raise DeploymentError("invalid workspace Spark settings response")
        desired_environment = {
            "name": name,
            "runtimeVersion": ENVIRONMENT_RUNTIME_VERSION,
        }
        settings_operation = "skip"
        if settings.get("environment") != desired_environment:
            self.client.request(
                "PATCH",
                f"workspaces/{workspace_id}/spark/settings",
                body={"environment": desired_environment},
                expected=(200,),
            )
            settings_operation = "update"
        return [
            PlanAction(operation, "Environment", name),
            PlanAction(settings_operation, "WorkspaceSparkSettings", name),
        ]

    def _publish_environment(
        self, workspace_id: str, environment_id: str
    ) -> None:
        path = (
            f"workspaces/{workspace_id}/environments/{environment_id}/"
            "staging/publish?beta=false"
        )
        result = self.client.request(
            "POST", path, expected=(200, 202)
        )
        state = _environment_publish_state(result)
        if state in ENVIRONMENT_SUCCESS:
            return
        if state in ENVIRONMENT_FAILURE:
            raise DeploymentError(
                f"Fabric Environment publish failed: {_safe_error(result)}"
            )
        item_path = (
            f"workspaces/{workspace_id}/environments/{environment_id}"
        )
        for _ in range(self.client.max_lro_polls):
            self.client.sleep(self.client.poll_interval)
            environment = self.client.request(
                "GET", item_path, expected=(200,)
            )
            state = _environment_publish_state(environment)
            if state in ENVIRONMENT_SUCCESS:
                return
            if state in ENVIRONMENT_FAILURE:
                raise DeploymentError(
                    "Fabric Environment publish failed: "
                    f"{_safe_error(environment)}"
                )
        raise DeploymentError(
            "Fabric Environment publish did not complete after "
            f"{self.client.max_lro_polls} polls"
        )

    def _required_names(self) -> tuple[str, str]:
        workspace = self.config.fabric.workspace_name
        lakehouse = self.config.fabric.lakehouse_name
        if not workspace or not lakehouse:
            raise ConfigurationError(
                "fabric.workspace_name and fabric.lakehouse_name are required "
                "for deployment"
            )
        return workspace, lakehouse

    def _find_workspace(self, name: str) -> dict[str, Any] | None:
        return self._find_unique(
            self._list("workspaces"), name, "Workspace"
        )

    def _find_item(
        self, workspace_id: str, item_type: str, name: str
    ) -> dict[str, Any] | None:
        items = self._list(
            f"workspaces/{workspace_id}/items?type={item_type}"
        )
        typed = [item for item in items if item.get("type") == item_type]
        return self._find_unique(typed, name, item_type)

    def _list(self, path: str) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        next_path: str | None = path
        while next_path:
            payload = self.client.request("GET", next_path, expected=(200,))
            if not isinstance(payload, dict):
                raise DeploymentError(f"invalid Fabric list response for {path}")
            page = payload.get("value", [])
            if not isinstance(page, list):
                raise DeploymentError(f"invalid Fabric list response for {path}")
            values.extend(item for item in page if isinstance(item, dict))
            next_path = payload.get("continuationUri")
            if not next_path:
                token = payload.get("continuationToken")
                separator = "&" if "?" in path else "?"
                next_path = (
                    f"{path}{separator}continuationToken={token}"
                    if token
                    else None
                )
        return values

    @staticmethod
    def _find_unique(
        items: list[dict[str, Any]], name: str, kind: str
    ) -> dict[str, Any] | None:
        matches = [item for item in items if item.get("displayName") == name]
        if len(matches) > 1:
            raise DeploymentError(
                f"multiple {kind} items have display name {name!r}"
            )
        return matches[0] if matches else None

    def _plan_item(
        self, workspace_id: str, item_type: str, name: str
    ) -> PlanAction:
        item = self._find_item(workspace_id, item_type, name)
        return PlanAction("update" if item else "create", item_type, name)

    def _ensure_item(
        self,
        workspace_id: str,
        item_type: str,
        name: str,
        *,
        creation_payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], str]:
        existing = self._find_item(workspace_id, item_type, name)
        if existing:
            return existing, "update"
        body: dict[str, Any] = {"displayName": name, "type": item_type}
        if creation_payload:
            body["creationPayload"] = creation_payload
        created = self.client.request(
            "POST",
            f"workspaces/{workspace_id}/items",
            body=body,
            expected=(201, 202),
        )
        item_id = _optional_result_id(created)
        if item_id is None:
            discovered = self._find_item(workspace_id, item_type, name)
            if discovered is None:
                raise DeploymentError(
                    f"Fabric completed {item_type} creation but {name!r} "
                    "could not be discovered"
                )
            item_id = _id(discovered, f"{item_type} {name}")
        return {"id": item_id, "displayName": name, "type": item_type}, "create"

    def _update_if_needed(
        self,
        workspace_id: str,
        item_type: str,
        name: str,
        item_id: str,
        parts: list[DefinitionPart],
        operation: str,
    ) -> str:
        digest = _parts_hash(parts)
        previous = self.state.get("items", {}).get(_state_key(item_type, name), {})
        if (
            operation == "update"
            and previous.get("id") == item_id
            and previous.get("sha256") == digest
        ):
            return "skip"
        update_metadata = any(part.path == ".platform" for part in parts)
        update_path = (
            f"workspaces/{workspace_id}/items/{item_id}/updateDefinition"
        )
        if update_metadata:
            update_path += "?updateMetadata=true"
        self.client.request(
            "POST",
            update_path,
            body=_definition_body(parts),
            expected=(200, 202),
        )
        self._record(item_type, name, item_id, digest)
        return operation

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"version": 1, "items": {}}
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError(
                f"cannot read deployment state {self.state_path}: {error}"
            ) from error
        if not isinstance(value, dict) or value.get("version") != 1:
            raise DeploymentError(
                f"unsupported deployment state in {self.state_path}"
            )
        value.setdefault("items", {})
        return value

    def _prepare_state(self, workspace_name: str, workspace_id: str) -> None:
        if (
            self.state.get("workspace_name") != workspace_name
            or self.state.get("workspace_id") != workspace_id
        ):
            self.state = {
                "version": 1,
                "workspace_name": workspace_name,
                "workspace_id": workspace_id,
                "items": {},
            }
            self._save_state()

    def _record(
        self,
        item_type: str,
        name: str,
        item_id: str,
        digest: str | None,
        **extra: Any,
    ) -> None:
        record = {
            "id": item_id,
            "sha256": digest,
        }
        record.update(extra)
        self.state.setdefault("items", {})[_state_key(item_type, name)] = record
        self._save_state()

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.state_path.with_suffix(self.state_path.suffix + ".new")
        temp.write_text(
            json.dumps(self.state, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temp.replace(self.state_path)


def resolve_asset_root(config_path: Path | None = None) -> Path:
    """Find source assets or the copy bundled in an installed wheel."""
    candidates: list[Path] = []
    if config_path is not None:
        resolved = config_path.resolve()
        candidates.extend([resolved.parent, *resolved.parents])
    cwd = Path.cwd()
    candidates.extend([cwd, *cwd.parents])
    for candidate in candidates:
        if (candidate / "fabric/notebooks").is_dir() and (
            candidate / "assets/powerbi"
        ).is_dir():
            return candidate
    bundled = importlib.resources.files("copilot_metrics_fabric").joinpath(
        "_deployment_assets"
    )
    bundled_path = Path(str(bundled))
    if (bundled_path / "fabric/notebooks").is_dir() and (
        bundled_path / "assets/powerbi"
    ).is_dir():
        return bundled_path
    raise DeploymentError(
        "deployment assets were not found beside the configuration, in a "
        "source checkout, or in the installed package"
    )


def build_project_wheel(root: Path) -> Path:
    """Build a reproducible project wheel, with an installed-package fallback."""
    output = (root if (root / "pyproject.toml").is_file() else Path.cwd()) / (
        ".fabric-build"
    )
    shutil.rmtree(output, ignore_errors=True)
    output.mkdir(parents=True, exist_ok=True)
    pyproject = root / "pyproject.toml"
    build_error = "source pyproject.toml is unavailable"
    if pyproject.is_file():
        environment = os.environ.copy()
        environment["SOURCE_DATE_EPOCH"] = "315532800"
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "build",
                    "--wheel",
                    "--outdir",
                    str(output),
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
        except OSError as error:
            completed = None
            build_error = f"{type(error).__name__}: {error}"
        else:
            build_error = _sanitize(completed.stderr or completed.stdout)
        if completed is not None and completed.returncode == 0:
            wheels = sorted(output.glob("*.whl"))
            if len(wheels) == 1:
                return wheels[0]
            build_error = "build did not produce exactly one wheel"
    try:
        return _build_installed_wheel(output)
    except (OSError, importlib.metadata.PackageNotFoundError) as error:
        shutil.rmtree(output, ignore_errors=True)
        raise DeploymentError(
            "could not build the project wheel from source or the installed "
            f"package ({build_error}; {type(error).__name__})"
        ) from error


def _build_installed_wheel(output: Path) -> Path:
    distribution = importlib.metadata.distribution(
        "github-copilot-metrics-fabric"
    )
    version = distribution.version
    normalized_name = "github_copilot_metrics_fabric"
    wheel = output / f"{normalized_name}-{version}-py3-none-any.whl"
    package_root = Path(__file__).parent
    dist_info = f"{normalized_name}-{version}.dist-info"
    records: list[tuple[str, str, str]] = []
    with zipfile.ZipFile(
        wheel, "w", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        for path in sorted(package_root.rglob("*")):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            relative = (
                Path("copilot_metrics_fabric")
                / path.relative_to(package_root)
            ).as_posix()
            content = path.read_bytes()
            _write_reproducible_zip_part(archive, relative, content)
            records.append((relative, "", ""))
        metadata = (
            "Metadata-Version: 2.3\n"
            "Name: github-copilot-metrics-fabric\n"
            f"Version: {version}\n"
            "Requires-Python: >=3.10\n"
        ).encode()
        wheel_metadata = (
            b"Wheel-Version: 1.0\n"
            b"Generator: github-copilot-metrics-fabric\n"
            b"Root-Is-Purelib: true\n"
            b"Tag: py3-none-any\n"
        )
        for relative, content in (
            (f"{dist_info}/METADATA", metadata),
            (f"{dist_info}/WHEEL", wheel_metadata),
        ):
            _write_reproducible_zip_part(archive, relative, content)
            records.append((relative, "", ""))
        record_path = f"{dist_info}/RECORD"
        stream = io.StringIO()
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerows([*records, (record_path, "", "")])
        _write_reproducible_zip_part(
            archive, record_path, stream.getvalue().encode()
        )
    return wheel


def _write_reproducible_zip_part(
    archive: zipfile.ZipFile, path: str, content: bytes
) -> None:
    info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    archive.writestr(info, content)


def _clean_wheel_build(wheel_path: Path, root: Path) -> None:
    source_build = root / ".fabric-build"
    cwd_build = Path.cwd() / ".fabric-build"
    try:
        build_directory = next(
            candidate
            for candidate in (source_build, cwd_build)
            if wheel_path.resolve().is_relative_to(candidate.resolve())
        )
    except StopIteration:
        return
    except AttributeError:
        for candidate in (source_build, cwd_build):
            try:
                wheel_path.resolve().relative_to(candidate.resolve())
            except ValueError:
                continue
            build_directory = candidate
            break
        else:
            return
    shutil.rmtree(build_directory, ignore_errors=True)


def _environment_parts(root: Path, wheel_path: Path) -> list[DefinitionPart]:
    del root
    environment_yml = yaml.safe_dump(
        {"dependencies": [{"pip": _runtime_dependencies()}]},
        sort_keys=False,
    ).encode()
    spark_compute = yaml.safe_dump(
        {
            "enable_native_execution_engine": False,
            "instance_pool_id": None,
            "driver_cores": 4,
            "driver_memory": "28g",
            "executor_cores": 4,
            "executor_memory": "28g",
            "dynamic_executor_allocation": {
                "enabled": True,
                "min_executors": 1,
                "max_executors": 2,
            },
            "spark_conf": {},
            "runtime_version": ENVIRONMENT_RUNTIME_VERSION,
        },
        sort_keys=False,
    ).encode()
    try:
        wheel_content = wheel_path.read_bytes()
    except OSError as error:
        raise DeploymentError(
            f"cannot read built wheel {wheel_path}: {error}"
        ) from error
    return [
        DefinitionPart(
            f"Libraries/CustomLibraries/{wheel_path.name}", wheel_content
        ),
        DefinitionPart(
            "Libraries/PublicLibraries/environment.yml", environment_yml
        ),
        DefinitionPart("Setting/Sparkcompute.yml", spark_compute),
    ]


def _runtime_dependencies() -> list[str]:
    requirements = importlib.metadata.requires(
        "github-copilot-metrics-fabric"
    )
    if not requirements:
        return [
            "azure-identity>=1.19,<2",
            "PyYAML>=6.0.2,<7",
            "requests>=2.32,<3",
        ]
    dependencies = []
    for requirement in requirements:
        base, _, marker = requirement.partition(";")
        if "extra ==" in marker or "platform_system == \"Windows\"" in marker:
            continue
        dependencies.append(base.strip())
    return sorted(dependencies, key=str.lower)


def validate_assets(root: Path) -> DeploymentAssets:
    """Validate all local definitions before any Fabric request is made."""
    notebooks = tuple((name, root / path) for name, path in FabricDeployer.NOTEBOOKS)
    pipeline = (
        root
        / "fabric/pipelines/copilot_metrics_orchestration.DataPipeline"
        / "pipeline-content.json"
    )
    semantic_model = (
        root
        / "assets/powerbi/GitHubCopilotMetrics"
        / "GitHubCopilotMetrics.SemanticModel"
    )
    report = (
        root
        / "assets/powerbi/GitHubCopilotMetrics"
        / "GitHubCopilotMetrics.Report"
    )
    required = [*(path for _, path in notebooks), pipeline, semantic_model, report]
    missing = [str(path.relative_to(root)) for path in required if not path.exists()]
    if missing:
        raise DeploymentError("missing deployment assets: " + ", ".join(missing))

    for name, path in notebooks:
        try:
            notebook = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError(f"invalid notebook {path}: {error}") from error
        if notebook.get("nbformat") != 4 or not isinstance(
            notebook.get("cells"), list
        ):
            raise DeploymentError(f"notebook {path} is not nbformat 4")
        for index, cell in enumerate(notebook["cells"]):
            if cell.get("cell_type") == "code" and (
                cell.get("outputs") != []
                or cell.get("execution_count", object()) is not None
            ):
                raise DeploymentError(
                    f"notebook {name} code cell {index} must have empty "
                    "outputs and null execution_count"
                )

    pipeline_value = _read_json(pipeline)
    activities = pipeline_value.get("properties", {}).get("activities", [])
    expected = ["Bronze ingestion", "Silver normalization", "Gold materialization"]
    if [item.get("name") for item in activities] != expected:
        raise DeploymentError("pipeline does not reference notebooks in order")
    for parameter in (
        "workspace_id",
        "bronze_notebook_id",
        "silver_notebook_id",
        "gold_notebook_id",
    ):
        if parameter not in pipeline_value.get("properties", {}).get(
            "parameters", {}
        ):
            raise DeploymentError(f"pipeline is missing parameter {parameter}")

    model_files = list((semantic_model / "definition").rglob("*"))
    if not (semantic_model / "definition.pbism").is_file() or not any(
        path.is_file() and path.suffix == ".tmdl" for path in model_files
    ):
        raise DeploymentError("semantic model is missing PBISM or TMDL assets")
    binding = _read_json(report / "definition.pbir")
    relative_model = binding.get("datasetReference", {}).get("byPath", {}).get(
        "path"
    )
    if relative_model != "../GitHubCopilotMetrics.SemanticModel":
        raise DeploymentError("report semantic model reference is invalid")
    if not (report / "definition/report.json").is_file():
        raise DeploymentError("report definition is incomplete")
    return DeploymentAssets(notebooks, pipeline, semantic_model, report)


def _bind_notebook(
    path: Path, workspace_id: str, lakehouse_id: str, lakehouse_name: str
) -> bytes:
    notebook = _read_json(path)
    metadata = notebook.setdefault("metadata", {})
    dependencies = metadata.setdefault("dependencies", {})
    dependencies["lakehouse"] = {
        "default_lakehouse": lakehouse_id,
        "default_lakehouse_name": lakehouse_name,
        "default_lakehouse_workspace_id": workspace_id,
    }
    return _json_bytes(notebook)


def _pipeline_content(
    path: Path, workspace_id: str, notebook_ids: Mapping[str, str]
) -> bytes:
    value = _read_json(path)
    parameters = value["properties"]["parameters"]
    parameters["workspace_id"]["defaultValue"] = workspace_id
    id_by_activity = {
        "Bronze ingestion": notebook_ids["ingest_bronze"],
        "Silver normalization": notebook_ids["build_silver"],
        "Gold materialization": notebook_ids["build_gold"],
    }
    parameter_by_activity = {
        "Bronze ingestion": "bronze_notebook_id",
        "Silver normalization": "silver_notebook_id",
        "Gold materialization": "gold_notebook_id",
    }
    for activity in value["properties"]["activities"]:
        notebook_id = id_by_activity[activity["name"]]
        parameters[parameter_by_activity[activity["name"]]][
            "defaultValue"
        ] = notebook_id
    return _json_bytes(value)


def _directory_parts(
    root: Path, replacements: Mapping[str, str] | None = None
) -> list[DefinitionPart]:
    parts = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        content = path.read_bytes()
        if replacements:
            text = content.decode("utf-8")
            for old, new in replacements.items():
                text = text.replace(old, new)
            content = text.encode("utf-8")
        parts.append(DefinitionPart(path.relative_to(root).as_posix(), content))
    return parts


def _report_parts(
    root: Path,
    workspace_name: str,
    model_name: str,
    model_id: str,
) -> list[DefinitionPart]:
    parts = _directory_parts(root)
    binding = {
        "$schema": (
            "https://developer.microsoft.com/json-schemas/fabric/item/report/"
            "definitionProperties/2.0.0/schema.json"
        ),
        "version": "4.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": (
                    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
                    f"{workspace_name};Initial Catalog={model_name};"
                    "Integrated Security=ClaimsToken"
                ),
                "pbiServiceModelId": model_id,
                "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                "connectionType": "pbiServiceXmlaStyleLive",
                "name": "EntityDataSource",
            }
        },
    }
    replacement = DefinitionPart("definition.pbir", _json_bytes(binding))
    return [replacement if part.path == replacement.path else part for part in parts]


def _definition_body(parts: list[DefinitionPart]) -> dict[str, Any]:
    definition: dict[str, Any] = {
            "parts": [
                {
                    "path": part.path,
                    "payload": base64.b64encode(part.content).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
                for part in parts
            ]
    }
    if any(part.path.endswith(".ipynb") for part in parts):
        definition["format"] = "ipynb"
    return {"definition": definition}


def _parts_hash(parts: list[DefinitionPart]) -> str:
    digest = hashlib.sha256()
    for part in sorted(parts, key=lambda value: value.path):
        digest.update(part.path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(part.content)
        digest.update(b"\0")
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DeploymentError(f"invalid JSON asset {path}: {error}") from error
    if not isinstance(value, dict):
        raise DeploymentError(f"JSON asset {path} must contain an object")
    return copy.deepcopy(value)


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, indent=1, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _retry_delay(response: ResponseLike, attempt: int) -> float:
    raw = response.headers.get("Retry-After")
    if raw:
        try:
            return min(max(float(raw), 0), 60)
        except ValueError:
            pass
    return min(2**attempt, 30)


def _response_json(response: ResponseLike) -> Any:
    try:
        return response.json()
    except (ValueError, TypeError):
        return {}


def _response_location(response: ResponseLike) -> str:
    location = (
        response.headers.get("Location")
        or response.headers.get("Operation-Location")
        or response.headers.get("operation-location")
    )
    if not location:
        raise DeploymentError(
            "Fabric returned 202 without a job instance location"
        )
    return location


def _job_instance_id(location: str) -> str:
    path = location.split("?", 1)[0].rstrip("/")
    value = path.rsplit("/", 1)[-1]
    if not value or value.lower() == "instances":
        raise DeploymentError(
            "Fabric job instance location does not contain a job id"
        )
    return value


def _status(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("status", "")).lower()


def _lakehouse_sql_endpoint(payload: Any) -> str:
    if not isinstance(payload, dict):
        raise DeploymentError("invalid Fabric Lakehouse response")
    properties = payload.get("properties", {})
    endpoint = (
        properties.get("sqlEndpointProperties", {})
        if isinstance(properties, dict)
        else {}
    )
    status = str(endpoint.get("provisioningStatus", "")).lower()
    connection_string = endpoint.get("connectionString")
    if status not in {"success", "succeeded"}:
        raise DeploymentError(
            "Fabric Lakehouse SQL endpoint is not provisioned successfully"
        )
    if not isinstance(connection_string, str) or not connection_string:
        raise DeploymentError(
            "Fabric Lakehouse SQL endpoint has no connection string"
        )
    return connection_string


def _environment_publish_state(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    properties = payload.get("properties", payload)
    if not isinstance(properties, dict):
        return ""
    details = properties.get("publishDetails", properties)
    if not isinstance(details, dict):
        return ""
    return str(details.get("state", "")).lower()


def _sanitize(value: str) -> str:
    sanitized = SIGNED_URL_PATTERN.sub(
        lambda match: match.group(0).split("?", 1)[0] + "?[REDACTED]",
        value,
    )
    return SECRET_PATTERN.sub(r"\1=[REDACTED]", sanitized)[:500]


def _safe_error(payload: Any) -> str:
    if not isinstance(payload, dict):
        return "no error details"
    error = payload.get("error", payload)
    if not isinstance(error, dict):
        return "no error details"
    code = str(error.get("errorCode") or error.get("code") or "unknown")
    message = str(error.get("message") or "no message")
    message = SIGNED_URL_PATTERN.sub(
        lambda match: match.group(0).split("?", 1)[0] + "?[REDACTED]",
        message,
    )
    message = SECRET_PATTERN.sub(r"\1=[REDACTED]", message)
    return f"{code}: {message[:500]}"


def _safe_path(url: str) -> str:
    return url.split("?", 1)[0].replace(FABRIC_BASE_URL.rstrip("/"), "")


def _id(value: Mapping[str, Any], description: str) -> str:
    item_id = value.get("id")
    if not isinstance(item_id, str) or not item_id:
        raise DeploymentError(f"Fabric {description} response did not include an id")
    return item_id


def _optional_result_id(value: Any) -> str | None:
    if isinstance(value, dict):
        candidate = value.get("id") or value.get("resourceId")
        if isinstance(candidate, str) and candidate:
            return candidate.rstrip("/").rsplit("/", 1)[-1]
        result = value.get("result")
        if isinstance(result, dict):
            candidate = result.get("id") or result.get("resourceId")
            if isinstance(candidate, str) and candidate:
                return candidate.rstrip("/").rsplit("/", 1)[-1]
    return None


def _state_key(item_type: str, name: str) -> str:
    return f"{item_type}:{name}"
