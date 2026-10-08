from __future__ import annotations

import copy
import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import requests

from copilot_metrics_fabric.config import parse_config
from copilot_metrics_fabric.deployment import (
    TELEMETRY_HEADER,
    TELEMETRY_VALUE,
    DeploymentError,
    FabricClient,
    FabricDeployer,
    validate_assets,
)

ROOT = Path(__file__).parents[1]


def assert_secret_absent(rendered: str, secret: str) -> None:
    if secret in rendered:
        raise AssertionError("sensitive value appeared in diagnostic output")


@dataclass
class FakeToken:
    token: str = "test-token"


class FakeCredential:
    def get_token(self, scope: str) -> FakeToken:
        assert scope == "https://api.fabric.microsoft.com/.default"
        return FakeToken()


class FakeCredentialWithToken:
    def __init__(self, token: str) -> None:
        self._token = token

    def get_token(self, scope: str) -> FakeToken:
        assert scope == "https://api.fabric.microsoft.com/.default"
        return FakeToken(self._token)


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: Any = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.payload = {} if payload is None else payload
        self.headers = headers or {}

    def json(self) -> Any:
        return self.payload


class QueueTransport:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def request(self, method, url, *, headers, json, timeout):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": {
                    key: "[REDACTED]" if key.lower() == "authorization" else value
                    for key, value in headers.items()
                },
                "json": json,
                "timeout": timeout,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FabricRouter:
    def __init__(self, *, workspace_exists: bool = True) -> None:
        self.workspace = (
            {"id": "workspace-id", "displayName": "Metrics Workspace"}
            if workspace_exists
            else None
        )
        self.items: list[dict[str, str]] = []
        self.calls: list[dict[str, Any]] = []
        self.next_id = 1
        self.fail_update_number: int | None = None
        self.update_count = 0

    def request(self, method, url, *, headers, json, timeout):
        assert headers[TELEMETRY_HEADER] == TELEMETRY_VALUE
        path = url.split("/v1/", 1)[1]
        self.calls.append({"method": method, "path": path, "json": json})
        if method == "GET" and path == "workspaces":
            values = [self.workspace] if self.workspace else []
            return FakeResponse(200, {"value": values})
        if method == "POST" and path == "workspaces":
            self.workspace = {
                "id": "workspace-id",
                "displayName": json["displayName"],
            }
            return FakeResponse(201, self.workspace)
        if method == "GET" and "/items?type=" in path:
            item_type = path.rsplit("=", 1)[1]
            return FakeResponse(
                200,
                {"value": [item for item in self.items if item["type"] == item_type]},
            )
        if method == "GET" and "/lakehouses/" in path:
            return FakeResponse(
                200,
                {
                    "properties": {
                        "sqlEndpointProperties": {
                            "provisioningStatus": "Success",
                            "connectionString": (
                                "server.datawarehouse.fabric.microsoft.com"
                            ),
                        }
                    }
                },
            )
        if method == "POST" and path.endswith("/items"):
            item = {
                "id": f"item-{self.next_id}",
                "displayName": json["displayName"],
                "type": json["type"],
            }
            self.next_id += 1
            self.items.append(item)
            return FakeResponse(201, item)
        if method == "POST" and "/updateDefinition" in path:
            self.update_count += 1
            if self.update_count == self.fail_update_number:
                return FakeResponse(
                    400,
                    {"error": {"code": "InvalidDefinition", "message": "bad asset"}},
                    {"requestId": "request-1"},
                )
            return FakeResponse(200, {"status": "Succeeded"})
        raise AssertionError(f"unexpected request: {method} {path}")


def deployment_config(state_file: str, *, create_workspace: bool = False):
    return parse_config(
        {
            "schema_version": 1,
            "github": {
                "mode": "organization",
                "organizations": ["example-org"],
            },
            "fabric": {
                "workspace_name": "Metrics Workspace",
                "lakehouse_name": "Metrics Lakehouse",
                "create_workspace": create_workspace,
                "state_file": state_file,
            },
        }
    )


def environment_deployment_config(
    state_file: str, *, create_environment: bool = True
):
    return parse_config(
        {
            "schema_version": 1,
            "github": {
                "mode": "organization",
                "organizations": ["example-org"],
            },
            "fabric": {
                "workspace_name": "Metrics Workspace",
                "lakehouse_name": "Metrics Lakehouse",
                "environment_name": "Metrics Environment",
                "create_environment": create_environment,
                "state_file": state_file,
            },
        }
    )


def client(transport) -> FabricClient:
    return FabricClient(
        credential=FakeCredential(),
        transport=transport,
        sleep=lambda _: None,
        poll_interval=0,
    )


class BearerCheckingTransport:
    def __init__(self, token: str) -> None:
        self._expected_authorization_hash = hashlib.sha256(
            f"Bearer {token}".encode()
        ).digest()
        self.received_expected_header = False

    def request(self, method, url, *, headers, json, timeout):
        authorization = headers.get("Authorization")
        self.received_expected_header = (
            isinstance(authorization, str)
            and hashlib.sha256(authorization.encode()).digest()
            == self._expected_authorization_hash
        )
        return FakeResponse(200, {"value": []})


def test_validates_all_assets_and_notebook_structure_before_network() -> None:
    assets = validate_assets(ROOT)

    assert len(assets.notebooks) == 3
    assert assets.pipeline.name == "pipeline-content.json"
    assert assets.semantic_model.name.endswith("SemanticModel")
    assert assets.report.name.endswith("Report")


def test_discovery_plan_is_read_only(tmp_path: Path) -> None:
    router = FabricRouter()
    router.items.append(
        {
            "id": "lakehouse-id",
            "displayName": "Metrics Lakehouse",
            "type": "Lakehouse",
        }
    )
    deployer = FabricDeployer(
        deployment_config(str(tmp_path.name + "/state.json")),
        root=ROOT,
        client=client(router),
    )

    actions = deployer.plan()

    assert actions[0].operation == "update"
    assert all(call["method"] == "GET" for call in router.calls)
    assert not (ROOT / tmp_path.name / "state.json").exists()


def test_deploy_creates_workspace_and_schema_lakehouse(tmp_path: Path) -> None:
    router = FabricRouter(workspace_exists=False)
    state = f".pytest_cache/{tmp_path.name}-create-state.json"
    state_path = ROOT / state
    state_path.unlink(missing_ok=True)
    deployer = FabricDeployer(
        deployment_config(state, create_workspace=True),
        root=ROOT,
        client=client(router),
    )

    actions = deployer.deploy()

    workspace_call = next(
        call
        for call in router.calls
        if call["method"] == "POST" and call["path"] == "workspaces"
    )
    lakehouse_call = next(
        call
        for call in router.calls
        if call["method"] == "POST"
        and call["path"].endswith("/items")
        and call["json"]["type"] == "Lakehouse"
    )
    assert workspace_call["json"]["displayName"] == "Metrics Workspace"
    assert lakehouse_call["json"]["creationPayload"] == {"enableSchemas": True}
    notebook_updates = [
        call["path"]
        for call in router.calls
        if call["method"] == "POST"
        and "/updateDefinition" in call["path"]
        and any(
            item["id"] in call["path"]
            for item in router.items
            if item["type"] == "Notebook"
        )
    ]
    assert notebook_updates
    assert all("updateMetadata=true" not in path for path in notebook_updates)
    report_update = next(
        call["path"]
        for call in router.calls
        if call["method"] == "POST"
        and "/updateDefinition" in call["path"]
        and any(
            item["id"] in call["path"]
            for item in router.items
            if item["type"] == "Report"
        )
    )
    assert "updateMetadata=true" not in report_update
    assert any(action.item_type == "Report" for action in actions)
    state_path.unlink(missing_ok=True)


def test_client_retries_polls_lro_and_sends_telemetry_header() -> None:
    transport = QueueTransport(
        [
            FakeResponse(429, headers={"Retry-After": "0"}),
            FakeResponse(
                202,
                headers={"Location": "https://api.fabric.microsoft.com/v1/operations/1"},
            ),
            FakeResponse(200, {"status": "Running"}),
            FakeResponse(200, {"status": "Succeeded", "id": "created-id"}),
        ]
    )

    result = client(transport).request(
        "POST", "workspaces", body={"displayName": "x"}, expected=(202,)
    )

    assert result["id"] == "created-id"
    assert len(transport.calls) == 4
    assert all(
        call["headers"][TELEMETRY_HEADER] == TELEMETRY_VALUE
        for call in transport.calls
    )


def test_fabric_transport_receives_azure_access_token_bearer_header(caplog) -> None:
    token = "fabric-transport-token"
    transport = BearerCheckingTransport(token)
    fabric = FabricClient(
        credential=FakeCredentialWithToken(token),
        transport=transport,
        sleep=lambda _: None,
    )

    fabric.request("GET", "workspaces")

    assert transport.received_expected_header
    assert_secret_absent(repr(vars(transport)), token)
    assert_secret_absent(caplog.text, token)


def test_client_reports_sanitized_errors_and_transport_failures(caplog) -> None:
    token = "fabric-transport-token"
    transport = QueueTransport(
        [
            requests.ConnectionError("socket failed"),
            FakeResponse(
                400,
                {
                    "error": {
                        "code": "BadRequest",
                        "message": "token=very-sensitive invalid",
                    }
                },
                {"requestId": "abc"},
            ),
        ]
    )
    fabric = FabricClient(
        credential=FakeCredentialWithToken(token),
        transport=transport,
        sleep=lambda _: None,
        max_retries=1,
    )

    with pytest.raises(DeploymentError) as caught:
        fabric.request("GET", "workspaces", expected=(200,))

    message = str(caught.value)
    assert "BadRequest" in message
    assert_secret_absent(message, "very-sensitive")
    assert_secret_absent(message, token)
    assert_secret_absent(repr(transport.calls), token)
    assert_secret_absent(caplog.text, token)
    assert "request abc" in message


@pytest.mark.parametrize("status", ["Failed", "Cancelled"])
def test_lro_terminal_failures_are_sanitized(status: str) -> None:
    signed_url = "https://reports.test/file?signature=very-sensitive"
    transport = QueueTransport(
        [
            FakeResponse(
                202,
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/operations/1"
                    )
                },
            ),
            FakeResponse(
                200,
                {
                    "status": status,
                    "error": {
                        "code": "OperationFailed",
                        "message": (
                            f"download {signed_url} token=also-secret"
                        ),
                    },
                },
            ),
        ]
    )

    with pytest.raises(DeploymentError) as caught:
        client(transport).request("POST", "workspaces", expected=(202,))

    message = str(caught.value)
    assert "OperationFailed" in message
    assert_secret_absent(message, "very-sensitive")
    assert_secret_absent(message, "also-secret")


def test_lro_timeout_is_bounded() -> None:
    transport = QueueTransport(
        [
            FakeResponse(
                202,
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/operations/1"
                    )
                },
            ),
            FakeResponse(202, {"status": "Running"}),
            FakeResponse(202, {"status": "Running"}),
        ]
    )
    fabric = FabricClient(
        credential=FakeCredential(),
        transport=transport,
        sleep=lambda _: None,
        poll_interval=0,
        max_lro_polls=2,
    )

    with pytest.raises(DeploymentError, match="did not complete after 2 polls"):
        fabric.request("POST", "workspaces", expected=(202,))

    assert len(transport.calls) == 3


def test_partial_resume_skips_completed_definition(tmp_path: Path) -> None:
    state = f".pytest_cache/{tmp_path.name}-resume-state.json"
    state_path = ROOT / state
    state_path.unlink(missing_ok=True)
    router = FabricRouter()
    router.fail_update_number = 2
    deployer = FabricDeployer(
        deployment_config(state), root=ROOT, client=client(router)
    )

    with pytest.raises(DeploymentError):
        deployer.deploy()
    first_notebook_updates = router.update_count
    assert first_notebook_updates == 2

    router.fail_update_number = None
    resumed = FabricDeployer(
        deployment_config(state), root=ROOT, client=client(router)
    )
    actions = resumed.deploy()

    assert next(
        action
        for action in actions
        if action.item_type == "Notebook"
        and action.display_name == "ingest_bronze"
    ).operation == "skip"
    state_path.unlink(missing_ok=True)


def test_duplicate_display_names_are_explicit_errors(tmp_path: Path) -> None:
    router = FabricRouter()
    router.items.extend(
        [
            {
                "id": "one",
                "displayName": "Metrics Lakehouse",
                "type": "Lakehouse",
            },
            {
                "id": "two",
                "displayName": "Metrics Lakehouse",
                "type": "Lakehouse",
            },
        ]
    )
    deployer = FabricDeployer(
        deployment_config(f".pytest_cache/{tmp_path.name}-duplicate.json"),
        root=ROOT,
        client=client(router),
    )

    with pytest.raises(DeploymentError, match="multiple Lakehouse"):
        deployer.plan()


def _copy_deployment_assets(destination: Path) -> None:
    for relative in ("fabric", "assets"):
        shutil.copytree(ROOT / relative, destination / relative)


def _invalidate_notebook(root: Path) -> None:
    (root / "fabric/notebooks/ingest_bronze.ipynb").write_text(
        "{}", encoding="utf-8"
    )


def _invalidate_pipeline(root: Path) -> None:
    path = (
        root
        / "fabric/pipelines/copilot_metrics_orchestration.DataPipeline"
        / "pipeline-content.json"
    )
    path.write_text(
        '{"properties":{"activities":[],"parameters":{}}}',
        encoding="utf-8",
    )


def _invalidate_report_binding(root: Path) -> None:
    path = (
        root
        / "assets/powerbi/GitHubCopilotMetrics"
        / "GitHubCopilotMetrics.Report"
        / "definition.pbir"
    )
    path.write_text(
        '{"datasetReference":{"byPath":{"path":"../wrong"}}}',
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (_invalidate_notebook, "not nbformat 4"),
        (_invalidate_pipeline, "does not reference notebooks in order"),
        (_invalidate_report_binding, "semantic model reference is invalid"),
    ],
)
def test_invalid_fabric_definitions_fail_before_network(
    tmp_path: Path, mutate, message: str
) -> None:
    _copy_deployment_assets(tmp_path)
    mutate(tmp_path)
    router = FabricRouter()

    with pytest.raises(DeploymentError, match=message):
        FabricDeployer(
            deployment_config(".state.json"),
            root=tmp_path,
            client=client(router),
        )

    assert router.calls == []


class EnvironmentRouter(FabricRouter):
    def __init__(self, *, environment_exists: bool = False) -> None:
        super().__init__()
        if environment_exists:
            self.items.append(
                {
                    "id": "environment-id",
                    "displayName": "Metrics Environment",
                    "type": "Environment",
                }
            )
        self.settings = {
            "automaticLog": {"enabled": True},
            "pool": {"customizeComputeEnabled": False},
            "environment": {"name": "", "runtimeVersion": "2.0"},
            "job": {"sessionTimeoutInMinutes": 20},
        }
        self.publish_states = ["Success"]
        self.publish_calls = 0
        self.environment_updates = 0

    def request(self, method, url, *, headers, json, timeout):
        assert headers[TELEMETRY_HEADER] == TELEMETRY_VALUE
        path = url.split("/v1/", 1)[1]
        if method == "POST" and path == "workspaces/workspace-id/environments":
            item = {
                "id": "environment-id",
                "displayName": json["displayName"],
                "type": "Environment",
            }
            self.items.append(item)
            self.calls.append({"method": method, "path": path, "json": json})
            return FakeResponse(201, item)
        if method == "POST" and path.endswith(
            "/environments/environment-id/updateDefinition"
        ):
            self.environment_updates += 1
            self.calls.append({"method": method, "path": path, "json": json})
            return FakeResponse(200)
        if method == "POST" and path.endswith(
            "/environments/environment-id/staging/publish?beta=false"
        ):
            self.publish_calls += 1
            self.calls.append({"method": method, "path": path, "json": json})
            state = self.publish_states.pop(0) if self.publish_states else "Success"
            return FakeResponse(200, {"publishDetails": {"state": state}})
        if method == "GET" and path.endswith(
            "/environments/environment-id"
        ):
            self.calls.append({"method": method, "path": path, "json": json})
            state = self.publish_states.pop(0)
            return FakeResponse(
                200,
                {"properties": {"publishDetails": {"state": state}}},
            )
        if method == "GET" and path == "workspaces/workspace-id/spark/settings":
            self.calls.append({"method": method, "path": path, "json": json})
            return FakeResponse(200, copy.deepcopy(self.settings))
        if method == "PATCH" and path == "workspaces/workspace-id/spark/settings":
            self.calls.append({"method": method, "path": path, "json": json})
            self.settings["environment"] = copy.deepcopy(json["environment"])
            return FakeResponse(200, copy.deepcopy(self.settings))
        return super().request(
            method, url, headers=headers, json=json, timeout=timeout
        )


def _wheel_builder(tmp_path: Path, content: bytes = b"wheel"):
    def build(_root: Path) -> Path:
        wheel = tmp_path / "github_copilot_metrics_fabric-0.1.0-py3-none-any.whl"
        wheel.write_bytes(content)
        return wheel

    return build


def test_environment_create_publish_and_workspace_patch_preserves_settings(
    tmp_path: Path,
) -> None:
    router = EnvironmentRouter()
    state = f".pytest_cache/{tmp_path.name}-environment-create.json"
    state_path = ROOT / state
    state_path.unlink(missing_ok=True)
    original_settings = copy.deepcopy(router.settings)
    deployer = FabricDeployer(
        environment_deployment_config(state),
        root=ROOT,
        client=client(router),
        wheel_builder=_wheel_builder(tmp_path),
    )

    actions = deployer.deploy()

    create_call = next(
        call
        for call in router.calls
        if call["path"] == "workspaces/workspace-id/environments"
    )
    paths = {
        part["path"] for part in create_call["json"]["definition"]["parts"]
    }
    assert paths == {
        "Libraries/CustomLibraries/"
        "github_copilot_metrics_fabric-0.1.0-py3-none-any.whl",
        "Libraries/PublicLibraries/environment.yml",
        "Setting/Sparkcompute.yml",
    }
    patch = next(call for call in router.calls if call["method"] == "PATCH")
    assert patch["json"] == {
        "environment": {
            "name": "Metrics Environment",
            "runtimeVersion": "2.0",
        }
    }
    for key in ("automaticLog", "pool", "job"):
        assert router.settings[key] == original_settings[key]
    assert any(
        action.item_type == "Environment" and action.operation == "create"
        for action in actions
    )
    state_path.unlink(missing_ok=True)


def test_environment_reuse_is_content_hash_aware(tmp_path: Path) -> None:
    router = EnvironmentRouter()
    state = f".pytest_cache/{tmp_path.name}-environment-reuse.json"
    state_path = ROOT / state
    state_path.unlink(missing_ok=True)
    builder = _wheel_builder(tmp_path)
    FabricDeployer(
        environment_deployment_config(state),
        root=ROOT,
        client=client(router),
        wheel_builder=builder,
    ).deploy()
    publish_count = router.publish_calls

    actions = FabricDeployer(
        environment_deployment_config(state),
        root=ROOT,
        client=client(router),
        wheel_builder=builder,
    ).deploy()

    assert router.publish_calls == publish_count
    assert router.environment_updates == 0
    assert next(
        action for action in actions if action.item_type == "Environment"
    ).operation == "skip"
    state_path.unlink(missing_ok=True)


def test_environment_update_republishes_changed_wheel(tmp_path: Path) -> None:
    router = EnvironmentRouter()
    state = f".pytest_cache/{tmp_path.name}-environment-update.json"
    state_path = ROOT / state
    state_path.unlink(missing_ok=True)
    FabricDeployer(
        environment_deployment_config(state),
        root=ROOT,
        client=client(router),
        wheel_builder=_wheel_builder(tmp_path, b"first"),
    ).deploy()

    FabricDeployer(
        environment_deployment_config(state),
        root=ROOT,
        client=client(router),
        wheel_builder=_wheel_builder(tmp_path, b"second"),
    ).deploy()

    assert router.environment_updates == 1
    assert router.publish_calls == 2
    state_path.unlink(missing_ok=True)


def test_environment_publish_polls_to_success_and_reports_failure(
    tmp_path: Path,
) -> None:
    success_router = EnvironmentRouter(environment_exists=True)
    success_router.publish_states = ["Running", "Success"]
    success_state = f".pytest_cache/{tmp_path.name}-publish-success.json"
    success_path = ROOT / success_state
    success_path.unlink(missing_ok=True)
    FabricDeployer(
        environment_deployment_config(success_state),
        root=ROOT,
        client=client(success_router),
        wheel_builder=_wheel_builder(tmp_path),
    ).deploy()
    assert any(
        call["method"] == "GET"
        and call["path"].endswith("/environments/environment-id")
        for call in success_router.calls
    )
    success_path.unlink(missing_ok=True)

    failure_router = EnvironmentRouter(environment_exists=True)
    failure_router.publish_states = ["Running", "Failed"]
    failure_state = f".pytest_cache/{tmp_path.name}-publish-failure.json"
    failure_path = ROOT / failure_state
    failure_path.unlink(missing_ok=True)
    with pytest.raises(DeploymentError, match="publish failed"):
        FabricDeployer(
            environment_deployment_config(failure_state),
            root=ROOT,
            client=client(failure_router),
            wheel_builder=_wheel_builder(tmp_path),
        ).deploy()
    failure_path.unlink(missing_ok=True)


def test_environment_publish_retry_and_state_resume(tmp_path: Path) -> None:
    class RetryEnvironmentRouter(EnvironmentRouter):
        def __init__(self) -> None:
            super().__init__(environment_exists=True)
            self.throttle_once = True

        def request(self, method, url, *, headers, json, timeout):
            path = url.split("/v1/", 1)[1]
            if (
                method == "POST"
                and path.endswith("/staging/publish?beta=false")
                and self.throttle_once
            ):
                self.throttle_once = False
                self.calls.append(
                    {"method": method, "path": path, "json": json}
                )
                return FakeResponse(429, headers={"Retry-After": "0"})
            return super().request(
                method, url, headers=headers, json=json, timeout=timeout
            )

    router = RetryEnvironmentRouter()
    router.publish_states = ["Running", "Failed"]
    state = f".pytest_cache/{tmp_path.name}-environment-resume.json"
    state_path = ROOT / state
    state_path.unlink(missing_ok=True)
    config = environment_deployment_config(state)
    builder = _wheel_builder(tmp_path)
    with pytest.raises(DeploymentError):
        FabricDeployer(
            config, root=ROOT, client=client(router), wheel_builder=builder
        ).deploy()
    assert router.environment_updates == 1

    router.publish_states = ["Success"]
    FabricDeployer(
        config, root=ROOT, client=client(router), wheel_builder=builder
    ).deploy()

    assert router.environment_updates == 1
    publish_posts = [
        call
        for call in router.calls
        if call["method"] == "POST"
        and call["path"].endswith("/staging/publish?beta=false")
    ]
    assert len(publish_posts) == 3
    state_path.unlink(missing_ok=True)


def test_environment_plan_is_read_only_and_does_not_build_wheel(
    tmp_path: Path,
) -> None:
    router = EnvironmentRouter(environment_exists=True)

    def fail_build(_root: Path) -> Path:
        raise AssertionError("plan must not build the wheel")

    deployer = FabricDeployer(
        environment_deployment_config(
            f".pytest_cache/{tmp_path.name}-environment-plan.json"
        ),
        root=ROOT,
        client=client(router),
        wheel_builder=fail_build,
    )

    actions = deployer.plan()

    assert any(action.item_type == "Environment" for action in actions)
    assert all(call["method"] == "GET" for call in router.calls)
