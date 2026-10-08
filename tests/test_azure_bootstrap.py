from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from copilot_metrics_fabric.azure_bootstrap import (
    KEY_VAULT_SECRETS_OFFICER,
    KEY_VAULT_SECRETS_USER,
    ActionKind,
    AzureBootstrapService,
    AzureCli,
    AzurePermissionError,
    CliResult,
    GitHubMetricsTokenValidator,
    GitHubTokenValidationError,
)
from copilot_metrics_fabric.config import parse_config

SUBSCRIPTION = "00000000-0000-0000-0000-000000000000"
TOKEN = "github_pat_sensitive_test_value"


def bootstrap_config(
    *, create: bool = True, runtime_principal_id: str | None = None
):
    return parse_config(
        {
            "schema_version": 1,
            "github": {
                "mode": "organization",
                "organizations": ["example-org"],
            },
            "azure": {
                "subscription_id": SUBSCRIPTION,
                "resource_group_name": "metrics-rg",
                "location": "eastus2",
                "create_resource_group": create,
                "key_vault_name": "metrics-kv-123",
                "create_key_vault": create,
                "github_token_secret_name": "github-token",
                "fabric_runtime_principal_id": runtime_principal_id,
                "fabric_runtime_principal_type": "ServicePrincipal",
            },
        }
    )


class AzureRouter:
    def __init__(
        self,
        *,
        principal_type: str = "user",
        resources_exist: bool = False,
        role_exists: bool = False,
        permission_failure: bool = False,
    ) -> None:
        self.principal_type = principal_type
        self.resource_group = resources_exist
        self.vault = resources_exist
        self.roles: set[tuple[str, str]] = set()
        if role_exists:
            self.roles.update(
                {
                    ("user-object-id", KEY_VAULT_SECRETS_OFFICER),
                    ("user-object-id", KEY_VAULT_SECRETS_USER),
                    ("sp-object-id", KEY_VAULT_SECRETS_OFFICER),
                    ("sp-object-id", KEY_VAULT_SECRETS_USER),
                }
            )
        self.permission_failure = permission_failure
        self.calls: list[tuple[str, ...]] = []
        self.secret_value: str | None = None

    def __call__(self, command, *, input, timeout):
        assert input is None
        assert timeout > 0
        args = tuple(command[1:])
        self.calls.append(tuple(command))
        if args[:2] == ("version", "--output"):
            return result({"azure-cli": "2.70.0"})
        if args[:2] == ("account", "set"):
            return CliResult(0)
        if args[:2] == ("account", "show"):
            return result(
                {
                    "id": SUBSCRIPTION,
                    "user": {
                        "type": self.principal_type,
                        "name": "client-id",
                    },
                }
            )
        if args[:4] == ("ad", "signed-in-user", "show", "--output"):
            return result({"id": "user-object-id"})
        if args[:3] == ("ad", "sp", "show"):
            return result({"id": "sp-object-id"})
        if args[:2] == ("group", "show"):
            if self.permission_failure:
                return CliResult(1, stderr="AuthorizationFailed")
            if not self.resource_group:
                return CliResult(1, stderr="ResourceGroupNotFound")
            return result({"id": "/subscriptions/sub/resourceGroups/metrics-rg"})
        if args[:2] == ("group", "create"):
            self.resource_group = True
            return result({"id": "/subscriptions/sub/resourceGroups/metrics-rg"})
        if args[:2] == ("keyvault", "show"):
            if not self.vault:
                return CliResult(1, stderr="VaultNotFound")
            return result(vault_payload())
        if args[:2] == ("keyvault", "create"):
            self.vault = True
            return result(vault_payload())
        if args[:3] == ("role", "assignment", "list"):
            object_id = args[args.index("--assignee-object-id") + 1]
            role = args[args.index("--role") + 1]
            return result(
                [{"id": "assignment-id"}]
                if (object_id, role) in self.roles
                else []
            )
        if args[:3] == ("role", "assignment", "create"):
            object_id = args[args.index("--assignee-object-id") + 1]
            role = args[args.index("--role") + 1]
            self.roles.add((object_id, role))
            return CliResult(0)
        if args[:3] == ("keyvault", "secret", "set"):
            path = Path(args[args.index("--file") + 1])
            self.secret_value = path.read_text(encoding="utf-8")
            return CliResult(0)
        raise AssertionError(f"unexpected Azure CLI command: {args}")


def result(payload: Any) -> CliResult:
    return CliResult(0, json.dumps(payload))


def vault_payload() -> dict[str, Any]:
    return {
        "id": (
            f"/subscriptions/{SUBSCRIPTION}/resourceGroups/metrics-rg"
            "/providers/Microsoft.KeyVault/vaults/metrics-kv-123"
        ),
        "properties": {"enableRbacAuthorization": True},
    }


def service(router: AzureRouter, tmp_path: Path, *, create: bool = True):
    cli = AzureCli(
        executable="az",
        executor=router,
        secret_directory=tmp_path,
    )
    endpoints: list[str] = []

    def validate(endpoint: str, token: str) -> None:
        assert token == TOKEN
        endpoints.append(endpoint)

    return (
        AzureBootstrapService(
            bootstrap_config(create=create),
            cli=cli,
            token_validator=validate,
            today=lambda: date(2026, 10, 3),
        ),
        endpoints,
    )


def test_assigns_reader_to_explicit_fabric_runtime_identity(
    tmp_path: Path,
) -> None:
    runtime_id = "11111111-1111-1111-1111-111111111111"
    router = AzureRouter(resources_exist=True)
    cli = AzureCli(
        executable="az", executor=router, secret_directory=tmp_path
    )
    bootstrap = AzureBootstrapService(
        bootstrap_config(
            create=False, runtime_principal_id=runtime_id
        ),
        cli=cli,
        token_validator=lambda _endpoint, _token: None,
        today=lambda: date(2026, 10, 3),
    )

    bootstrap.apply(lambda: TOKEN)

    assert (runtime_id, KEY_VAULT_SECRETS_USER) in router.roles
    assert ("user-object-id", KEY_VAULT_SECRETS_OFFICER) in router.roles


def test_apply_creates_resources_assigns_role_and_writes_secret(
    tmp_path: Path,
) -> None:
    router = AzureRouter()
    bootstrap, endpoints = service(router, tmp_path)

    plan = bootstrap.apply(lambda: TOKEN)

    assert router.resource_group and router.vault
    assert ("user-object-id", KEY_VAULT_SECRETS_OFFICER) in router.roles
    assert ("user-object-id", KEY_VAULT_SECRETS_USER) in router.roles
    assert router.secret_value == TOKEN
    assert endpoints == [
        "https://api.github.com/orgs/example-org/copilot/metrics/reports/"
        "organization-1-day?day=2026-10-02"
    ]
    assert any(action.kind is ActionKind.CREATE for action in plan.actions)
    assert any(action.kind is ActionKind.ASSIGN for action in plan.actions)
    assert not list(tmp_path.glob(".ghcp-secret-*"))


def test_apply_reuses_resources_and_existing_role(tmp_path: Path) -> None:
    router = AzureRouter(resources_exist=True, role_exists=True)
    bootstrap, _ = service(router, tmp_path, create=False)

    plan = bootstrap.apply(lambda: TOKEN)

    assert sum(action.kind is ActionKind.REUSE for action in plan.actions) == 4
    assert not any(
        call[1:4] == ("role", "assignment", "create") for call in router.calls
    )


def test_explicitly_selects_configured_subscription(tmp_path: Path) -> None:
    router = AzureRouter(resources_exist=True, role_exists=True)
    bootstrap, _ = service(router, tmp_path, create=False)

    bootstrap.apply(lambda: TOKEN)

    assert ("az", "account", "set", "--subscription", SUBSCRIPTION) in router.calls
    scoped = [
        call
        for call in router.calls
        if call[1] in {"group", "keyvault", "role"}
    ]
    assert all(
        "--subscription" in call and SUBSCRIPTION in call for call in scoped
    )


@pytest.mark.parametrize(
    ("principal_type", "expected_command", "object_id", "assignment_type"),
    [
        ("user", ("ad", "signed-in-user", "show"), "user-object-id", "User"),
        (
            "servicePrincipal",
            ("ad", "sp", "show"),
            "sp-object-id",
            "ServicePrincipal",
        ),
    ],
)
def test_resolves_user_and_service_principal_identities(
    tmp_path: Path,
    principal_type: str,
    expected_command: tuple[str, ...],
    object_id: str,
    assignment_type: str,
) -> None:
    router = AzureRouter(
        principal_type=principal_type,
        resources_exist=True,
    )
    bootstrap, _ = service(router, tmp_path, create=False)

    bootstrap.apply(lambda: TOKEN)

    assert any(
        call[1 : 1 + len(expected_command)] == expected_command
        for call in router.calls
    )
    creates = [
        call
        for call in router.calls
        if call[1:4] == ("role", "assignment", "create")
    ]
    assert len(creates) == 2
    assert all(
        create[create.index("--assignee-object-id") + 1] == object_id
        for create in creates
    )
    assert all(
        create[create.index("--assignee-principal-type") + 1]
        == assignment_type
        for create in creates
    )


def test_permission_failures_are_typed_and_redacted(tmp_path: Path) -> None:
    router = AzureRouter(permission_failure=True)
    bootstrap, _ = service(router, tmp_path)

    with pytest.raises(AzurePermissionError) as captured:
        bootstrap.plan(lambda: TOKEN)

    assert TOKEN not in str(captured.value)
    assert TOKEN not in repr(captured.value)


class FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class FakeSession:
    def __init__(self, status_code: int, expected_token: str | None = None) -> None:
        self.status_code = status_code
        self.expected_authorization_hash = (
            hashlib.sha256(f"Bearer {expected_token}".encode()).digest()
            if expected_token is not None
            else None
        )
        self.received_expected_bearer = False
        self.calls: list[dict[str, Any]] = []

    def get(self, url, **kwargs):
        headers = kwargs["headers"]
        if self.expected_authorization_hash is not None:
            authorization = headers.get("Authorization")
            self.received_expected_bearer = (
                isinstance(authorization, str)
                and hashlib.sha256(authorization.encode()).digest()
                == self.expected_authorization_hash
            )
        recorded = {
            **kwargs,
            "headers": {
                key: "[REDACTED]" if key.lower() == "authorization" else value
                for key, value in headers.items()
            },
        }
        self.calls.append({"url": url, **recorded})
        return FakeResponse(self.status_code)


@pytest.mark.parametrize("status", [200, 204])
def test_token_validation_accepts_metrics_success(status: int) -> None:
    session = FakeSession(status, expected_token=TOKEN)
    validator = GitHubMetricsTokenValidator(session=session)

    validator("https://api.github.com/metrics", TOKEN)
    assert session.received_expected_bearer
    assert session.calls[0]["headers"]["Authorization"] == "[REDACTED]"
    assert TOKEN not in repr(session.calls)
    assert TOKEN not in repr(vars(session))
    assert session.calls[0]["allow_redirects"] is False


def test_token_validation_failure_does_not_disclose_token() -> None:
    validator = GitHubMetricsTokenValidator(session=FakeSession(403))

    with pytest.raises(GitHubTokenValidationError) as captured:
        validator("https://api.github.com/metrics", TOKEN)

    assert TOKEN not in str(captured.value)
    assert TOKEN not in repr(captured.value)


def test_secret_never_appears_in_argv_actions_or_errors(tmp_path: Path) -> None:
    router = AzureRouter(resources_exist=True)
    bootstrap, _ = service(router, tmp_path, create=False)

    plan = bootstrap.apply(lambda: TOKEN)

    rendered_calls = repr(router.calls)
    assert TOKEN not in rendered_calls
    assert TOKEN not in repr(plan)
    secret_call = next(
        call
        for call in router.calls
        if call[1:4] == ("keyvault", "secret", "set")
    )
    assert "--value" not in secret_call
    assert "--file" in secret_call


def test_dry_run_performs_zero_cloud_writes(tmp_path: Path) -> None:
    router = AzureRouter()
    bootstrap, _ = service(router, tmp_path)

    plan = bootstrap.plan(lambda: TOKEN)

    mutating = {
        ("account", "set"),
        ("group", "create"),
        ("keyvault", "create"),
        ("role", "assignment", "create"),
        ("keyvault", "secret", "set"),
    }
    assert not any(
        any(call[1 : 1 + len(prefix)] == prefix for prefix in mutating)
        for call in router.calls
    )
    assert router.secret_value is None
    assert any(action.write for action in plan.actions)
