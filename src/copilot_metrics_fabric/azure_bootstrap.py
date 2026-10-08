"""Read-only planning and idempotent Azure bootstrap operations."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, urlencode

import requests

from copilot_metrics_fabric.config import AppConfig
from copilot_metrics_fabric.github_client import API_VERSION

KEY_VAULT_SECRETS_USER = "Key Vault Secrets User"
KEY_VAULT_SECRETS_OFFICER = "Key Vault Secrets Officer"
MINIMUM_AZURE_CLI_VERSION = (2, 61, 0)


class AzureBootstrapError(RuntimeError):
    """Base exception for safe-to-display bootstrap failures."""


class AzureCliUnavailableError(AzureBootstrapError):
    """Raised when Azure CLI cannot be found or is too old."""


class AzureAuthenticationError(AzureBootstrapError):
    """Raised when Azure CLI has no authenticated account."""


class AzurePermissionError(AzureBootstrapError):
    """Raised when Azure denies an operation."""


class AzureResourceError(AzureBootstrapError):
    """Raised when a configured Azure resource is invalid or unavailable."""


class GitHubTokenValidationError(AzureBootstrapError):
    """Raised when the supplied token cannot access configured metrics."""


class SecretWriteError(AzureBootstrapError):
    """Raised when a secret cannot be stored securely."""


class ActionKind(str, Enum):
    """Typed bootstrap action kinds."""

    VERIFY = "verify"
    SELECT = "select"
    VALIDATE = "validate"
    CREATE = "create"
    REUSE = "reuse"
    ASSIGN = "assign"
    WRITE = "write"


@dataclass(frozen=True, slots=True)
class BootstrapAction:
    """One safe-to-display planned or completed bootstrap action."""

    kind: ActionKind
    resource_type: str
    resource_name: str
    write: bool = False


@dataclass(frozen=True, slots=True)
class ExecutionPrincipal:
    """The authenticated Azure object receiving Key Vault data-plane access."""

    object_id: str
    principal_type: str


@dataclass(frozen=True, slots=True)
class BootstrapPlan:
    """A deterministic Azure bootstrap plan."""

    actions: tuple[BootstrapAction, ...]
    principal: ExecutionPrincipal
    vault_id: str | None


@dataclass(frozen=True, slots=True)
class CliResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandExecutor(Protocol):
    def __call__(
        self,
        command: Sequence[str],
        *,
        input: str | None,
        timeout: float,
    ) -> CliResult:
        """Execute one Azure CLI command without invoking a shell."""


def _subprocess_executor(
    command: Sequence[str],
    *,
    input: str | None,
    timeout: float,
) -> CliResult:
    try:
        completed = subprocess.run(
            list(command),
            input=input,
            capture_output=True,
            text=True,
            shell=False,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        raise AzureCliUnavailableError(
            "Azure CLI could not be executed"
        ) from None
    return CliResult(completed.returncode, completed.stdout, completed.stderr)


class AzureCli:
    """Cross-platform, shell-free Azure CLI adapter with typed failures."""

    def __init__(
        self,
        *,
        executable: str | None = None,
        executor: CommandExecutor = _subprocess_executor,
        locator: Callable[[str], str | None] = shutil.which,
        timeout: float = 60,
        secret_directory: Path | None = None,
    ) -> None:
        self._executable = executable
        self._executor = executor
        self._locator = locator
        self._timeout = timeout
        self._secret_directory = secret_directory or Path.cwd()

    def verify_available(self) -> str:
        executable = self._executable or self._locator("az")
        if not executable:
            raise AzureCliUnavailableError("Azure CLI executable 'az' was not found")
        self._executable = executable
        payload = self.run_json(("version", "--output", "json"))
        raw_version = payload.get("azure-cli") if isinstance(payload, dict) else None
        version = _parse_version(raw_version)
        if version < MINIMUM_AZURE_CLI_VERSION:
            minimum = ".".join(map(str, MINIMUM_AZURE_CLI_VERSION))
            raise AzureCliUnavailableError(
                f"Azure CLI {minimum} or newer is required"
            )
        return str(raw_version)

    def run_json(
        self,
        arguments: Sequence[str],
        *,
        allow_not_found: bool = False,
    ) -> Any | None:
        result = self._run(arguments)
        if result.returncode:
            if _is_permission_failure(result.stderr):
                raise AzurePermissionError("Azure denied the requested operation")
            if allow_not_found and _is_not_found(result.stderr):
                return None
            raise AzureResourceError("Azure CLI operation failed")
        if not result.stdout.strip():
            return None
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            raise AzureResourceError(
                "Azure CLI returned an invalid JSON response"
            ) from None

    def run_no_output(self, arguments: Sequence[str]) -> None:
        result = self._run(arguments)
        if result.returncode:
            if _is_permission_failure(result.stderr):
                raise AzurePermissionError("Azure denied the requested operation")
            raise AzureResourceError("Azure CLI operation failed")

    def write_secret(
        self,
        vault_name: str,
        secret_name: str,
        value: str,
        *,
        subscription_id: str,
    ) -> None:
        """Write a secret through a restrictive short-lived file, never argv."""
        if not value:
            raise SecretWriteError("GitHub token provider returned no credential")
        directory = self._secret_directory.resolve()
        path = directory / f".ghcp-secret-{uuid.uuid4().hex}"
        descriptor: int | None = None
        try:
            directory.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
                descriptor = None
                handle.write(value)
                handle.flush()
                os.fsync(handle.fileno())
            self.run_no_output(
                (
                    "keyvault",
                    "secret",
                    "set",
                    "--vault-name",
                    vault_name,
                    "--name",
                    secret_name,
                    "--file",
                    str(path),
                    "--subscription",
                    subscription_id,
                    "--output",
                    "none",
                )
            )
        except AzureBootstrapError:
            raise
        except OSError:
            raise SecretWriteError(
                "could not prepare the secure secret input"
            ) from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
            _destroy_file(path)

    def _run(self, arguments: Sequence[str]) -> CliResult:
        executable = self._executable or self._locator("az")
        if not executable:
            raise AzureCliUnavailableError("Azure CLI executable 'az' was not found")
        self._executable = executable
        return self._executor(
            (executable, *arguments),
            input=None,
            timeout=self._timeout,
        )


class TokenValidator(Protocol):
    def __call__(self, endpoint: str, token: str) -> None:
        """Validate a token without retaining or displaying it."""


class GitHubMetricsTokenValidator:
    """Validate access to configured Copilot metrics endpoints."""

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout: float = 30,
    ) -> None:
        self._session = session or requests.Session()
        self._timeout = timeout

    def __call__(self, endpoint: str, token: str) -> None:
        try:
            response = self._session.get(
                endpoint,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {token}",
                    "X-GitHub-Api-Version": API_VERSION,
                    "User-Agent": "github-copilot-metrics-fabric",
                },
                timeout=self._timeout,
                allow_redirects=False,
            )
        except requests.RequestException:
            raise GitHubTokenValidationError(
                "GitHub token validation request failed"
            ) from None
        if response.status_code not in {200, 204}:
            if response.status_code == 401:
                message = "GitHub rejected the supplied token"
            elif response.status_code in {403, 404}:
                message = "GitHub token cannot access the configured metrics endpoint"
            else:
                message = (
                    "GitHub token validation returned HTTP "
                    f"{response.status_code}"
                )
            raise GitHubTokenValidationError(message)


class AzureBootstrapService:
    """Plan or apply Azure resource, RBAC, and secret reconciliation."""

    def __init__(
        self,
        config: AppConfig,
        *,
        cli: AzureCli,
        token_validator: TokenValidator | None = None,
        today: Callable[[], date] | None = None,
    ) -> None:
        self._config = config
        self._cli = cli
        self._validate_token = token_validator or GitHubMetricsTokenValidator()
        self._today = today or (lambda: datetime.now(timezone.utc).date())

    def plan(
        self, token_provider: Callable[[], str] | None = None
    ) -> BootstrapPlan:
        """Return a discovery-backed plan without cloud resource writes."""
        return self._reconcile(token_provider, apply=False)

    def apply(self, token_provider: Callable[[], str]) -> BootstrapPlan:
        """Apply the idempotent plan and store the validated token."""
        return self._reconcile(token_provider, apply=True)

    def _reconcile(
        self,
        token_provider: Callable[[], str] | None,
        *,
        apply: bool,
    ) -> BootstrapPlan:
        azure = self._config.azure
        if not azure.subscription_id:
            raise AzureResourceError("azure.subscription_id is required")
        if not azure.resource_group_name:
            raise AzureResourceError("azure.resource_group_name is required")
        if not azure.key_vault_name:
            raise AzureResourceError("azure.key_vault_name is required")

        actions: list[BootstrapAction] = []
        version = self._cli.verify_available()
        actions.append(BootstrapAction(ActionKind.VERIFY, "AzureCLI", version))

        try:
            account = self._cli.run_json(
                ("account", "show", "--output", "json")
            )
        except AzureResourceError:
            raise AzureAuthenticationError("Azure CLI is not logged in") from None
        if not isinstance(account, dict):
            raise AzureAuthenticationError("Azure CLI is not logged in")
        if apply:
            self._cli.run_no_output(
                ("account", "set", "--subscription", azure.subscription_id)
            )
        selected = self._cli.run_json(
            (
                "account",
                "show",
                "--subscription",
                azure.subscription_id,
                "--output",
                "json",
            )
        )
        if (
            not isinstance(selected, dict)
            or selected.get("id") != azure.subscription_id
        ):
            raise AzureAuthenticationError(
                "Azure CLI could not select the configured subscription"
            )
        actions.append(
            BootstrapAction(
                ActionKind.SELECT,
                "Subscription",
                azure.subscription_id,
            )
        )

        principal = self._resolve_principal(selected)
        token = ""
        if apply:
            if token_provider is None:
                raise GitHubTokenValidationError(
                    "GitHub token provider is required"
                )
            token = token_provider()
            if not isinstance(token, str) or not token.strip():
                raise GitHubTokenValidationError(
                    "GitHub token provider returned no credential"
                )
            for endpoint in self._metrics_endpoints():
                self._validate_token(endpoint, token)
        actions.append(
            BootstrapAction(ActionKind.VALIDATE, "GitHubToken", "configured scopes")
        )

        resource_group = self._cli.run_json(
            (
                "group",
                "show",
                "--name",
                azure.resource_group_name,
                "--subscription",
                azure.subscription_id,
                "--output",
                "json",
            ),
            allow_not_found=True,
        )
        if resource_group is None:
            if not azure.create_resource_group:
                raise AzureResourceError(
                    "configured Azure resource group was not found"
                )
            actions.append(
                BootstrapAction(
                    ActionKind.CREATE,
                    "ResourceGroup",
                    azure.resource_group_name,
                    write=True,
                )
            )
            if apply:
                resource_group = self._cli.run_json(
                    (
                        "group",
                        "create",
                        "--name",
                        azure.resource_group_name,
                        "--location",
                        azure.location,
                        "--subscription",
                        azure.subscription_id,
                        "--output",
                        "json",
                    )
                )
        else:
            actions.append(
                BootstrapAction(
                    ActionKind.REUSE,
                    "ResourceGroup",
                    azure.resource_group_name,
                )
            )

        vault = self._cli.run_json(
            (
                "keyvault",
                "show",
                "--name",
                azure.key_vault_name,
                "--resource-group",
                azure.resource_group_name,
                "--subscription",
                azure.subscription_id,
                "--output",
                "json",
            ),
            allow_not_found=True,
        )
        if vault is None:
            if not azure.create_key_vault:
                raise AzureResourceError("configured Azure Key Vault was not found")
            actions.append(
                BootstrapAction(
                    ActionKind.CREATE,
                    "KeyVault",
                    azure.key_vault_name,
                    write=True,
                )
            )
            if apply:
                vault = self._cli.run_json(
                    (
                        "keyvault",
                        "create",
                        "--name",
                        azure.key_vault_name,
                        "--resource-group",
                        azure.resource_group_name,
                        "--location",
                        azure.location,
                        "--enable-rbac-authorization",
                        "true",
                        "--subscription",
                        azure.subscription_id,
                        "--output",
                        "json",
                    )
                )
        else:
            properties = vault.get("properties", {}) if isinstance(vault, dict) else {}
            if properties.get("enableRbacAuthorization") is not True:
                raise AzureResourceError(
                    "configured Azure Key Vault is not RBAC-enabled"
                )
            actions.append(
                BootstrapAction(
                    ActionKind.REUSE,
                    "KeyVault",
                    azure.key_vault_name,
                )
            )

        vault_id = vault.get("id") if isinstance(vault, dict) else None
        if not vault_id and apply:
            raise AzureResourceError("Azure Key Vault response did not contain an ID")
        runtime_principal = ExecutionPrincipal(
            azure.fabric_runtime_principal_id or principal.object_id,
            (
                azure.fabric_runtime_principal_type
                if azure.fabric_runtime_principal_id
                else principal.principal_type
            ),
        )
        role_targets = (
            (KEY_VAULT_SECRETS_OFFICER, principal),
            (KEY_VAULT_SECRETS_USER, runtime_principal),
        )
        for role_name, role_principal in role_targets:
            assigned = False
            if vault_id:
                assignments = self._cli.run_json(
                    (
                        "role",
                        "assignment",
                        "list",
                        "--assignee-object-id",
                        role_principal.object_id,
                        "--role",
                        role_name,
                        "--scope",
                        vault_id,
                        "--subscription",
                        azure.subscription_id,
                        "--output",
                        "json",
                    )
                )
                assigned = isinstance(assignments, list) and bool(assignments)
            actions.append(
                BootstrapAction(
                    ActionKind.REUSE if assigned else ActionKind.ASSIGN,
                    "RoleAssignment",
                    role_name,
                    write=not assigned,
                )
            )
            if apply and not assigned:
                assert vault_id is not None
                self._cli.run_no_output(
                    (
                        "role",
                        "assignment",
                        "create",
                        "--assignee-object-id",
                        role_principal.object_id,
                        "--assignee-principal-type",
                        role_principal.principal_type,
                        "--role",
                        role_name,
                        "--scope",
                        vault_id,
                        "--subscription",
                        azure.subscription_id,
                        "--output",
                        "none",
                    )
                )

        actions.append(
            BootstrapAction(
                ActionKind.WRITE,
                "KeyVaultSecret",
                azure.github_token_secret_name,
                write=True,
            )
        )
        if apply:
            self._cli.write_secret(
                azure.key_vault_name,
                azure.github_token_secret_name,
                token,
                subscription_id=azure.subscription_id,
            )
        token = ""
        return BootstrapPlan(tuple(actions), principal, vault_id)

    def _resolve_principal(
        self, account: Mapping[str, Any]
    ) -> ExecutionPrincipal:
        user = account.get("user")
        if not isinstance(user, dict):
            raise AzureAuthenticationError(
                "Azure CLI account has no authenticated principal"
            )
        principal_kind = str(user.get("type", "")).lower()
        if principal_kind == "user":
            payload = self._cli.run_json(
                ("ad", "signed-in-user", "show", "--output", "json")
            )
            principal_type = "User"
        elif principal_kind in {"serviceprincipal", "service_principal"}:
            client_id = user.get("name")
            if not isinstance(client_id, str) or not client_id:
                raise AzureAuthenticationError(
                    "Azure CLI service principal has no client ID"
                )
            payload = self._cli.run_json(
                ("ad", "sp", "show", "--id", client_id, "--output", "json")
            )
            principal_type = "ServicePrincipal"
        else:
            raise AzureAuthenticationError(
                "Azure CLI principal type is not supported"
            )
        object_id = payload.get("id") if isinstance(payload, dict) else None
        if not isinstance(object_id, str) or not object_id:
            raise AzureAuthenticationError(
                "Azure CLI could not resolve the authenticated principal"
            )
        return ExecutionPrincipal(object_id, principal_type)

    def _metrics_endpoints(self) -> tuple[str, ...]:
        github = self._config.github
        day = self._today() - timedelta(days=1)
        query = urlencode({"day": day.isoformat()})
        if github.mode == "enterprise":
            assert github.enterprise is not None
            path = (
                f"/enterprises/{quote(github.enterprise, safe='')}"
                "/copilot/metrics/reports/enterprise-1-day"
            )
            return (f"https://api.github.com{path}?{query}",)
        return tuple(
            "https://api.github.com"
            f"/orgs/{quote(organization, safe='')}"
            f"/copilot/metrics/reports/organization-1-day?{query}"
            for organization in github.organizations
        )


def _parse_version(raw: object) -> tuple[int, int, int]:
    if not isinstance(raw, str):
        raise AzureCliUnavailableError(
            "Azure CLI version could not be determined"
        )
    parts = raw.split(".", 3)
    try:
        values = tuple(int(part) for part in parts[:3])
    except ValueError:
        raise AzureCliUnavailableError(
            "Azure CLI version could not be determined"
        ) from None
    if len(values) != 3:
        raise AzureCliUnavailableError(
            "Azure CLI version could not be determined"
        )
    return values


def _is_permission_failure(stderr: str) -> bool:
    lowered = stderr.lower()
    return any(
        marker in lowered
        for marker in (
            "authorizationfailed",
            "forbidden",
            "does not have authorization",
            "permission",
        )
    )


def _is_not_found(stderr: str) -> bool:
    lowered = stderr.lower()
    return any(
        marker in lowered
        for marker in (
            "resourcenotfound",
            "resourcegroupnotfound",
            "vaultnotfound",
            "could not be found",
            "was not found",
        )
    )


def _destroy_file(path: Path) -> None:
    try:
        if path.exists():
            size = path.stat().st_size
            with path.open("r+b", buffering=0) as handle:
                handle.write(b"\0" * size)
                handle.flush()
                os.fsync(handle.fileno())
            path.unlink()
    except OSError:
        with suppress(OSError):
            path.unlink(missing_ok=True)
