"""Load and validate the project's non-secret YAML configuration."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePath
from typing import Any

import yaml

from copilot_metrics_fabric.fabric_timezones import FABRIC_TIME_ZONE_IDS

_SLUG_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$")
_NAME_PATTERN = re.compile(r"^[^\x00-\x1f<>:\"/\\|?*]+$")
_RESOURCE_GROUP_PATTERN = re.compile(r"^[\w().-]{1,90}$")
_LOCATION_PATTERN = re.compile(r"^[a-z0-9-]{2,64}$")
_KEY_VAULT_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9-]{1,22}[A-Za-z0-9]$")
_SUBSCRIPTION_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_SECRET_FIELD_PATTERN = re.compile(
    r"(?i)(?:^|_)(token|password|credential|client_secret|access_key|"
    r"connection_string|private_key|secret)(?:$|_)"
)


class ConfigurationError(ValueError):
    """Raised when a configuration file does not satisfy the contract."""


@dataclass(frozen=True, slots=True)
class GitHubConfig:
    mode: str
    organizations: tuple[str, ...]
    enterprise: str | None = None


@dataclass(frozen=True, slots=True)
class CollectionConfig:
    lookback_days: int = 28
    output_directory: Path = Path("data/raw")


@dataclass(frozen=True, slots=True)
class AzureConfig:
    subscription_id: str | None = None
    resource_group_name: str | None = None
    location: str = "eastus2"
    create_resource_group: bool = False
    key_vault_name: str | None = None
    create_key_vault: bool = False
    github_token_secret_name: str = "github-copilot-metrics-token"
    fabric_runtime_principal_id: str | None = None
    fabric_runtime_principal_type: str = "ServicePrincipal"


@dataclass(frozen=True, slots=True)
class FabricConfig:
    workspace_name: str | None = None
    lakehouse_name: str | None = None
    create_workspace: bool = False
    capacity_id: str | None = None
    pipeline_name: str = "copilot_metrics_orchestration"
    semantic_model_name: str = "GitHubCopilotMetrics"
    report_name: str = "GitHubCopilotMetrics"
    environment_name: str | None = None
    create_environment: bool = False
    state_file: Path = Path(".fabric-deploy-state.json")
    report_types: tuple[str, ...] = (
        "entity",
        "users",
        "user-teams",
        "repositories",
    )
    calculation_lookback_days: int = 27
    earliest_date: date | None = None
    lakehouse_files_root: str = "/lakehouse/default/Files"
    bronze_folder: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    audit_schema: str = "audit"
    telemetry_lag_days: int = 2


@dataclass(frozen=True, slots=True)
class BackfillConfig:
    enabled: bool = False
    start_date: date | None = None
    end_date: date | None = None
    wait_for_completion: bool = True


@dataclass(frozen=True, slots=True)
class ScheduleConfig:
    enabled: bool = False
    time: str = "02:00"
    timezone: str = "UTC"
    trailing_days: int = 28


@dataclass(frozen=True, slots=True)
class AppConfig:
    schema_version: int
    github: GitHubConfig
    collection: CollectionConfig
    fabric: FabricConfig
    azure: AzureConfig = AzureConfig()
    backfill: BackfillConfig = BackfillConfig()
    schedule: ScheduleConfig = ScheduleConfig()


def load_config(path: str | Path) -> AppConfig:
    """Load a YAML file and return a validated, immutable configuration."""
    config_path = Path(path)
    try:
        content = config_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ConfigurationError(f"cannot read {config_path}: {error}") from error

    try:
        raw = yaml.safe_load(content)
    except yaml.YAMLError as error:
        raise ConfigurationError(f"invalid YAML in {config_path}: {error}") from error

    return parse_config(raw)


def parse_config(raw: Any) -> AppConfig:
    """Validate an already-decoded YAML value against contract version 1."""
    root = _mapping(raw, "root")
    _reject_secret_fields(root)
    _only_keys(
        root,
        {
            "schema_version",
            "github",
            "collection",
            "azure",
            "fabric",
            "backfill",
            "schedule",
        },
        "root",
    )

    schema_version = root.get("schema_version")
    if type(schema_version) is not int or schema_version != 1:
        raise ConfigurationError("schema_version must be the integer 1")

    github = _parse_github(root.get("github"))
    collection = _parse_collection(root.get("collection", {}))
    azure = _parse_azure(root.get("azure", {}))
    fabric = _parse_fabric(root.get("fabric", {}))
    backfill = _parse_backfill(root.get("backfill", {}))
    schedule = _parse_schedule(root.get("schedule", {}))
    return AppConfig(
        schema_version,
        github,
        collection,
        fabric,
        azure,
        backfill,
        schedule,
    )


def _parse_github(raw: Any) -> GitHubConfig:
    value = _mapping(raw, "github")
    _only_keys(value, {"mode", "organizations", "enterprise"}, "github")

    mode = value.get("mode")
    if mode not in {"organization", "enterprise"}:
        raise ConfigurationError(
            "github.mode must be either 'organization' or 'enterprise'"
        )

    organizations_raw = value.get("organizations", [])
    if not isinstance(organizations_raw, list):
        raise ConfigurationError("github.organizations must be a list")
    organizations = tuple(
        _slug(item, f"github.organizations[{index}]")
        for index, item in enumerate(organizations_raw)
    )
    if len(set(organizations)) != len(organizations):
        raise ConfigurationError("github.organizations must not contain duplicates")

    enterprise_raw = value.get("enterprise")
    enterprise = (
        None
        if enterprise_raw is None
        else _slug(enterprise_raw, "github.enterprise")
    )

    if mode == "organization":
        if not organizations:
            raise ConfigurationError(
                "github.organizations must contain at least one organization "
                "in organization mode"
            )
        if enterprise is not None:
            raise ConfigurationError(
                "github.enterprise is only allowed in enterprise mode"
            )
    elif enterprise is None:
        raise ConfigurationError(
            "github.enterprise is required in enterprise mode"
        )

    return GitHubConfig(mode, organizations, enterprise)


def _parse_collection(raw: Any) -> CollectionConfig:
    value = _mapping(raw, "collection")
    _only_keys(value, {"lookback_days", "output_directory"}, "collection")

    lookback_days = value.get("lookback_days", 28)
    if type(lookback_days) is not int or not 1 <= lookback_days <= 90:
        raise ConfigurationError(
            "collection.lookback_days must be an integer from 1 through 90"
        )

    output_raw = value.get("output_directory", "data/raw")
    if not isinstance(output_raw, str) or not output_raw.strip():
        raise ConfigurationError(
            "collection.output_directory must be a non-empty string"
        )
    output = Path(output_raw)
    path_parts = PurePath(output_raw.replace("\\", "/")).parts
    if output.is_absolute() or ".." in path_parts:
        raise ConfigurationError(
            "collection.output_directory must be a relative path without '..'"
        )

    return CollectionConfig(lookback_days, output)


def _parse_azure(raw: Any) -> AzureConfig:
    value = _mapping(raw, "azure")
    _only_keys(
        value,
        {
            "subscription_id",
            "resource_group_name",
            "location",
            "create_resource_group",
            "key_vault_name",
            "create_key_vault",
            "github_token_secret_name",
            "fabric_runtime_principal_id",
            "fabric_runtime_principal_type",
        },
        "azure",
    )
    create_resource_group = _boolean(
        value.get("create_resource_group", False),
        "azure.create_resource_group",
    )
    create_key_vault = _boolean(
        value.get("create_key_vault", False), "azure.create_key_vault"
    )
    subscription_id = _optional_pattern(
        value.get("subscription_id"),
        "azure.subscription_id",
        _SUBSCRIPTION_PATTERN,
        "a valid Azure subscription UUID",
    )
    resource_group_name = _optional_pattern(
        value.get("resource_group_name"),
        "azure.resource_group_name",
        _RESOURCE_GROUP_PATTERN,
        "a valid Azure resource group name",
    )
    location = value.get("location", "eastus2")
    if not isinstance(location, str) or not _LOCATION_PATTERN.fullmatch(location):
        raise ConfigurationError(
            "azure.location must be a lowercase Azure region name"
        )
    key_vault_name = _optional_pattern(
        value.get("key_vault_name"),
        "azure.key_vault_name",
        _KEY_VAULT_PATTERN,
        "a valid 3-24 character Key Vault name",
    )
    secret_name = value.get(
        "github_token_secret_name", "github-copilot-metrics-token"
    )
    if (
        not isinstance(secret_name, str)
        or not re.fullmatch(r"[A-Za-z0-9-]{1,127}", secret_name)
    ):
        raise ConfigurationError(
            "azure.github_token_secret_name must be a valid Key Vault secret name"
        )
    if create_resource_group and resource_group_name is None:
        raise ConfigurationError(
            "azure.resource_group_name is required when create_resource_group is true"
        )
    if create_key_vault and key_vault_name is None:
        raise ConfigurationError(
            "azure.key_vault_name is required when create_key_vault is true"
        )
    if (create_resource_group or create_key_vault) and subscription_id is None:
        raise ConfigurationError(
            "azure.subscription_id is required when Azure resource creation "
            "is enabled"
        )
    if create_key_vault and resource_group_name is None:
        raise ConfigurationError(
            "azure.resource_group_name is required when create_key_vault is true"
        )
    runtime_principal_id = _optional_pattern(
        value.get("fabric_runtime_principal_id"),
        "azure.fabric_runtime_principal_id",
        _SUBSCRIPTION_PATTERN,
        "a valid Microsoft Entra object UUID",
    )
    runtime_principal_type = value.get(
        "fabric_runtime_principal_type", "ServicePrincipal"
    )
    if runtime_principal_type not in {"User", "ServicePrincipal"}:
        raise ConfigurationError(
            "azure.fabric_runtime_principal_type must be User or "
            "ServicePrincipal"
        )
    return AzureConfig(
        subscription_id,
        resource_group_name,
        location,
        create_resource_group,
        key_vault_name,
        create_key_vault,
        secret_name,
        runtime_principal_id,
        runtime_principal_type,
    )


def _parse_fabric(raw: Any) -> FabricConfig:
    value = _mapping(raw, "fabric")
    _only_keys(
        value,
        {
            "workspace_name",
            "lakehouse_name",
            "create_workspace",
            "capacity_id",
            "pipeline_name",
            "semantic_model_name",
            "report_name",
            "environment_name",
            "create_environment",
            "state_file",
            "report_types",
            "calculation_lookback_days",
            "earliest_date",
            "lakehouse_files_root",
            "bronze_folder",
            "silver_schema",
            "gold_schema",
            "audit_schema",
            "telemetry_lag_days",
        },
        "fabric",
    )
    create_workspace = _boolean(
        value.get("create_workspace", False), "fabric.create_workspace"
    )
    create_environment = _boolean(
        value.get("create_environment", False), "fabric.create_environment"
    )
    state_raw = value.get("state_file", ".fabric-deploy-state.json")
    if not isinstance(state_raw, str) or not state_raw.strip():
        raise ConfigurationError("fabric.state_file must be a non-empty string")
    state_file = Path(state_raw)
    state_parts = PurePath(state_raw.replace("\\", "/")).parts
    if state_file.is_absolute() or ".." in state_parts:
        raise ConfigurationError(
            "fabric.state_file must be a relative path without '..'"
        )
    environment_name = _optional_name(
        value.get("environment_name"), "fabric.environment_name"
    )
    if create_environment and environment_name is None:
        raise ConfigurationError(
            "fabric.environment_name is required when create_environment is true"
        )
    report_types_raw = value.get(
        "report_types", ["entity", "users", "user-teams", "repositories"]
    )
    if not isinstance(report_types_raw, list) or not report_types_raw:
        raise ConfigurationError("fabric.report_types must be a non-empty list")
    allowed_report_types = {"entity", "users", "user-teams", "repositories"}
    report_types = tuple(report_types_raw)
    if (
        not all(isinstance(item, str) for item in report_types)
        or any(item not in allowed_report_types for item in report_types)
        or len(set(report_types)) != len(report_types)
    ):
        raise ConfigurationError(
            "fabric.report_types must contain unique supported report types"
        )
    calculation_lookback_days = value.get("calculation_lookback_days", 27)
    if (
        type(calculation_lookback_days) is not int
        or not 0 <= calculation_lookback_days <= 90
    ):
        raise ConfigurationError(
            "fabric.calculation_lookback_days must be an integer from 0 through 90"
        )
    earliest_date = _optional_date(
        value.get("earliest_date"), "fabric.earliest_date"
    )
    lakehouse_files_root = _non_empty_string(
        value.get("lakehouse_files_root", "/lakehouse/default/Files"),
        "fabric.lakehouse_files_root",
    )
    bronze_folder = _simple_identifier(
        value.get("bronze_folder", "bronze"), "fabric.bronze_folder"
    )
    silver_schema = _simple_identifier(
        value.get("silver_schema", "silver"), "fabric.silver_schema"
    )
    gold_schema = _simple_identifier(
        value.get("gold_schema", "gold"), "fabric.gold_schema"
    )
    audit_schema = _simple_identifier(
        value.get("audit_schema", "audit"), "fabric.audit_schema"
    )
    telemetry_lag_days = value.get("telemetry_lag_days", 2)
    if type(telemetry_lag_days) is not int or not 0 <= telemetry_lag_days <= 30:
        raise ConfigurationError(
            "fabric.telemetry_lag_days must be an integer from 0 through 30"
        )
    return FabricConfig(
        _optional_name(value.get("workspace_name"), "fabric.workspace_name"),
        _optional_name(value.get("lakehouse_name"), "fabric.lakehouse_name"),
        create_workspace,
        _optional_identifier(value.get("capacity_id"), "fabric.capacity_id"),
        _name(
            value.get("pipeline_name", "copilot_metrics_orchestration"),
            "fabric.pipeline_name",
        ),
        _name(
            value.get("semantic_model_name", "GitHubCopilotMetrics"),
            "fabric.semantic_model_name",
        ),
        _name(
            value.get("report_name", "GitHubCopilotMetrics"),
            "fabric.report_name",
        ),
        environment_name,
        create_environment,
        state_file,
        report_types,
        calculation_lookback_days,
        earliest_date,
        lakehouse_files_root,
        bronze_folder,
        silver_schema,
        gold_schema,
        audit_schema,
        telemetry_lag_days,
    )


def _parse_backfill(raw: Any) -> BackfillConfig:
    value = _mapping(raw, "backfill")
    _only_keys(
        value,
        {"enabled", "start_date", "end_date", "wait_for_completion"},
        "backfill",
    )
    enabled = _boolean(value.get("enabled", False), "backfill.enabled")
    wait_for_completion = _boolean(
        value.get("wait_for_completion", True),
        "backfill.wait_for_completion",
    )
    start_date = _optional_date(value.get("start_date"), "backfill.start_date")
    end_date = _optional_date(value.get("end_date"), "backfill.end_date")
    if (start_date is None) != (end_date is None):
        raise ConfigurationError(
            "backfill.start_date and backfill.end_date must both be provided"
        )
    if start_date is not None and start_date > end_date:
        raise ConfigurationError(
            "backfill.start_date must be on or before backfill.end_date"
        )
    if enabled and start_date is None:
        raise ConfigurationError(
            "backfill dates are required when backfill.enabled is true"
        )
    return BackfillConfig(enabled, start_date, end_date, wait_for_completion)


def _parse_schedule(raw: Any) -> ScheduleConfig:
    value = _mapping(raw, "schedule")
    _only_keys(
        value, {"enabled", "time", "timezone", "trailing_days"}, "schedule"
    )
    enabled = _boolean(value.get("enabled", False), "schedule.enabled")
    schedule_time = value.get("time", "02:00")
    if not isinstance(schedule_time, str) or not re.fullmatch(
        r"(?:[01]\d|2[0-3]):[0-5]\d", schedule_time
    ):
        raise ConfigurationError("schedule.time must use 24-hour HH:MM format")
    timezone = value.get("timezone", "UTC")
    if not isinstance(timezone, str) or timezone not in FABRIC_TIME_ZONE_IDS:
        raise ConfigurationError(
            "schedule.timezone must be a Windows time-zone ID supported by "
            "Fabric, such as 'UTC', 'Pacific Standard Time', or 'UTC+12'"
        )
    trailing_days = value.get("trailing_days", 28)
    if type(trailing_days) is not int or not 1 <= trailing_days <= 90:
        raise ConfigurationError(
            "schedule.trailing_days must be an integer from 1 through 90"
        )
    return ScheduleConfig(enabled, schedule_time, timezone, trailing_days)


def _mapping(raw: Any, location: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ConfigurationError(f"{location} must be a mapping")
    if not all(isinstance(key, str) for key in raw):
        raise ConfigurationError(f"{location} keys must be strings")
    return raw


def _reject_secret_fields(value: Any, location: str = "root") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if isinstance(key, str) and _is_secret_field(key):
                raise ConfigurationError(
                    f"unknown key(s) in {location.removeprefix('root.')}: "
                    f"{key} (secret-like configuration fields are not allowed)"
                )
            _reject_secret_fields(nested, f"{location}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_secret_fields(nested, f"{location}[{index}]")


def _is_secret_field(key: str) -> bool:
    if key == "github_token_secret_name":
        return False
    return bool(_SECRET_FIELD_PATTERN.search(key))


def _only_keys(
    value: dict[str, Any], allowed: set[str], location: str
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        keys = ", ".join(unknown)
        raise ConfigurationError(f"unknown key(s) in {location}: {keys}")


def _slug(raw: Any, location: str) -> str:
    if not isinstance(raw, str) or not _SLUG_PATTERN.fullmatch(raw):
        raise ConfigurationError(
            f"{location} must be a valid GitHub organization or enterprise slug"
        )
    return raw


def _optional_name(raw: Any, location: str) -> str | None:
    if raw is None:
        return None
    if (
        not isinstance(raw, str)
        or not raw.strip()
        or len(raw) > 128
        or not _NAME_PATTERN.fullmatch(raw)
    ):
        raise ConfigurationError(
            f"{location} must be null or a valid name of at most 128 characters"
        )
    return raw


def _name(raw: Any, location: str) -> str:
    value = _optional_name(raw, location)
    if value is None:
        raise ConfigurationError(f"{location} must be a valid non-empty name")
    return value


def _optional_identifier(raw: Any, location: str) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip() or len(raw) > 128:
        raise ConfigurationError(
            f"{location} must be null or a non-empty identifier"
        )
    return raw


def _optional_pattern(
    raw: Any,
    location: str,
    pattern: re.Pattern[str],
    description: str,
) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not pattern.fullmatch(raw):
        raise ConfigurationError(f"{location} must be null or {description}")
    return raw


def _boolean(raw: Any, location: str) -> bool:
    if type(raw) is not bool:
        raise ConfigurationError(f"{location} must be a boolean")
    return raw


def _optional_date(raw: Any, location: str) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date):
        return raw
    if not isinstance(raw, str):
        raise ConfigurationError(f"{location} must be an ISO date (YYYY-MM-DD)")
    try:
        return date.fromisoformat(raw)
    except ValueError as error:
        raise ConfigurationError(
            f"{location} must be an ISO date (YYYY-MM-DD)"
        ) from error


def _non_empty_string(raw: Any, location: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigurationError(f"{location} must be a non-empty string")
    return raw


def _simple_identifier(raw: Any, location: str) -> str:
    value = _non_empty_string(raw, location)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,127}", value):
        raise ConfigurationError(f"{location} must be a valid identifier")
    return value
