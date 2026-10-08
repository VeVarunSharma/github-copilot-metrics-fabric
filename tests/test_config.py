from pathlib import Path

import pytest

from copilot_metrics_fabric.config import ConfigurationError, load_config, parse_config


def organization_config() -> dict[str, object]:
    return {
        "schema_version": 1,
        "github": {
            "mode": "organization",
            "organizations": ["example-org", "second-org"],
        },
        "collection": {
            "lookback_days": 28,
            "output_directory": "data/raw",
        },
        "fabric": {"workspace_name": None, "lakehouse_name": "Metrics"},
    }


def test_parses_organization_mode() -> None:
    config = parse_config(organization_config())

    assert config.github.mode == "organization"
    assert config.github.organizations == ("example-org", "second-org")
    assert config.collection.output_directory == Path("data/raw")
    assert config.fabric.lakehouse_name == "Metrics"


def test_parses_enterprise_mode_with_optional_org_filter() -> None:
    raw = organization_config()
    raw["github"] = {
        "mode": "enterprise",
        "enterprise": "example-enterprise",
        "organizations": [],
    }

    config = parse_config(raw)

    assert config.github.enterprise == "example-enterprise"
    assert config.github.organizations == ()


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            {"github": {"mode": "organization", "organizations": []}},
            "at least one organization",
        ),
        (
            {"github": {"mode": "enterprise", "organizations": []}},
            "enterprise is required",
        ),
        (
            {"collection": {"lookback_days": 0}},
            "integer from 1 through 90",
        ),
        (
            {"collection": {"output_directory": "../outside"}},
            "relative path",
        ),
        (
            {"github": {"mode": "organization", "organizations": ["bad slug"]}},
            "valid GitHub",
        ),
    ],
)
def test_rejects_invalid_values(
    change: dict[str, object], message: str
) -> None:
    raw = organization_config()
    raw.update(change)

    with pytest.raises(ConfigurationError, match=message):
        parse_config(raw)


def test_rejects_unknown_keys_including_secret_like_fields() -> None:
    raw = organization_config()
    raw["github"] = {
        "mode": "organization",
        "organizations": ["example-org"],
        "token": "do-not-accept-secrets",
    }

    with pytest.raises(ConfigurationError, match=r"unknown key.*token"):
        parse_config(raw)


def test_loads_yaml_file(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    path.write_text(
        "schema_version: 1\n"
        "github:\n"
        "  mode: organization\n"
        "  organizations: [example-org]\n",
        encoding="utf-8",
    )

    config = load_config(path)

    assert config.collection.lookback_days == 28
    assert config.fabric.workspace_name is None
    assert config.azure.location == "eastus2"
    assert config.fabric.create_environment is False
    assert config.backfill.enabled is False
    assert config.schedule.timezone == "UTC"


def test_parses_end_to_end_bootstrap_settings() -> None:
    raw = organization_config()
    raw.update(
        {
            "azure": {
                "subscription_id": "00000000-0000-0000-0000-000000000000",
                "resource_group_name": "metrics-rg",
                "location": "westus2",
                "create_resource_group": True,
                "key_vault_name": "metrics-kv-123",
                "create_key_vault": True,
                "github_token_secret_name": "github-token",
            },
            "fabric": {
                "workspace_name": "Metrics",
                "lakehouse_name": "Metrics",
                "environment_name": "Metrics Environment",
                "create_environment": True,
            },
            "backfill": {
                "enabled": True,
                "start_date": "2026-09-01",
                "end_date": "2026-09-30",
            },
            "schedule": {
                "enabled": True,
                "time": "03:30",
                "timezone": "UTC",
                "trailing_days": 30,
            },
        }
    )

    config = parse_config(raw)

    assert config.azure.create_key_vault is True
    assert config.fabric.environment_name == "Metrics Environment"
    assert config.backfill.start_date.isoformat() == "2026-09-01"
    assert config.backfill.wait_for_completion is True
    assert config.schedule.time == "03:30"
    assert config.fabric.report_types == (
        "entity",
        "users",
        "user-teams",
        "repositories",
    )


@pytest.mark.parametrize(
    "timezone",
    ["UTC", "Pacific Standard Time", "UTC-11", "UTC+12"],
)
def test_accepts_fabric_windows_schedule_timezone(timezone: str) -> None:
    raw = organization_config()
    raw["schedule"] = {
        "enabled": True,
        "time": "02:00",
        "timezone": timezone,
        "trailing_days": 28,
    }

    config = parse_config(raw)

    assert config.schedule.timezone == timezone


@pytest.mark.parametrize(
    "timezone",
    [
        "Bogus Standard Time",
        "Mars/Olympus",
        "utc",
        "America/Los_Angeles",
        "",
    ],
)
def test_rejects_unsupported_fabric_schedule_timezone(timezone: str) -> None:
    raw = organization_config()
    raw["schedule"] = {"timezone": timezone}

    with pytest.raises(ConfigurationError, match="supported by Fabric"):
        parse_config(raw)


@pytest.mark.parametrize(
    ("section", "value", "message"),
    [
        ("backfill", {"start_date": "not-a-date"}, "ISO date"),
        (
            "backfill",
            {"start_date": "2026-10-02", "end_date": "2026-10-01"},
            "on or before",
        ),
        ("schedule", {"time": "25:00"}, "HH:MM"),
        ("schedule", {"trailing_days": 0}, "1 through 90"),
    ],
)
def test_rejects_invalid_bootstrap_values(section, value, message) -> None:
    raw = organization_config()
    raw[section] = value

    with pytest.raises(ConfigurationError, match=message):
        parse_config(raw)


@pytest.mark.parametrize(
    "field",
    ["token", "password", "client_secret", "connection_string", "private_key"],
)
def test_rejects_secret_fields_anywhere(field: str) -> None:
    raw = organization_config()
    raw["azure"] = {field: "must-not-persist"}

    with pytest.raises(ConfigurationError, match="secret-like"):
        parse_config(raw)


def test_reports_invalid_yaml(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    path.write_text("github: [", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="invalid YAML"):
        load_config(path)
