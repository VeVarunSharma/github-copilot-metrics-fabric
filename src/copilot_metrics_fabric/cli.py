"""Command-line entry points for the project."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

import yaml

from copilot_metrics_fabric import __version__
from copilot_metrics_fabric.azure_bootstrap import AzureBootstrapError
from copilot_metrics_fabric.bootstrap import BootstrapError, BootstrapOrchestrator
from copilot_metrics_fabric.bootstrap_jobs import SqlBackfillCompletionStore
from copilot_metrics_fabric.config import (
    ConfigurationError,
    load_config,
    parse_config,
)
from copilot_metrics_fabric.deployment import (
    DeploymentError,
    FabricClient,
    FabricDeployer,
    resolve_asset_root,
    validate_assets,
)
from copilot_metrics_fabric.sql_audit import FabricSqlAuditExecutor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ghcp-metrics",
        description="GitHub Copilot metrics for Microsoft Fabric",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser(
        "init",
        help="Create a validated, non-secret bootstrap configuration",
    )
    init.add_argument("--output", type=Path, default=Path("config/config.yml"))
    init.add_argument("--force", action="store_true")
    init.add_argument(
        "--defaults",
        "--non-interactive",
        dest="defaults",
        action="store_true",
        help="Do not prompt; use safe defaults and supplied scope options",
    )
    init.add_argument("--scope", choices=("organization", "enterprise"))
    init.add_argument("--organization", action="append", dest="organizations")
    init.add_argument("--enterprise")

    validate = subparsers.add_parser(
        "validate-config",
        help="Validate a non-secret YAML configuration file",
    )
    validate.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the YAML configuration file",
    )
    validate_assets_parser = subparsers.add_parser(
        "validate",
        help="Validate configuration and all deployment assets",
    )
    validate_assets_parser.add_argument("--config", type=Path, required=True)

    plan = subparsers.add_parser(
        "plan",
        help="Discover Fabric resources and print a write-free deployment plan",
    )
    plan.add_argument("--config", type=Path, required=True)

    deploy = subparsers.add_parser(
        "deploy",
        help="Idempotently deploy all Fabric assets",
    )
    deploy.add_argument("--config", type=Path, required=True)
    deploy.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform validation and discovery only; make no writes",
    )
    deploy.add_argument(
        "--skip-bi",
        action="store_true",
        help="Deploy data engineering assets only; add BI after Gold tables exist",
    )

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="Plan, apply, resume, or inspect the end-to-end setup",
    )
    bootstrap_commands = bootstrap.add_subparsers(
        dest="bootstrap_command", required=True
    )
    for name in ("plan", "status"):
        command = bootstrap_commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--json", action="store_true")
    for name in ("apply", "resume"):
        command = bootstrap_commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument(
            "--yes",
            action="store_true",
            help="Skip the interactive confirmation",
        )
        command.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "init":
        try:
            raw = _init_config(args)
            config = parse_config(raw)
            _write_config(args.output, raw, force=args.force)
        except ConfigurationError as error:
            print(f"Configuration error: {error}", file=sys.stderr)
            return 2
        print(
            f"Wrote non-secret {config.github.mode} configuration to "
            f"{args.output}."
        )
        return 0

    if args.command == "validate-config":
        try:
            config = load_config(args.config)
        except ConfigurationError as error:
            print(f"Configuration error: {error}", file=sys.stderr)
            return 2
        print(
            f"Configuration is valid (schema version {config.schema_version}, "
            f"GitHub mode: {config.github.mode})."
        )
        return 0

    if args.command == "validate":
        try:
            config = load_config(args.config)
            validate_assets(resolve_asset_root(args.config))
            if not config.fabric.workspace_name or not config.fabric.lakehouse_name:
                raise ConfigurationError(
                    "fabric.workspace_name and fabric.lakehouse_name are "
                    "required for deployment"
                )
        except (ConfigurationError, DeploymentError) as error:
            print(f"Validation error: {error}", file=sys.stderr)
            return 2
        print("Configuration and deployment assets are valid.")
        return 0

    if args.command in {"plan", "deploy"}:
        try:
            config = load_config(args.config)
            root = resolve_asset_root(args.config)
            state_root = (
                root if (root / "pyproject.toml").is_file() else args.config.parent
            )
            deployer_options = {}
            if state_root != root:
                deployer_options["state_root"] = state_root
            deployer = FabricDeployer(
                config,
                root=root,
                client=FabricClient(),
                include_bi=not args.skip_bi,
                **deployer_options,
            )
            dry_run = args.command == "plan" or args.dry_run
            actions = deployer.plan() if dry_run else deployer.deploy()
        except (ConfigurationError, DeploymentError) as error:
            print(f"Deployment error: {error}", file=sys.stderr)
            return 2
        heading = "Deployment plan (no writes):" if dry_run else "Deployment complete:"
        print(heading)
        for action in actions:
            print(f"  {action}")
        return 0

    if args.command == "bootstrap":
        try:
            config = load_config(args.config)
            root = resolve_asset_root(args.config)
            fabric_client = FabricClient()
            orchestrator = BootstrapOrchestrator(
                config,
                root=root,
                config_path=args.config,
                fabric_client=fabric_client,
                backfill_completion_store=SqlBackfillCompletionStore(
                    FabricSqlAuditExecutor(config, fabric_client)
                ),
            )
            if args.bootstrap_command == "plan":
                plan = orchestrator.plan()
                _print_bootstrap_plan(plan, json_output=args.json)
                return 0
            if args.bootstrap_command == "status":
                status = orchestrator.status()
                _print_bootstrap_value(status, json_output=args.json)
                return 0

            resume = args.bootstrap_command == "resume"
            plan = orchestrator.plan()
            if not args.yes:
                _print_bootstrap_plan(plan, json_output=False)
                answer = input("Apply these persistent cloud changes? [y/N]: ")
                if answer.strip().lower() not in {"y", "yes"}:
                    print("Bootstrap cancelled; no changes were made.")
                    return 0
            orchestrator.token_provider = lambda: (
                os.environ.get("GITHUB_TOKEN")
                or getpass.getpass("GitHub token (input hidden): ")
            )
            result = orchestrator.apply(resume=resume)
        except (
            ConfigurationError,
            DeploymentError,
            AzureBootstrapError,
            BootstrapError,
        ) as error:
            print(f"Bootstrap error: {error}", file=sys.stderr)
            return 2
        _print_bootstrap_value(
            {
                "status": "complete",
                "jobs": result.jobs,
                "links": result.links,
            },
            json_output=args.json,
        )
        return 0

    return 1


def _print_bootstrap_plan(plan, *, json_output: bool) -> None:
    if json_output:
        print(json.dumps(plan.to_dict(), indent=2, sort_keys=True))
        return
    print("Bootstrap plan:")
    for action in plan.azure:
        marker = "WRITE" if action.write else "READ "
        print(
            f"  {marker} Azure {action.kind.value}: "
            f"{action.resource_type} {action.resource_name}"
        )
    for action in plan.fabric:
        print(f"  WRITE Fabric {action}")
    for action in plan.jobs:
        print(
            f"  WRITE Fabric {action.operation.upper()}: "
            f"{action.resource} ({action.scope.key})"
        )


def _print_bootstrap_value(value: dict[str, object], *, json_output: bool) -> None:
    if json_output:
        print(json.dumps(value, indent=2, sort_keys=True, default=str))
        return
    if value.get("status") == "complete":
        print("Bootstrap complete.")
    phases = value.get("completed_phases")
    if phases:
        print(f"Completed phases: {', '.join(str(item) for item in phases)}")
    jobs = value.get("jobs")
    if jobs:
        print("Jobs:")
        for scope, job in dict(jobs).items():
            print(f"  {scope}: {job}")
    links = value.get("links")
    if links:
        print("Fabric links:")
        for name, link in dict(links).items():
            print(f"  {name}: {link}")


def _init_config(args: argparse.Namespace) -> dict[str, object]:
    if args.defaults:
        mode = args.scope or ("enterprise" if args.enterprise else "organization")
        if mode == "organization" and args.enterprise:
            raise ConfigurationError(
                "--enterprise cannot be used with organization scope"
            )
        organizations = args.organizations or (
            ["example-organization"] if mode == "organization" else []
        )
        enterprise = args.enterprise or (
            "example-enterprise" if mode == "enterprise" else None
        )
        return _config_template(mode, organizations, enterprise)

    mode = args.scope or _prompt_choice(
        "GitHub scope [organization/enterprise]", ("organization", "enterprise")
    )
    if mode == "organization":
        if args.enterprise:
            raise ConfigurationError(
                "--enterprise cannot be used with organization scope"
            )
        organizations = args.organizations or _prompt_slugs(
            "GitHub organization slug(s), comma-separated"
        )
        enterprise = None
    else:
        organizations = args.organizations or []
        enterprise = args.enterprise or _prompt("GitHub enterprise slug")
    raw = _config_template(mode, organizations, enterprise)

    azure = raw["azure"]
    fabric = raw["fabric"]
    assert isinstance(azure, dict)
    assert isinstance(fabric, dict)
    azure["subscription_id"] = _prompt("Azure subscription ID")
    azure["resource_group_name"] = _prompt_default(
        "Azure resource group name", "github-copilot-metrics"
    )
    azure["location"] = _prompt_default("Azure location", "eastus2")
    azure["create_resource_group"] = _prompt_yes_no(
        "Create the resource group if missing", True
    )
    azure["key_vault_name"] = _prompt("Globally unique Azure Key Vault name")
    azure["create_key_vault"] = _prompt_yes_no(
        "Create the Key Vault if missing", True
    )
    azure["fabric_runtime_principal_id"] = _prompt_optional(
        "Fabric runtime Entra object ID "
        "(blank uses the current bootstrap identity)"
    )
    fabric["workspace_name"] = _prompt_default(
        "Fabric workspace name", "GitHub Copilot Metrics"
    )
    fabric["create_workspace"] = _prompt_yes_no(
        "Create the Fabric workspace if missing", True
    )
    fabric["capacity_id"] = _prompt_optional(
        "Fabric capacity ID (leave blank if assignment is automatic)"
    )
    fabric["environment_name"] = _prompt_default(
        "Fabric Environment name", "GitHubCopilotMetrics"
    )
    fabric["create_environment"] = _prompt_yes_no(
        "Create the Fabric Environment if missing", True
    )

    if _prompt_yes_no("Configure an initial backfill", False):
        backfill = raw["backfill"]
        assert isinstance(backfill, dict)
        backfill["enabled"] = True
        backfill["start_date"] = _prompt("Backfill start date (YYYY-MM-DD)")
        backfill["end_date"] = _prompt("Backfill end date (YYYY-MM-DD)")

    schedule = raw["schedule"]
    assert isinstance(schedule, dict)
    schedule["enabled"] = _prompt_yes_no("Enable a daily schedule", True)
    schedule["time"] = _prompt_default("Daily run time (HH:MM)", "02:00")
    schedule["timezone"] = _prompt_default(
        "Fabric Windows time-zone ID (for example UTC or Pacific Standard Time)",
        "UTC",
    )
    return raw


def _config_template(
    mode: str, organizations: list[str], enterprise: str | None
) -> dict[str, object]:
    yesterday = date.today() - timedelta(days=1)
    start = yesterday - timedelta(days=27)
    github: dict[str, object] = {
        "mode": mode,
        "organizations": organizations,
    }
    if mode == "enterprise":
        github["enterprise"] = enterprise
    return {
        "schema_version": 1,
        "github": github,
        "collection": {
            "lookback_days": 28,
            "output_directory": "data/raw",
        },
        "azure": {
            "subscription_id": None,
            "resource_group_name": None,
            "location": "eastus2",
            "create_resource_group": False,
            "key_vault_name": None,
            "create_key_vault": False,
            "github_token_secret_name": "github-copilot-metrics-token",
            "fabric_runtime_principal_id": None,
            "fabric_runtime_principal_type": "ServicePrincipal",
        },
        "fabric": {
            "workspace_name": "GitHub Copilot Metrics",
            "lakehouse_name": "GitHubCopilotMetrics",
            "create_workspace": False,
            "capacity_id": None,
            "pipeline_name": "copilot_metrics_orchestration",
            "semantic_model_name": "GitHubCopilotMetrics",
            "report_name": "GitHubCopilotMetrics",
            "environment_name": "GitHubCopilotMetrics",
            "create_environment": False,
            "state_file": ".fabric-deploy-state.json",
            "report_types": [
                "entity",
                "users",
                "user-teams",
                "repositories",
            ],
            "calculation_lookback_days": 27,
            "earliest_date": None,
            "lakehouse_files_root": "/lakehouse/default/Files",
            "bronze_folder": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold",
            "audit_schema": "audit",
            "telemetry_lag_days": 2,
        },
        "backfill": {
            "enabled": False,
            "start_date": start.isoformat(),
            "end_date": yesterday.isoformat(),
            "wait_for_completion": True,
        },
        "schedule": {
            "enabled": True,
            "time": "02:00",
            "timezone": "UTC",
            "trailing_days": 28,
        },
    }


def _write_config(path: Path, raw: dict[str, object], *, force: bool) -> None:
    if path.exists() and not force:
        raise ConfigurationError(
            f"{path} already exists; use --force to overwrite it"
        )
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        header = (
            "# Non-secret bootstrap configuration. Never add tokens, passwords, "
            "or secret values.\n"
        )
        path.write_text(
            header + yaml.safe_dump(raw, sort_keys=False),
            encoding="utf-8",
        )
    except OSError as error:
        raise ConfigurationError(f"cannot write {path}: {error}") from error


def _prompt(label: str) -> str:
    value = input(f"{label}: ").strip()
    if not value:
        raise ConfigurationError(f"{label} is required")
    return value


def _prompt_optional(label: str) -> str | None:
    return input(f"{label} (leave blank to configure later): ").strip() or None


def _prompt_default(label: str, default: str) -> str:
    return input(f"{label} [{default}]: ").strip() or default


def _prompt_choice(label: str, choices: tuple[str, ...]) -> str:
    value = _prompt(label).lower()
    if value not in choices:
        raise ConfigurationError(
            f"{label} must be one of: {', '.join(choices)}"
        )
    return value


def _prompt_slugs(label: str) -> list[str]:
    return [item.strip() for item in _prompt(label).split(",") if item.strip()]


def _prompt_yes_no(label: str, default: bool) -> bool:
    marker = "Y/n" if default else "y/N"
    value = input(f"{label} [{marker}]: ").strip().lower()
    if not value:
        return default
    if value in {"y", "yes"}:
        return True
    if value in {"n", "no"}:
        return False
    raise ConfigurationError(f"{label} must be yes or no")
