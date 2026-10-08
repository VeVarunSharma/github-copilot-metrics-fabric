"""End-to-end, resumable bootstrap orchestration."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from copilot_metrics_fabric.azure_bootstrap import (
    AzureBootstrapService,
    AzureCli,
    BootstrapAction,
)
from copilot_metrics_fabric.bootstrap_jobs import (
    BackfillCompletionStore,
    FabricJobBootstrap,
    JobApplyResult,
    JobPlanAction,
    configured_scopes,
)
from copilot_metrics_fabric.config import AppConfig
from copilot_metrics_fabric.deployment import (
    FabricClient,
    FabricDeployer,
    PlanAction,
)

BOOTSTRAP_STATE_FILE = ".ghcp-bootstrap-state.json"


class BootstrapError(RuntimeError):
    """Raised when the integrated bootstrap cannot complete safely."""


@dataclass(frozen=True, slots=True)
class BootstrapPlan:
    azure: tuple[BootstrapAction, ...]
    fabric: tuple[PlanAction, ...]
    jobs: tuple[JobPlanAction, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "azure": [asdict(action) for action in self.azure],
            "fabric": [asdict(action) for action in self.fabric],
            "jobs": [
                {
                    "operation": action.operation,
                    "resource": action.resource,
                    "scope": asdict(action.scope),
                }
                for action in self.jobs
            ],
        }


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    plan: BootstrapPlan
    jobs: dict[str, str | dict[str, Any]]
    links: dict[str, str]


class BootstrapOrchestrator:
    """Coordinate Azure, Fabric deployment, initial jobs, and schedules."""

    def __init__(
        self,
        config: AppConfig,
        *,
        root: Path,
        config_path: Path,
        fabric_client: FabricClient | None = None,
        azure_cli: AzureCli | None = None,
        token_provider: Callable[[], str] | None = None,
        state_root: Path | None = None,
        backfill_completion_store: BackfillCompletionStore | None = None,
    ) -> None:
        self.config = config
        self.root = root
        self.config_path = config_path
        self.state_root = state_root or (
            root if (root / "pyproject.toml").is_file() else config_path.parent
        )
        self.state_path = self.state_root / BOOTSTRAP_STATE_FILE
        self.fabric_client = fabric_client or FabricClient()
        self.azure_cli = azure_cli or AzureCli(
            secret_directory=self.state_root
        )
        self.token_provider = token_provider
        self.backfill_completion_store = backfill_completion_store

    def plan(self) -> BootstrapPlan:
        azure = AzureBootstrapService(
            self.config, cli=self.azure_cli
        ).plan(None)
        deployer = self._deployer()
        fabric = tuple(deployer.plan())
        requires_pipeline_creation = any(
            action.operation == "create"
            and action.item_type in {"Workspace", "DataPipeline"}
            for action in fabric
        )
        if requires_pipeline_creation:
            jobs = self._configured_job_plan()
        else:
            jobs = tuple(self._jobs().plan())
        return BootstrapPlan(tuple(azure.actions), fabric, jobs)

    def apply(self, *, resume: bool = False) -> BootstrapResult:
        state = self._load_state()
        completed = set(state.get("completed_phases", []))
        azure_actions: tuple[BootstrapAction, ...] = ()
        fabric_actions: tuple[PlanAction, ...] = ()
        job_actions: tuple[JobPlanAction, ...] = ()
        jobs: dict[str, str | dict[str, Any]] = {}

        if not resume or "azure" not in completed:
            if self.token_provider is None:
                raise BootstrapError("a secure GitHub token provider is required")
            azure = AzureBootstrapService(
                self.config, cli=self.azure_cli
            ).apply(self.token_provider)
            azure_actions = tuple(azure.actions)
            self._complete_phase(state, "azure")
            completed.add("azure")

        if not resume or "fabric" not in completed:
            fabric_actions = tuple(self._deployer().deploy())
            self._complete_phase(state, "fabric")
            completed.add("fabric")

        applied: JobApplyResult = self._jobs().apply(
            rerun_failed=not resume
        )
        job_actions = applied.actions
        jobs = applied.jobs
        state["jobs"] = _json_safe(jobs)
        if "jobs" not in completed:
            self._complete_phase(state, "jobs")

        links = self._resource_links()
        state["links"] = links
        self._save_state(state)
        return BootstrapResult(
            BootstrapPlan(azure_actions, fabric_actions, job_actions),
            jobs,
            links,
        )

    def status(self) -> dict[str, Any]:
        state = self._load_state()
        jobs = self._jobs().status()
        state["jobs"] = _json_safe(jobs)
        self._save_state(state)
        return {
            "completed_phases": list(state.get("completed_phases", [])),
            "jobs": jobs,
            "links": state.get("links", self._resource_links()),
            "state_file": str(self.state_path),
        }

    def _deployer(self) -> FabricDeployer:
        options: dict[str, Path] = {}
        if self.state_root != self.root:
            options["state_root"] = self.state_root
        return FabricDeployer(
            self.config,
            root=self.root,
            client=self.fabric_client,
            **options,
        )

    def _jobs(self) -> FabricJobBootstrap:
        return FabricJobBootstrap(
            self.config,
            root=self.root,
            client=self.fabric_client,
            state_root=self.state_root,
            completion_store=self.backfill_completion_store,
        )

    def _configured_job_plan(self) -> tuple[JobPlanAction, ...]:
        actions: list[JobPlanAction] = []
        for scope in configured_scopes(self.config):
            if self.config.backfill.enabled:
                actions.append(JobPlanAction("run", "Backfill", scope))
            if self.config.schedule.enabled:
                actions.append(
                    JobPlanAction("create-or-update", "DailySchedule", scope)
                )
        return tuple(actions)

    def _resource_links(self) -> dict[str, str]:
        deployment_state = self.state_root / self.config.fabric.state_file
        try:
            value = json.loads(deployment_state.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        workspace_id = value.get("workspace_id")
        items = value.get("items", {})
        if not isinstance(workspace_id, str) or not isinstance(items, dict):
            return {}
        base = f"https://app.fabric.microsoft.com/groups/{workspace_id}"
        links = {"workspace": f"{base}/list"}
        type_routes = {
            "Lakehouse": "lakehouses",
            "DataPipeline": "pipelines",
            "SemanticModel": "datasets",
            "Report": "reports",
            "Environment": "environments",
        }
        for key, record in items.items():
            if not isinstance(record, dict) or not isinstance(record.get("id"), str):
                continue
            item_type, _, _name = key.partition(":")
            route = type_routes.get(item_type)
            if route:
                links[item_type.lower()] = f"{base}/{route}/{record['id']}"
        return links

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"version": 1, "completed_phases": []}
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise BootstrapError(
                f"cannot read bootstrap state {self.state_path}: {error}"
            ) from error
        if not isinstance(value, dict) or value.get("version") != 1:
            raise BootstrapError(
                f"unsupported bootstrap state in {self.state_path}"
            )
        value.setdefault("completed_phases", [])
        return value

    def _complete_phase(self, state: dict[str, Any], phase: str) -> None:
        phases = state.setdefault("completed_phases", [])
        if phase not in phases:
            phases.append(phase)
        self._save_state(state)

    def _save_state(self, state: dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".json.new")
        temporary.write_text(
            json.dumps(state, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.state_path)


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))
