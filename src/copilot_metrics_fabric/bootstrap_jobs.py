"""Plan and apply Fabric Data Pipeline jobs and managed schedules."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from copilot_metrics_fabric.config import AppConfig, ConfigurationError
from copilot_metrics_fabric.deployment import (
    DeploymentError,
    FabricClient,
    FabricJobFailure,
)

JOB_TYPE = "Pipeline"
JOB_STATE_FILE = ".ghcp-job-state.json"
JOB_STATE_VERSION = 2


@dataclass(frozen=True, slots=True)
class JobScope:
    kind: str
    slug: str

    @property
    def key(self) -> str:
        return f"{self.kind}:{self.slug}"


@dataclass(frozen=True, slots=True)
class JobPlanAction:
    operation: str
    resource: str
    scope: JobScope


@dataclass(frozen=True, slots=True)
class JobApplyResult:
    actions: tuple[JobPlanAction, ...]
    jobs: dict[str, str | dict[str, Any]]


@dataclass(frozen=True, slots=True)
class BackfillCompletionQuery:
    """Durable audit fields that identify a completed backfill."""

    audit_schema: str
    scope_kind: str
    scope_slug: str
    start_date: str
    end_date: str
    bootstrap_signature: str


class BackfillCompletionStore(Protocol):
    """Remote lookup for successful Gold audit records."""

    def is_complete(self, query: BackfillCompletionQuery) -> bool: ...


class AuditQueryExecutor(Protocol):
    """Execute a parameterized scalar query against the Fabric SQL endpoint."""

    def scalar_bool(
        self, statement: str, parameters: tuple[str, ...]
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class SqlBackfillCompletionStore:
    """Query the durable Gold completion row without owning SQL connectivity."""

    executor: AuditQueryExecutor

    def is_complete(self, query: BackfillCompletionQuery) -> bool:
        schema = query.audit_schema.replace("]", "]]" )
        statement = f"""
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM [{schema}].[pipeline_run_results]
    WHERE [stage] = 'gold'
      AND [status] = 'succeeded'
      AND [validation_status] = 'passed'
      AND [scope_kind] = ?
      AND [scope_slug] = ?
      AND [start_date] = ?
      AND [end_date] = ?
      AND JSON_VALUE([details_json], '$.run_mode') = 'backfill'
      AND JSON_VALUE([details_json], '$.bootstrap_signature') = ?
) THEN 1 ELSE 0 END
""".strip()
        return self.executor.scalar_bool(
            statement,
            (
                query.scope_kind,
                query.scope_slug,
                query.start_date,
                query.end_date,
                query.bootstrap_signature,
            ),
        )


def configured_scopes(config: AppConfig) -> tuple[JobScope, ...]:
    """Return one pipeline run scope for each configured GitHub scope."""
    if config.github.mode == "enterprise":
        if config.github.enterprise is None:
            raise ConfigurationError("github.enterprise is required")
        return (JobScope("enterprise", config.github.enterprise),)
    return tuple(
        JobScope("organization", organization)
        for organization in config.github.organizations
    )


def build_pipeline_parameters(
    config: AppConfig,
    scope: JobScope,
    *,
    run_mode: str,
) -> list[dict[str, Any]]:
    """Build typed Core Job Scheduler parameters for a pipeline invocation."""
    if run_mode not in {"backfill", "daily"}:
        raise ConfigurationError("run_mode must be backfill or daily")
    if not config.azure.key_vault_name:
        raise ConfigurationError(
            "azure.key_vault_name is required for Fabric pipeline jobs"
        )
    if run_mode == "backfill" and (
        config.backfill.start_date is None or config.backfill.end_date is None
    ):
        raise ConfigurationError("backfill dates are required")

    fabric = config.fabric
    values: list[tuple[str, Any, str]] = [
        ("run_mode", run_mode, "Text"),
        ("scope_kind", scope.kind, "Text"),
        ("scope_slug", scope.slug, "Text"),
        ("entity_type", "copilot_usage", "Text"),
        ("report_types", ",".join(fabric.report_types), "Text"),
        ("trailing_days", config.schedule.trailing_days, "Integer"),
        (
            "calculation_lookback_days",
            fabric.calculation_lookback_days,
            "Integer",
        ),
        (
            "start_date",
            config.backfill.start_date.isoformat()
            if run_mode == "backfill"
            else "",
            "Text",
        ),
        (
            "end_date",
            config.backfill.end_date.isoformat()
            if run_mode == "backfill"
            else "",
            "Text",
        ),
        (
            "earliest_date",
            fabric.earliest_date.isoformat() if fabric.earliest_date else "",
            "Text",
        ),
        ("lakehouse_files_root", fabric.lakehouse_files_root, "Text"),
        ("bronze_folder", fabric.bronze_folder, "Text"),
        ("silver_schema", fabric.silver_schema, "Text"),
        ("gold_schema", fabric.gold_schema, "Text"),
        ("audit_schema", fabric.audit_schema, "Text"),
        ("telemetry_lag_days", fabric.telemetry_lag_days, "Integer"),
        (
            "key_vault_uri",
            f"https://{config.azure.key_vault_name}.vault.azure.net/",
            "Text",
        ),
        (
            "github_token_secret_name",
            config.azure.github_token_secret_name,
            "Text",
        ),
    ]
    signature_payload = {name: value for name, value, _type in values}
    bootstrap_signature = (
        hashlib.sha256(
            json.dumps(
                signature_payload, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()
        if run_mode == "backfill"
        else ""
    )
    values.append(("bootstrap_signature", bootstrap_signature, "Text"))
    return [
        {"name": name, "value": value, "type": parameter_type}
        for name, value, parameter_type in values
    ]


class FabricJobBootstrap:
    """Read-only planning and idempotent apply for pipeline jobs."""

    def __init__(
        self,
        config: AppConfig,
        *,
        root: Path,
        client: FabricClient,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        state_root: Path | None = None,
        completion_store: BackfillCompletionStore | None = None,
    ) -> None:
        self.config = config
        self.root = root
        self.client = client
        self.now = now
        self.completion_store = completion_store
        self.state_path = (state_root or root) / JOB_STATE_FILE
        self.state = self._load_state()

    def plan(self) -> list[JobPlanAction]:
        workspace_id, pipeline_id = self._resolve_targets()
        actions: list[JobPlanAction] = []
        if self.config.backfill.enabled:
            for scope in configured_scopes(self.config):
                current = self._reconcile_backfill(
                    scope, workspace_id, pipeline_id
                )
                operation = self._backfill_operation(scope, current)
                actions.append(JobPlanAction(operation, "Backfill", scope))
        schedules = self._list_schedules(workspace_id, pipeline_id)
        for scope in configured_scopes(self.config):
            operation, current = self._schedule_operation(schedules, scope)
            if self.config.schedule.enabled:
                actions.append(
                    JobPlanAction(operation, "DailySchedule", scope)
                )
            elif current is not None and current.get("enabled") is not False:
                actions.append(JobPlanAction("disable", "DailySchedule", scope))
        return actions

    def apply(self, *, rerun_failed: bool = True) -> JobApplyResult:
        workspace_id, pipeline_id = self._resolve_targets()
        actions: list[JobPlanAction] = []
        jobs: dict[str, str | dict[str, Any]] = {}
        if self.config.backfill.enabled:
            for scope in configured_scopes(self.config):
                current = self._reconcile_backfill(
                    scope, workspace_id, pipeline_id
                )
                operation = self._backfill_operation(scope, current)
                if operation == "skip":
                    actions.append(JobPlanAction("skip", "Backfill", scope))
                    jobs[scope.key] = self._record_job_value(current)
                    continue
                if operation == "rerun" and not rerun_failed:
                    actions.append(JobPlanAction("failed", "Backfill", scope))
                    jobs[scope.key] = self._record_job_value(current)
                    continue
                parameters = build_pipeline_parameters(
                    self.config, scope, run_mode="backfill"
                )
                signature = self._backfill_signature(scope)
                try:
                    job = self.client.run_data_pipeline(
                        workspace_id,
                        pipeline_id,
                        parameters,
                        wait=self.config.backfill.wait_for_completion,
                    )
                except FabricJobFailure as error:
                    self._store_backfill(
                        scope,
                        signature=signature,
                        status=error.status,
                        job_id=error.job_id,
                        error=str(error),
                    )
                    raise
                jobs[scope.key] = job
                actions.append(JobPlanAction("run", "Backfill", scope))
                if isinstance(job, str):
                    self._store_backfill(
                        scope,
                        signature=signature,
                        status="pending",
                        job_id=job,
                    )
                else:
                    self._store_backfill(
                        scope,
                        signature=signature,
                        status="completed",
                        job_id=_optional_id(job),
                        job=job,
                    )

        schedules = self._list_schedules(workspace_id, pipeline_id)
        for scope in configured_scopes(self.config):
            operation, current = self._schedule_operation(schedules, scope)
            if self.config.schedule.enabled:
                body = self._schedule_body(scope, current)
                base = (
                    f"workspaces/{workspace_id}/items/{pipeline_id}/"
                    f"jobs/{JOB_TYPE}/schedules"
                )
                if operation == "create":
                    created = self.client.request(
                        "POST", base, body=body, expected=(201,)
                    )
                    schedule_id = _required_id(created, "schedule")
                    self.state.setdefault("schedules", {})[
                        scope.key
                    ] = schedule_id
                    self._save_state()
                elif operation == "update":
                    schedule_id = _required_id(current, "schedule")
                    self.client.request(
                        "PATCH",
                        f"{base}/{schedule_id}",
                        body=body,
                        expected=(200,),
                    )
                    self.state.setdefault("schedules", {})[
                        scope.key
                    ] = schedule_id
                    self._save_state()
                elif operation == "reuse" and current is not None:
                    self.state.setdefault("schedules", {})[
                        scope.key
                    ] = _required_id(current, "schedule")
                    self._save_state()
                actions.append(
                    JobPlanAction(operation, "DailySchedule", scope)
                )
            elif current is not None and current.get("enabled") is not False:
                schedule_id = _required_id(current, "schedule")
                base = (
                    f"workspaces/{workspace_id}/items/{pipeline_id}/"
                    f"jobs/{JOB_TYPE}/schedules/{schedule_id}"
                )
                disabled = {
                    "enabled": False,
                    "configuration": current.get("configuration", {}),
                    "executionData": current.get("executionData", {}),
                }
                self.client.request(
                    "PATCH", base, body=disabled, expected=(200,)
                )
                actions.append(
                    JobPlanAction("disable", "DailySchedule", scope)
                )
        return JobApplyResult(tuple(actions), jobs)

    def status(self) -> dict[str, dict[str, Any]]:
        """Reconcile and return persisted backfill job status by scope."""
        workspace_id, pipeline_id = self._resolve_targets()
        for scope in configured_scopes(self.config):
            if self.config.backfill.enabled:
                self._reconcile_backfill(scope, workspace_id, pipeline_id)
        return _json_safe(self.state.get("backfills", {}))

    def _resolve_targets(self) -> tuple[str, str]:
        workspace_name = self.config.fabric.workspace_name
        if not workspace_name:
            raise ConfigurationError("fabric.workspace_name is required")
        workspaces = self._list("workspaces")
        workspace = _unique_named(workspaces, workspace_name, "Workspace")
        if workspace is None:
            raise DeploymentError(f"workspace {workspace_name!r} does not exist")
        workspace_id = _required_id(workspace, "workspace")
        items = self._list(
            f"workspaces/{workspace_id}/items?type=DataPipeline"
        )
        pipeline = _unique_named(
            items, self.config.fabric.pipeline_name, "DataPipeline"
        )
        if pipeline is None:
            raise DeploymentError(
                f"pipeline {self.config.fabric.pipeline_name!r} does not exist"
            )
        return workspace_id, _required_id(pipeline, "pipeline")

    def _list_schedules(
        self, workspace_id: str, pipeline_id: str
    ) -> list[dict[str, Any]]:
        return self._list(
            f"workspaces/{workspace_id}/items/{pipeline_id}/"
            f"jobs/{JOB_TYPE}/schedules"
        )

    def _list(self, path: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        next_path: str | None = path
        while next_path:
            payload = self.client.request("GET", next_path, expected=(200,))
            if not isinstance(payload, dict) or not isinstance(
                payload.get("value", []), list
            ):
                raise DeploymentError(f"invalid Fabric list response for {path}")
            results.extend(
                item
                for item in payload.get("value", [])
                if isinstance(item, dict)
            )
            next_path = payload.get("continuationUri")
            if not next_path and payload.get("continuationToken"):
                separator = "&" if "?" in path else "?"
                next_path = (
                    f"{path}{separator}continuationToken="
                    f"{payload['continuationToken']}"
                )
        return results

    def _schedule_operation(
        self, schedules: Iterable[dict[str, Any]], scope: JobScope
    ) -> tuple[str, dict[str, Any] | None]:
        managed = [
            schedule
            for schedule in schedules
            if _is_managed_daily_schedule(schedule, self.config, scope)
        ]
        if len(managed) > 1:
            raise DeploymentError(
                f"multiple managed daily schedules exist for {scope.key}"
            )
        if not managed:
            return "create", None
        current = managed[0]
        desired = self._schedule_body(scope, current)
        comparable = {
            key: current.get(key)
            for key in ("enabled", "configuration", "executionData")
        }
        return ("reuse" if comparable == desired else "update"), current

    def _schedule_body(
        self, scope: JobScope, current: dict[str, Any] | None
    ) -> dict[str, Any]:
        current_configuration = (
            current.get("configuration", {}) if current else {}
        )
        start = current_configuration.get("startDateTime")
        end = current_configuration.get("endDateTime")
        if not start:
            start = (
                self.now().astimezone(UTC) + timedelta(days=1)
            ).strftime("%Y-%m-%dT00:00:00Z")
        if not end:
            end = "2099-12-31T23:59:59Z"
        return {
            "enabled": True,
            "configuration": {
                "startDateTime": start,
                "endDateTime": end,
                "localTimeZoneId": self.config.schedule.timezone,
                "type": "Daily",
                "times": [self.config.schedule.time],
            },
            "executionData": {
                "parameters": _parameter_mapping(
                    build_pipeline_parameters(
                        self.config, scope, run_mode="daily"
                    )
                )
            },
        }

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {
                "version": JOB_STATE_VERSION,
                "schedules": {},
                "backfills": {},
            }
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError(
                f"cannot read job state {self.state_path}: {error}"
            ) from error
        if not isinstance(value, dict) or value.get("version") not in {
            1,
            JOB_STATE_VERSION,
        }:
            raise DeploymentError(
                f"unsupported job state in {self.state_path}"
            )
        value.setdefault("schedules", {})
        value.setdefault("backfills", {})
        if value.get("version") == 1:
            value = self._migrate_state(value)
            self._write_state(value)
        return value

    def _save_state(self) -> None:
        self._write_state(self.state)

    def _write_state(self, value: dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".json.new")
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.state_path)

    def _migrate_state(self, value: dict[str, Any]) -> dict[str, Any]:
        migrated = {
            "version": JOB_STATE_VERSION,
            "schedules": value.get("schedules", {}),
            "backfills": {},
        }
        for scope_key, raw in value.get("backfills", {}).items():
            if not isinstance(raw, dict):
                continue
            record = dict(raw)
            job = record.get("job")
            if isinstance(job, str):
                record.update({"status": "pending", "job_id": job})
                record.pop("job", None)
            elif isinstance(job, dict):
                status = str(job.get("status", "completed")).lower()
                record["status"] = (
                    "completed"
                    if status in {"completed", "succeeded"}
                    else status
                )
                job_id = _optional_id(job)
                if job_id:
                    record["job_id"] = job_id
            else:
                record["status"] = "completed"
            migrated["backfills"][scope_key] = record
        return migrated

    def _backfill_signature(self, scope: JobScope) -> str:
        parameters = _parameter_mapping(
            build_pipeline_parameters(
                self.config, scope, run_mode="backfill"
            )
        )
        value = parameters.get("bootstrap_signature")
        if not isinstance(value, str) or not value:
            raise DeploymentError("backfill bootstrap signature is missing")
        return value

    def _reconcile_backfill(
        self,
        scope: JobScope,
        workspace_id: str,
        pipeline_id: str,
    ) -> dict[str, Any]:
        signature = self._backfill_signature(scope)
        current = self.state.get("backfills", {}).get(scope.key, {})
        if (
            current.get("signature") == signature
            and current.get("status") == "completed"
        ):
            return current
        if self._remote_backfill_is_complete(scope):
            return self._store_backfill(
                scope,
                signature=signature,
                status="completed",
                job_id=None,
                source="audit.pipeline_run_results",
            )
        if (
            current.get("signature") != signature
            or current.get("status") != "pending"
        ):
            return current
        job_id = current.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            raise DeploymentError(
                f"pending backfill for {scope.key} is missing a Fabric job id"
            )
        try:
            job = self.client.wait_for_data_pipeline_job(
                workspace_id, pipeline_id, job_id
            )
        except FabricJobFailure as error:
            return self._store_backfill(
                scope,
                signature=current["signature"],
                status=error.status,
                job_id=job_id,
                error=str(error),
            )
        return self._store_backfill(
            scope,
            signature=current["signature"],
            status="completed",
            job_id=job_id,
            job=job,
        )

    def _remote_backfill_is_complete(self, scope: JobScope) -> bool:
        if self.completion_store is None:
            return False
        start = self.config.backfill.start_date
        end = self.config.backfill.end_date
        if start is None or end is None:
            return False
        return self.completion_store.is_complete(
            BackfillCompletionQuery(
                audit_schema=self.config.fabric.audit_schema,
                scope_kind=scope.kind,
                scope_slug=scope.slug,
                start_date=start.isoformat(),
                end_date=end.isoformat(),
                bootstrap_signature=self._backfill_signature(scope),
            )
        )

    def _backfill_operation(
        self, scope: JobScope, current: dict[str, Any]
    ) -> str:
        if current.get("signature") != self._backfill_signature(scope):
            return "run"
        status = current.get("status")
        if status == "completed":
            return "skip"
        if status in {"failed", "cancelled", "canceled"}:
            return "rerun"
        if status == "pending":
            return "pending"
        return "run"

    def _store_backfill(
        self,
        scope: JobScope,
        *,
        signature: str,
        status: str,
        job_id: str | None,
        job: dict[str, Any] | None = None,
        error: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "signature": signature,
            "status": status,
            "updated_at": self.now().astimezone(UTC).isoformat(),
        }
        if job_id:
            record["job_id"] = job_id
        if job is not None:
            record["job"] = _json_safe(job)
        if error:
            record["error"] = error
        if source:
            record["source"] = source
        self.state.setdefault("backfills", {})[scope.key] = record
        self._save_state()
        return record

    @staticmethod
    def _record_job_value(record: dict[str, Any]) -> str | dict[str, Any]:
        job = record.get("job")
        if isinstance(job, dict):
            return _json_safe(job)
        job_id = record.get("job_id")
        if isinstance(job_id, str):
            if record.get("status") == "pending":
                return job_id
            result: dict[str, Any] = {
                "id": job_id,
                "status": record.get("status"),
            }
            if record.get("error"):
                result["error"] = record["error"]
            return result
        return {"status": record.get("status", "unknown")}


def _is_managed_daily_schedule(
    schedule: dict[str, Any],
    config: AppConfig,
    scope: JobScope,
) -> bool:
    """Recognize ownership only from fields Fabric accepts and returns."""
    execution_data = schedule.get("executionData")
    if not isinstance(execution_data, dict):
        return False
    parameters = execution_data.get("parameters")
    if not isinstance(parameters, dict):
        return False
    expected = _parameter_mapping(
        build_pipeline_parameters(config, scope, run_mode="daily")
    )
    return (
        set(parameters) == set(expected)
        and parameters.get("run_mode") == "daily"
        and parameters.get("scope_kind") == scope.kind
        and parameters.get("scope_slug") == scope.slug
    )


def _parameter_mapping(
    parameters: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    return {
        str(parameter["name"]): parameter.get("value")
        for parameter in parameters
    }


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _unique_named(
    items: Iterable[dict[str, Any]], name: str, kind: str
) -> dict[str, Any] | None:
    matches = [item for item in items if item.get("displayName") == name]
    if len(matches) > 1:
        raise DeploymentError(f"multiple {kind} items have display name {name!r}")
    return matches[0] if matches else None


def _required_id(value: dict[str, Any] | None, kind: str) -> str:
    item_id = value.get("id") if value else None
    if not isinstance(item_id, str) or not item_id:
        raise DeploymentError(f"Fabric {kind} response is missing an id")
    return item_id


def _optional_id(value: dict[str, Any]) -> str | None:
    item_id = value.get("id")
    return item_id if isinstance(item_id, str) and item_id else None
