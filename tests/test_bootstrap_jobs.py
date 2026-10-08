from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from copilot_metrics_fabric.bootstrap_jobs import (
    JOB_STATE_FILE,
    BackfillCompletionQuery,
    FabricJobBootstrap,
    JobScope,
    SqlBackfillCompletionStore,
    build_pipeline_parameters,
)
from copilot_metrics_fabric.config import parse_config
from copilot_metrics_fabric.deployment import (
    TELEMETRY_HEADER,
    TELEMETRY_VALUE,
    DeploymentError,
    FabricClient,
)


@dataclass
class Token:
    token: str = "token"


class Credential:
    def get_token(self, scope: str) -> Token:
        return Token()


class Response:
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
    def __init__(self, responses: list[Response]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def request(self, method, url, *, headers, json, timeout):
        self.calls.append(
            {"method": method, "url": url, "headers": headers, "json": json}
        )
        return self.responses.pop(0)


class QueryExecutor:
    def __init__(self, result: bool) -> None:
        self.result = result
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def scalar_bool(
        self, statement: str, parameters: tuple[str, ...]
    ) -> bool:
        self.calls.append((statement, parameters))
        return self.result


class CompletionStore:
    def __init__(self, completed: bool = False) -> None:
        self.completed = completed
        self.queries: list[BackfillCompletionQuery] = []

    def is_complete(self, query: BackfillCompletionQuery) -> bool:
        self.queries.append(query)
        return self.completed


class JobsRouter:
    def __init__(
        self,
        schedules: list[dict[str, Any]] | None = None,
        job_statuses: list[dict[str, Any]] | None = None,
    ) -> None:
        self.schedules = schedules or []
        self.job_statuses = job_statuses or []
        self.calls: list[dict[str, Any]] = []

    def request(self, method, url, *, headers, json, timeout):
        assert headers[TELEMETRY_HEADER] == TELEMETRY_VALUE
        path = url.split("/v1/", 1)[1]
        self.calls.append({"method": method, "path": path, "json": json})
        if method == "GET" and path == "workspaces":
            return Response(
                200, {"value": [{"id": "workspace", "displayName": "Metrics"}]}
            )
        if method == "GET" and path.endswith("items?type=DataPipeline"):
            return Response(
                200,
                {
                    "value": [
                        {
                            "id": "pipeline",
                            "displayName": "pipeline",
                            "type": "DataPipeline",
                        }
                    ]
                },
            )
        if method == "GET" and path.endswith("/jobs/Pipeline/schedules"):
            return Response(200, {"value": self.schedules})
        if method == "GET" and "/jobs/Pipeline/instances/" in path:
            return Response(200, self.job_statuses.pop(0))
        if method == "POST" and path.endswith("/jobs/Pipeline/instances"):
            return Response(
                202,
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/workspaces/"
                        "workspace/items/pipeline/jobs/instances/job-42"
                    )
                },
            )
        if method == "POST" and path.endswith("/jobs/Pipeline/schedules"):
            created = {"id": "created", **json}
            self.schedules.append(created)
            return Response(201, created)
        if method == "PATCH" and "/jobs/Pipeline/schedules/" in path:
            schedule_id = path.rsplit("/", 1)[-1]
            current = next(item for item in self.schedules if item["id"] == schedule_id)
            current.update(json)
            return Response(200, current)
        raise AssertionError(f"unexpected request: {method} {path}")


def config(*, wait: bool = True):
    return parse_config(
        {
            "schema_version": 1,
            "github": {
                "mode": "organization",
                "organizations": ["example-org"],
            },
            "azure": {
                "key_vault_name": "metrics-vault",
                "github_token_secret_name": "github-token",
            },
            "fabric": {
                "workspace_name": "Metrics",
                "lakehouse_name": "Lakehouse",
                "pipeline_name": "pipeline",
                "report_types": ["entity", "repositories"],
                "calculation_lookback_days": 14,
                "earliest_date": "2026-01-01",
                "lakehouse_files_root": "/lakehouse/default/Files",
                "bronze_folder": "raw-bronze",
                "silver_schema": "silver",
                "gold_schema": "gold",
                "audit_schema": "audit",
                "telemetry_lag_days": 3,
            },
            "backfill": {
                "enabled": True,
                "start_date": "2026-09-01",
                "end_date": "2026-09-30",
                "wait_for_completion": wait,
            },
            "schedule": {
                "enabled": True,
                "time": "03:30",
                "timezone": "UTC+12",
                "trailing_days": 21,
            },
        }
    )


def schedule_config():
    return parse_config(
        {
            "schema_version": 1,
            "github": {
                "mode": "organization",
                "organizations": ["example-org"],
            },
            "azure": {"key_vault_name": "metrics-vault"},
            "fabric": {
                "workspace_name": "Metrics",
                "lakehouse_name": "Lakehouse",
                "pipeline_name": "pipeline",
            },
            "schedule": {"enabled": True},
        }
    )


def client(transport, **kwargs) -> FabricClient:
    return FabricClient(
        credential=Credential(),
        transport=transport,
        sleep=kwargs.pop("sleep", lambda _: None),
        poll_interval=0,
        **kwargs,
    )


def parameter_map(parameters: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {parameter["name"]: parameter for parameter in parameters}


def managed_schedule(body: dict[str, Any], schedule_id: str = "managed"):
    return {"id": schedule_id, **body}


def write_job_state(tmp_path: Path, schedule_id: str) -> None:
    (tmp_path / JOB_STATE_FILE).write_text(
        json.dumps(
            {
                "version": 1,
                "schedules": {
                    "organization:example-org": schedule_id,
                },
            }
        ),
        encoding="utf-8",
    )


def test_sql_completion_store_uses_durable_gold_audit_contract() -> None:
    executor = QueryExecutor(True)
    query = BackfillCompletionQuery(
        audit_schema="audit",
        scope_kind="organization",
        scope_slug="example-org",
        start_date="2026-09-01",
        end_date="2026-09-30",
        bootstrap_signature="abc123",
    )

    assert SqlBackfillCompletionStore(executor).is_complete(query) is True

    statement, parameters = executor.calls[0]
    assert "[audit].[pipeline_run_results]" in statement
    assert "[stage] = 'gold'" in statement
    assert "[status] = 'succeeded'" in statement
    assert "$.run_mode" in statement
    assert "$.bootstrap_signature" in statement
    assert parameters == (
        "organization",
        "example-org",
        "2026-09-01",
        "2026-09-30",
        "abc123",
    )


def test_builds_typed_backfill_and_daily_parameters() -> None:
    scope = JobScope("organization", "example-org")

    backfill = parameter_map(
        build_pipeline_parameters(config(), scope, run_mode="backfill")
    )
    daily = parameter_map(
        build_pipeline_parameters(config(), scope, run_mode="daily")
    )

    assert backfill["scope_slug"]["value"] == "example-org"
    assert backfill["report_types"]["value"] == "entity,repositories"
    assert backfill["calculation_lookback_days"] == {
        "name": "calculation_lookback_days",
        "value": 14,
        "type": "Integer",
    }
    assert backfill["start_date"]["value"] == "2026-09-01"
    assert len(backfill["bootstrap_signature"]["value"]) == 64
    assert daily["bootstrap_signature"]["value"] == ""
    assert daily["start_date"]["value"] == ""
    assert daily["trailing_days"]["value"] == 21
    assert daily["earliest_date"]["value"] == "2026-01-01"
    assert daily["key_vault_uri"]["value"] == (
        "https://metrics-vault.vault.azure.net/"
    )
    assert daily["github_token_secret_name"]["value"] == "github-token"
    assert daily["bronze_folder"]["value"] == "raw-bronze"
    assert daily["silver_schema"]["value"] == "silver"


def test_run_202_respects_retry_after_and_returns_job_id() -> None:
    location = (
        "https://api.fabric.microsoft.com/v1/workspaces/w/items/p/"
        "jobs/instances/job-1"
    )
    sleeps: list[float] = []
    transport = QueueTransport(
        [Response(202, headers={"Location": location, "Retry-After": "7"})]
    )
    fabric = client(transport, sleep=sleeps.append)
    parameters = [{"name": "run_mode", "value": "daily", "type": "Text"}]

    result = fabric.run_data_pipeline("w", "p", parameters, wait=False)

    assert result == "job-1"
    assert sleeps == []
    assert transport.calls[0]["json"] == {
        "executionData": {"parameters": {"run_mode": "daily"}}
    }
    assert transport.calls[0]["headers"][TELEMETRY_HEADER] == TELEMETRY_VALUE


def test_job_polling_completed_uses_retry_after_and_telemetry() -> None:
    location = (
        "https://api.fabric.microsoft.com/v1/workspaces/w/items/p/"
        "jobs/instances/job-1"
    )
    sleeps: list[float] = []
    transport = QueueTransport(
        [
            Response(202, headers={"Location": location, "Retry-After": "4"}),
            Response(
                200,
                {"id": "job-1", "status": "InProgress"},
                {"Retry-After": "2"},
            ),
            Response(200, {"id": "job-1", "status": "Completed"}),
        ]
    )

    result = client(transport, sleep=sleeps.append).run_data_pipeline(
        "w", "p", [], wait=True
    )

    assert result["status"] == "Completed"
    assert sleeps == [4.0, 2.0]
    assert all(
        call["headers"][TELEMETRY_HEADER] == TELEMETRY_VALUE
        for call in transport.calls
    )


@pytest.mark.parametrize("status", ["Failed", "Cancelled"])
def test_job_terminal_failures_are_redacted(status: str) -> None:
    location = (
        "https://api.fabric.microsoft.com/v1/workspaces/w/items/p/"
        "jobs/instances/job-1"
    )
    transport = QueueTransport(
        [
            Response(202, headers={"Location": location}),
            Response(
                200,
                {
                    "status": status,
                    "failureReason": {
                        "errorCode": "PipelineError",
                        "message": (
                            "token=super-secret "
                            "https://files.test/a?signature=signed-secret"
                        ),
                    },
                },
            ),
        ]
    )

    with pytest.raises(DeploymentError) as caught:
        client(transport).run_data_pipeline("w", "p", [], wait=True)

    message = str(caught.value)
    assert "PipelineError" in message
    assert "super-secret" not in message
    assert "signed-secret" not in message


def test_job_polling_timeout_is_bounded() -> None:
    location = (
        "https://api.fabric.microsoft.com/v1/workspaces/w/items/p/"
        "jobs/instances/job-1"
    )
    transport = QueueTransport(
        [
            Response(202, headers={"Location": location}),
            Response(200, {"status": "InProgress"}),
            Response(200, {"status": "InProgress"}),
        ]
    )

    with pytest.raises(DeploymentError, match="after 2 polls"):
        client(transport, max_lro_polls=2).run_data_pipeline(
            "w", "p", [], wait=True
        )


def test_plan_is_read_only_and_reports_schedule_create(tmp_path: Path) -> None:
    router = JobsRouter()
    bootstrap = FabricJobBootstrap(
        config(),
        root=tmp_path,
        client=client(router),
        now=lambda: datetime(2026, 10, 3, tzinfo=UTC),
    )

    actions = bootstrap.plan()

    assert [action.operation for action in actions] == ["run", "create"]
    assert all(call["method"] == "GET" for call in router.calls)


def test_schedule_payload_uses_validated_fabric_timezone(tmp_path: Path) -> None:
    router = JobsRouter()

    FabricJobBootstrap(
        config(wait=False),
        root=tmp_path,
        client=client(router),
        now=lambda: datetime(2026, 10, 3, tzinfo=UTC),
    ).apply()

    schedule_call = next(
        call
        for call in router.calls
        if call["path"].endswith("/jobs/Pipeline/schedules")
        and call["method"] == "POST"
    )
    assert schedule_call["json"]["configuration"]["localTimeZoneId"] == "UTC+12"


def test_fresh_runner_uses_remote_backfill_completion_without_duplicate(
    tmp_path: Path,
) -> None:
    router = JobsRouter()

    result = FabricJobBootstrap(
        config(wait=False),
        root=tmp_path,
        client=client(router),
        now=lambda: datetime(2026, 10, 3, tzinfo=UTC),
    ).apply()

    assert result.jobs == {"organization:example-org": "job-42"}
    run_call = next(
        call
        for call in router.calls
        if call["path"].endswith("/jobs/Pipeline/instances")
    )
    assert run_call["json"]["executionData"]["parameters"]["run_mode"] == (
        "backfill"
    )

    (tmp_path / JOB_STATE_FILE).unlink(missing_ok=True)
    completion_store = CompletionStore(completed=True)
    second_router = JobsRouter(router.schedules)
    second = FabricJobBootstrap(
        config(wait=False),
        root=tmp_path,
        client=client(second_router),
        now=lambda: datetime(2026, 10, 3, tzinfo=UTC),
        completion_store=completion_store,
    ).apply()

    assert second.actions[0].operation == "skip"
    assert completion_store.queries == [
        BackfillCompletionQuery(
            audit_schema="audit",
            scope_kind="organization",
            scope_slug="example-org",
            start_date="2026-09-01",
            end_date="2026-09-30",
            bootstrap_signature=parameter_map(
                build_pipeline_parameters(
                    config(wait=False),
                    JobScope("organization", "example-org"),
                    run_mode="backfill",
                )
            )["bootstrap_signature"]["value"],
        )
    ]
    assert not any(
        call["path"].endswith("/jobs/Pipeline/instances")
        for call in second_router.calls
    )


def test_async_backfill_stays_pending_then_completes_without_resubmission(
    tmp_path: Path,
) -> None:
    first_router = JobsRouter()
    FabricJobBootstrap(
        config(wait=False), root=tmp_path, client=client(first_router)
    ).apply()

    pending = json.loads((tmp_path / JOB_STATE_FILE).read_text(encoding="utf-8"))
    record = pending["backfills"]["organization:example-org"]
    assert record["status"] == "pending"
    assert record["job_id"] == "job-42"

    second_router = JobsRouter(
        first_router.schedules,
        [{"id": "job-42", "status": "Completed"}],
    )
    result = FabricJobBootstrap(
        config(wait=False), root=tmp_path, client=client(second_router)
    ).apply()

    assert result.actions[0].operation == "skip"
    assert not any(
        call["method"] == "POST"
        and call["path"].endswith("/jobs/Pipeline/instances")
        for call in second_router.calls
    )
    completed = json.loads(
        (tmp_path / JOB_STATE_FILE).read_text(encoding="utf-8")
    )
    assert completed["backfills"]["organization:example-org"]["status"] == (
        "completed"
    )


@pytest.mark.parametrize("terminal_status", ["Failed", "Cancelled"])
def test_async_failure_is_surfaced_and_requires_explicit_rerun(
    tmp_path: Path, terminal_status: str
) -> None:
    first_router = JobsRouter()
    FabricJobBootstrap(
        config(wait=False), root=tmp_path, client=client(first_router)
    ).apply()
    failed_router = JobsRouter(
        first_router.schedules,
        [
            {
                "id": "job-42",
                "status": terminal_status,
                "failureReason": {"errorCode": "PipelineFailed"},
            }
        ],
    )
    resumed = FabricJobBootstrap(
        config(wait=False), root=tmp_path, client=client(failed_router)
    ).apply(rerun_failed=False)

    assert resumed.actions[0].operation == "failed"
    assert resumed.jobs["organization:example-org"]["status"] == (
        terminal_status.lower()
    )
    assert not any(
        call["method"] == "POST"
        and call["path"].endswith("/jobs/Pipeline/instances")
        for call in failed_router.calls
    )

    rerun_router = JobsRouter(failed_router.schedules)
    rerun = FabricJobBootstrap(
        config(wait=False), root=tmp_path, client=client(rerun_router)
    ).apply()
    assert rerun.actions[0].operation == "run"
    assert any(
        call["method"] == "POST"
        and call["path"].endswith("/jobs/Pipeline/instances")
        for call in rerun_router.calls
    )


def test_pending_backfill_timeout_remains_pending_and_is_not_resubmitted(
    tmp_path: Path,
) -> None:
    first_router = JobsRouter()
    FabricJobBootstrap(
        config(wait=False), root=tmp_path, client=client(first_router)
    ).apply()
    timeout_router = JobsRouter(
        first_router.schedules,
        [{"status": "InProgress"}, {"status": "InProgress"}],
    )

    with pytest.raises(DeploymentError, match="after 2 polls"):
        FabricJobBootstrap(
            config(wait=False),
            root=tmp_path,
            client=client(timeout_router, max_lro_polls=2),
        ).apply()

    assert not any(
        call["method"] == "POST"
        and call["path"].endswith("/jobs/Pipeline/instances")
        for call in timeout_router.calls
    )
    state = json.loads((tmp_path / JOB_STATE_FILE).read_text(encoding="utf-8"))
    assert state["backfills"]["organization:example-org"]["status"] == "pending"


def test_version_one_async_state_migrates_to_pending(tmp_path: Path) -> None:
    state_path = tmp_path / JOB_STATE_FILE
    bootstrap = FabricJobBootstrap(
        config(wait=False), root=tmp_path, client=client(JobsRouter())
    )
    signature = bootstrap._backfill_signature(
        JobScope("organization", "example-org")
    )
    state_path.write_text(
        json.dumps(
            {
                "version": 1,
                "schedules": {},
                "backfills": {
                    "organization:example-org": {
                        "signature": signature,
                        "job": "job-42",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    FabricJobBootstrap(
        config(wait=False),
        root=tmp_path,
        client=client(
            JobsRouter(job_statuses=[{"id": "job-42", "status": "Completed"}])
        ),
    ).status()

    migrated = json.loads(state_path.read_text(encoding="utf-8"))
    assert migrated["version"] == 2
    assert migrated["backfills"]["organization:example-org"]["status"] == (
        "completed"
    )


def test_schedule_create_preserves_unrelated_schedule(tmp_path: Path) -> None:
    user_schedule = {
        "id": "user",
        "enabled": True,
        "configuration": {"type": "Daily", "times": ["03:30"]},
    }
    router = JobsRouter([user_schedule])

    result = FabricJobBootstrap(
        schedule_config(),
        root=tmp_path,
        client=client(router),
        now=lambda: datetime(2026, 10, 3, tzinfo=UTC),
    ).apply()

    assert result.actions[0].operation == "create"
    assert router.schedules[0] == user_schedule
    assert len(router.schedules) == 2
    assert router.schedules[1]["executionData"]["parameters"]["scope_slug"] == (
        "example-org"
    )


def test_stale_local_schedule_id_does_not_claim_unrelated_schedule(
    tmp_path: Path,
) -> None:
    write_job_state(tmp_path, "user")
    user_schedule = {
        "id": "user",
        "enabled": True,
        "configuration": {"type": "Daily", "times": ["03:30"]},
        "executionData": {"parameters": {"run_mode": "daily"}},
    }
    router = JobsRouter([user_schedule])

    result = FabricJobBootstrap(
        schedule_config(), root=tmp_path, client=client(router)
    ).apply()

    assert result.actions[-1].operation == "create"
    assert router.schedules[0] == user_schedule
    assert len(router.schedules) == 2


def test_fresh_runner_discovers_schedule_and_updates_it(tmp_path: Path) -> None:
    bootstrap = FabricJobBootstrap(
        schedule_config(),
        root=tmp_path,
        client=client(JobsRouter()),
        now=lambda: datetime(2026, 10, 3, tzinfo=UTC),
    )
    desired = bootstrap._schedule_body(
        JobScope("organization", "example-org"), None
    )

    reuse_router = JobsRouter([managed_schedule(desired)])
    reused = FabricJobBootstrap(
        schedule_config(), root=tmp_path, client=client(reuse_router)
    ).apply()
    assert reused.actions[-1].operation == "reuse"
    assert not any(call["method"] == "PATCH" for call in reuse_router.calls)

    changed = managed_schedule(desired)
    changed["configuration"] = {
        **desired["configuration"],
        "times": ["01:00"],
    }
    update_router = JobsRouter([changed])
    updated = FabricJobBootstrap(
        schedule_config(), root=tmp_path, client=client(update_router)
    ).apply()
    assert updated.actions[-1].operation == "update"
    assert any(call["method"] == "PATCH" for call in update_router.calls)


def test_schedule_ambiguity_fails_without_writes(tmp_path: Path) -> None:
    bootstrap = FabricJobBootstrap(
        schedule_config(),
        root=tmp_path,
        client=client(JobsRouter()),
        now=lambda: datetime(2026, 10, 3, tzinfo=UTC),
    )
    desired = bootstrap._schedule_body(
        JobScope("organization", "example-org"), None
    )
    router = JobsRouter(
        [
            managed_schedule(desired, "one"),
            managed_schedule(desired, "two"),
        ]
    )

    with pytest.raises(DeploymentError, match="multiple managed"):
        FabricJobBootstrap(
            schedule_config(), root=tmp_path, client=client(router)
        ).plan()

    assert all(call["method"] == "GET" for call in router.calls)


def test_disabling_schedule_disables_only_discoverable_managed_schedule(
    tmp_path: Path,
) -> None:
    disabled_config = parse_config(
        {
            "schema_version": 1,
            "github": {
                "mode": "organization",
                "organizations": ["example-org"],
            },
            "azure": {"key_vault_name": "metrics-vault"},
            "fabric": {
                "workspace_name": "Metrics",
                "lakehouse_name": "Lakehouse",
                "pipeline_name": "pipeline",
            },
            "schedule": {"enabled": False},
        }
    )
    user_schedule = {"id": "user", "enabled": True}
    desired = FabricJobBootstrap(
        disabled_config, root=tmp_path, client=client(JobsRouter())
    )._schedule_body(JobScope("organization", "example-org"), None)
    managed = {"id": "managed", **desired}
    router = JobsRouter([user_schedule, managed])

    result = FabricJobBootstrap(
        disabled_config, root=tmp_path, client=client(router)
    ).apply()

    assert result.actions[-1].operation == "disable"
    assert router.schedules[0]["enabled"] is True
    assert router.schedules[1]["enabled"] is False
