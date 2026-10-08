import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from copilot_metrics_fabric.silver import (
    CONTRACTS,
    NormalizedBatch,
    Snapshot,
    SourceContext,
    contracts,
    normalize_ndjson,
    normalize_records,
    select_latest_manifests,
)
from copilot_metrics_fabric.silver_spark import (
    _snapshot_condition,
    merge_dataframes,
    spark_struct_type,
)

FIXTURES = Path(__file__).parent / "fixtures"


def context(report_type, *, kind="enterprise", day="2025-10-01"):
    return SourceContext(
        scope_kind=kind,
        scope_slug="example",
        report_type=report_type,
        report_day=day,
        ingestion_id="ingestion-1",
        ingested_at="2025-10-03T00:00:00Z",
        source_path="bronze/report.ndjson",
    )


def test_user_normalization_deduplicates_and_quarantines_bad_rows():
    result = normalize_ndjson(
        (FIXTURES / "silver_users_edge_cases.ndjson").read_bytes(),
        context("users"),
    )

    assert len(result.rows["user_daily"]) == 1
    assert result.rows["user_daily"][0]["user_id"] == 1
    assert result.rows["user_daily"][0]["day_key"] == 20251001
    assert len(result.rows["feature_daily"]) == 1
    assert result.rows["feature_daily"][0]["user_id"] == 1
    assert len(result.rows["silver_quarantine"]) == 4
    assert any(
        json.loads(row["reason_codes_json"]) == ["user_id_must_be_nonnegative_integer"]
        for row in result.rows["silver_quarantine"]
    )
    assert any(
        row["field_path"] == "unexpected_preview_field"
        for row in result.rows["silver_schema_drift"]
    )
    assert any(
        row["check_name"] == "duplicate_upsert_key"
        for row in result.rows["silver_quality_issues"]
    )


def test_sparse_optional_arrays_and_complex_objects_are_flattened():
    record = {
        "day": "2025-10-01",
        "enterprise_id": "1",
        "user_id": 7,
        "ai_adoption_phase": {
            "phase": "Phase 2",
            "phase_number": 2,
            "version": "v1",
        },
        "totals_by_cli": {
            "prompt_count": 2,
            "request_count": 3,
            "session_count": 1,
            "token_usage": {
                "avg_tokens_per_request": 12.5,
                "output_tokens_sum": 20,
                "prompt_tokens_sum": 30,
            },
            "last_known_cli_version": {
                "cli_version": "1.0.8",
                "sampled_at": "2025-10-01T00:00:00Z",
            },
        },
        "totals_by_copilot_app": {"session_count": 1},
        "totals_by_3rd_party_agent": [
            {
                "agent_id": "2246796",
                "agent_name": "Example",
                "user_initiated_interaction_count": 2,
            }
        ],
        "totals_by_mcp": [{"mcp": "github-mcp-server", "interaction_count": 1}],
        "totals_by_skill": [],
        "totals_by_slash_cmd": [{"slash_cmd": "/plan", "interaction_count": 1}],
    }

    result = normalize_records([record], context("users"))

    user = result.rows["user_daily"][0]
    assert (user["adoption_phase"], user["adoption_phase_number"]) == ("Phase 2", 2)
    assert result.rows["cli_daily"][0]["last_known_cli_version"] == "1.0.8"
    assert result.rows["copilot_app_daily"][0]["request_count"] is None
    assert result.rows["third_party_agent_daily"][0]["agent_id"] == "2246796"
    assert result.rows["mcp_daily"][0]["mcp"] == "github-mcp-server"
    assert result.rows["slash_command_daily"][0]["slash_command"] == "/plan"
    assert result.rows["skill_daily"] == []


def test_entity_wrapper_expands_day_totals_adoption_and_feature_engagement():
    record = {
        "enterprise_id": "1",
        "report_start_day": "2025-09-04",
        "report_end_day": "2025-10-01",
        "day_totals": [
            {
                "day": "2025-10-01",
                "daily_active_users": 2,
                "pull_requests": {
                    "total_created": 2,
                    "total_created_by_copilot": 1,
                },
                "totals_by_ai_adoption_phase": [
                    {
                        "phase": "Phase 1",
                        "phase_number": 1,
                        "total_engaged_users": 2,
                        "users_in_phase_28d": 4,
                    }
                ],
            }
        ],
        "copilot_feature_engagement": {
            "active_user_count": 2,
            "totals_by_feature": [
                {"feature": "code_completion", "engaged_user_count": 2}
            ],
        },
    }

    result = normalize_records([record], context("entity"))

    assert result.rows["entity_daily"][0]["daily_active_users"] == 2
    assert result.rows["entity_daily"][0]["pr_total_created"] == 2
    assert result.rows["adoption_phase_daily"][0]["phase_number"] == 1
    assert result.rows["feature_engagement_daily"][0]["engaged_user_count"] == 2


@pytest.mark.parametrize(
    ("kind", "record", "expected_id"),
    [
        (
            "organization",
            {
                "day": "2025-10-01",
                "organization_id": "999",
                "user_id": 1001,
                "team_id": 42,
                "slug": "frontend",
            },
            "999",
        ),
        (
            "enterprise",
            {
                "day": "2025-10-01",
                "enterprise_id": "1",
                "user_id": 1001,
                "team_id": 9001,
                "slug": "eng-platform",
            },
            "1",
        ),
    ],
)
def test_team_memberships_support_organization_and_enterprise(
    kind, record, expected_id
):
    result = normalize_records([record], context("user-teams", kind=kind))

    row = result.rows["user_team_daily"][0]
    assert row["team_id"] in {42, 9001}
    assert row[f"{kind}_id"] == expected_id


def test_repository_pull_request_structures_are_expanded():
    content = (FIXTURES / "copilot_repos_1_day.ndjson").read_bytes()
    repo_context = SourceContext(
        scope_kind="enterprise",
        scope_slug="example",
        report_type="repositories",
        report_day="2026-07-14",
    )
    result = normalize_ndjson(content, repo_context)

    row = result.rows["repository_daily"][0]
    assert row["repo_id"] == 900000001
    assert isinstance(row["repo_id"], int)
    assert row["pr_total_created"] == 1

    enriched = json.loads(content)
    enriched["pull_requests"]["copilot_suggestions_by_comment_type"] = [
        {
            "comment_type": "documentation",
            "total_copilot_suggestions": 1,
            "total_copilot_applied_suggestions": 1,
        }
    ]
    enriched["pull_request_review_times"] = [
        {
            "authored_by": "human",
            "reviewed_by": "human",
            "total_merged": 1,
            "median_minutes_ready_to_first_review": 10.5,
        }
    ]
    result = normalize_records([enriched], repo_context)
    assert result.rows["repository_pr_comment_type_daily"][0]["repo_id"] == 900000001
    assert result.rows["repository_pr_review_time_daily"][0]["authored_by"] == "human"


def test_invalid_repository_counters_are_quarantined():
    record = {
        "day": "2026-07-14",
        "enterprise_id": "1",
        "organization_id": "2",
        "repo_id": 9,
        "pull_requests": {
            "total_created": 1,
            "total_created_by_copilot": 2,
        },
    }
    result = normalize_records(
        [record],
        SourceContext(
            scope_kind="enterprise",
            scope_slug="example",
            report_type="repositories",
            report_day="2026-07-14",
        ),
    )

    assert result.rows["repository_daily"] == []
    assert "exceeds" in result.rows["silver_quarantine"][0]["reason_codes_json"]


def test_contracts_are_explicit_and_pyspark_is_optional():
    exported = contracts()

    assert exported["user_daily"]["upsert_key"] == [
        "scope_kind",
        "scope_slug",
        "day",
        "user_id",
    ]
    assert any(
        column["name"] == "day_key" and column["type"] == "long"
        for column in exported["user_daily"]["columns"]
    )
    assert set(CONTRACTS) == set(exported)
    with pytest.raises(RuntimeError, match="PySpark"):
        spark_struct_type("user_daily")


def test_latest_complete_snapshots_remove_missing_rows_and_nested_rows():
    early_context = context("users", kind="organization")
    corrected_context = SourceContext(
        scope_kind=early_context.scope_kind,
        scope_slug=early_context.scope_slug,
        report_type=early_context.report_type,
        report_day=early_context.report_day,
        ingestion_id="ingestion-2",
        ingested_at="2025-10-04T00:00:00Z",
        source_path=early_context.source_path,
    )
    early = normalize_records(
        [
            {
                "day": "2025-10-01",
                "organization_id": "1",
                "user_id": 1,
                "totals_by_feature": [{"feature": "chat"}],
            },
            {
                "day": "2025-10-01",
                "organization_id": "1",
                "user_id": 2,
                "totals_by_feature": [{"feature": "completion"}],
            },
        ],
        early_context,
    )
    corrected = normalize_records(
        [
            {
                "day": "2025-10-01",
                "organization_id": "1",
                "user_id": 1,
                "totals_by_feature": [{"feature": "chat"}],
            }
        ],
        corrected_context,
    )
    combined = NormalizedBatch()
    combined.extend(early)
    combined.extend(corrected)
    combined.finalize()

    assert [row["user_id"] for row in combined.rows["user_daily"]] == [1]
    assert [row["feature"] for row in combined.rows["feature_daily"]] == ["chat"]
    condition = _snapshot_condition(
        CONTRACTS["user_daily"], tuple(combined.snapshots)
    )
    assert "`source_report_type` = 'users'" in condition
    assert "`source_report_day` = DATE '2025-10-01'" in condition


def test_latest_manifest_can_be_explicit_no_data():
    manifests = [
        {
            "scope": {"kind": "organization", "slug": "example"},
            "report_type": "users",
            "report_day": "2025-10-01",
            "ingestion_id": "one",
            "ingested_at": "2025-10-02T00:00:00Z",
            "status": "success",
        },
        {
            "scope": {"kind": "organization", "slug": "example"},
            "report_type": "users",
            "report_day": "2025-10-01",
            "ingestion_id": "two",
            "ingested_at": "2025-10-03T00:00:00Z",
            "status": "no_data",
        },
    ]

    assert select_latest_manifests(manifests)[0]["status"] == "no_data"


def test_silver_snapshot_replacement_uses_one_atomic_merge(monkeypatch):
    calls = []

    class Merge:
        def alias(self, value):
            return self

        def merge(self, source, condition):
            calls.append(("merge", condition))
            return self

        def whenMatchedUpdateAll(self):
            return self

        def whenNotMatchedInsertAll(self):
            return self

        def whenNotMatchedBySourceDelete(self, *, condition):
            calls.append(("delete-missing", condition))
            return self

        def execute(self):
            calls.append(("execute",))

    delta_tables = ModuleType("delta.tables")
    delta_tables.DeltaTable = SimpleNamespace(forName=lambda spark, name: Merge())
    monkeypatch.setitem(sys.modules, "delta", ModuleType("delta"))
    monkeypatch.setitem(sys.modules, "delta.tables", delta_tables)

    class Frame:
        def alias(self, name):
            return self

        def filter(self, predicate):
            calls.append(("validate", predicate))
            return self

        def limit(self, count):
            return self

        def count(self):
            return 0

    spark = SimpleNamespace(
        sql=lambda statement: None,
        catalog=SimpleNamespace(tableExists=lambda name: True),
    )
    snapshot = Snapshot(
        "organization",
        "example",
        "users",
        "2025-10-01",
        "new",
        "2025-10-03T00:00:00Z",
    )
    merge_dataframes(
        spark,
        {"user_daily": Frame()},
        snapshots=[snapshot],
    )

    assert [call[0] for call in calls].count("execute") == 1
    assert any(call[0] == "delete-missing" for call in calls)


def test_silver_replacement_rejects_rows_outside_snapshot(monkeypatch):
    class Merge:
        def alias(self, value):
            return self

        def merge(self, source, condition):
            return self

        def whenMatchedUpdateAll(self):
            return self

        def whenNotMatchedInsertAll(self):
            return self

    delta_tables = ModuleType("delta.tables")
    delta_tables.DeltaTable = SimpleNamespace(forName=lambda spark, name: Merge())
    monkeypatch.setitem(sys.modules, "delta", ModuleType("delta"))
    monkeypatch.setitem(sys.modules, "delta.tables", delta_tables)

    class Frame:
        def alias(self, name):
            return self

        def filter(self, predicate):
            return self

        def limit(self, count):
            return self

        def count(self):
            return 1

    spark = SimpleNamespace(
        sql=lambda statement: None,
        catalog=SimpleNamespace(tableExists=lambda name: True),
    )
    snapshot = Snapshot(
        "organization",
        "example",
        "users",
        "2025-10-01",
        "new",
        "2025-10-03T00:00:00Z",
    )

    with pytest.raises(ValueError, match="outside"):
        merge_dataframes(spark, {"user_daily": Frame()}, snapshots=[snapshot])


def test_snapshot_contract_rejects_unsafe_bounds():
    with pytest.raises(ValueError, match="scope_slug"):
        Snapshot(
            "organization",
            "",
            "users",
            "2025-10-01",
            None,
            None,
        )
    with pytest.raises(ValueError, match="report_day"):
        Snapshot(
            "organization",
            "example",
            "users",
            "not-a-date",
            None,
            None,
        )


@pytest.mark.parametrize(
    ("report_type", "early_records", "corrected_records", "table", "key"),
    [
        (
            "user-teams",
            [
                {"day": "2025-10-01", "user_id": 1, "team_id": 10},
                {"day": "2025-10-01", "user_id": 2, "team_id": 20},
            ],
            [{"day": "2025-10-01", "user_id": 1, "team_id": 10}],
            "user_team_daily",
            "team_id",
        ),
        (
            "repositories",
            [
                {
                    "day": "2025-10-01",
                    "organization_id": "1",
                    "repo_id": 10,
                },
                {
                    "day": "2025-10-01",
                    "organization_id": "1",
                    "repo_id": 20,
                },
            ],
            [
                {
                    "day": "2025-10-01",
                    "organization_id": "1",
                    "repo_id": 10,
                }
            ],
            "repository_daily",
            "repo_id",
        ),
    ],
)
def test_corrected_snapshots_remove_team_and_repository_rows(
    report_type, early_records, corrected_records, table, key
):
    first = context(report_type, kind="organization")
    for record in (*early_records, *corrected_records):
        record.setdefault("organization_id", "1")
    second = SourceContext(
        first.scope_kind,
        first.scope_slug,
        first.report_type,
        first.report_day,
        "ingestion-2",
        "2025-10-04T00:00:00Z",
    )
    combined = NormalizedBatch()
    combined.extend(normalize_records(early_records, first))
    combined.extend(normalize_records(corrected_records, second))
    combined.finalize()

    assert [row[key] for row in combined.rows[table]] == [10]
