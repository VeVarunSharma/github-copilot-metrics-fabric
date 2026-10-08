import sys
from datetime import date
from types import ModuleType, SimpleNamespace

import pytest

from copilot_metrics_fabric.gold import (
    GOLD_CONTRACTS,
    GoldBuildOptions,
    GoldReplacementWindow,
    build_gold,
    contracts,
    normalize_report_types,
)
from copilot_metrics_fabric.gold_spark import (
    _replacement_condition,
    merge_dataframes,
    spark_struct_type,
)


def user(
    day,
    user_id,
    *,
    interactions=0,
    generations=0,
    acceptances=0,
    suggested=0,
    added=0,
    ingested_at="2026-01-10T00:00:00Z",
):
    return {
        "scope_kind": "organization",
        "scope_slug": "example",
        "day": day,
        "user_id": user_id,
        "user_login": f"user-{user_id}",
        "user_initiated_interaction_count": interactions,
        "code_generation_activity_count": generations,
        "code_acceptance_activity_count": acceptances,
        "loc_suggested_to_add_sum": suggested,
        "loc_added_sum": added,
        "ai_credits_used": None,
        "source_ingested_at": ingested_at,
        "source_ingestion_id": ingested_at,
        "source_record_hash": ingested_at,
    }


def membership(day, user_id, team_id, slug):
    return {
        "scope_kind": "organization",
        "scope_slug": "example",
        "day": day,
        "user_id": user_id,
        "team_id": team_id,
        "team_slug": slug,
        "source_record_hash": f"{day}-{user_id}-{team_id}",
    }


def build(rows, **kwargs):
    options = kwargs.pop(
        "options",
        GoldBuildOptions(as_of_day="2026-01-10", built_at="fixed"),
    )
    return build_gold(rows, options=options, **kwargs)


def test_rates_use_ratio_of_summed_counts_and_zero_denominator_is_null():
    batch = build(
        {
            "user_daily": [
                user("2026-01-01", 1, generations=1, acceptances=1),
                user("2026-01-01", 2, generations=9, acceptances=0),
                user("2026-01-02", 1, generations=0, acceptances=0),
            ]
        }
    )

    first, second = batch.rows["entity_adoption_daily"]
    assert first["acceptance_rate"] == pytest.approx(0.1)
    assert second["acceptance_rate"] is None


def test_rolling_users_are_distinct_across_window_not_summed_daily():
    batch = build(
        {
            "user_daily": [
                user("2026-01-01", 1, interactions=1),
                user("2026-01-02", 1, interactions=2),
                user("2026-01-02", 2, interactions=1),
            ]
        }
    )

    row = next(
        row
        for row in batch.rows["adoption_rolling_daily"]
        if row["day"] == "2026-01-02"
        and row["dimension_type"] == "entity"
        and row["window_days"] == 7
    )
    assert row["distinct_active_users"] == 2


def test_rolling_lookback_is_calculated_before_requested_window_is_emitted():
    batch = build_gold(
        {
            "user_daily": [
                user("2026-01-01", 1, interactions=1),
                user("2026-01-28", 2, interactions=1),
            ]
        },
        current_user_rows=[
            user("2026-01-01", 1, interactions=1),
            user("2026-01-28", 2, interactions=1),
        ],
        options=GoldBuildOptions(
            as_of_day="2026-01-28",
            calendar_start="2026-01-28",
            calendar_end="2026-01-28",
            built_at="fixed",
        ),
    )

    rolling = {
        row["window_days"]: row["distinct_active_users"]
        for row in batch.rows["adoption_rolling_daily"]
        if row["dimension_type"] == "entity"
    }
    assert rolling == {7: 1, 28: 2}
    for name, contract in GOLD_CONTRACTS.items():
        if any(column.name == "day" for column in contract.columns):
            assert {row["day"] for row in batch.rows[name]} <= {"2026-01-28"}
    assert {row["user_id"] for row in batch.rows["user_adoption_current"]} == {1, 2}


def test_source_reported_rolling_values_are_preserved_not_summed():
    batch = build(
        {
            "entity_daily": [
                {
                    "scope_kind": "organization",
                    "scope_slug": "example",
                    "day": "2026-01-01",
                    "daily_active_users": 3,
                    "weekly_active_users": 10,
                    "monthly_active_users": 20,
                    "code_generation_activity_count": 5,
                    "code_acceptance_activity_count": 2,
                    "source_record_hash": "a",
                },
                {
                    "scope_kind": "organization",
                    "scope_slug": "example",
                    "day": "2026-01-02",
                    "daily_active_users": 4,
                    "weekly_active_users": 11,
                    "monthly_active_users": 21,
                    "source_record_hash": "b",
                },
            ]
        }
    )

    rows = batch.rows["entity_adoption_daily"]
    assert [row["source_reported_weekly_active_users"] for row in rows] == [10, 11]
    assert rows[0]["acceptance_rate"] == pytest.approx(0.4)


def test_multi_team_membership_is_preserved_and_marked_non_additive():
    batch = build(
        {
            "user_daily": [user("2026-01-01", 1, interactions=1)],
            "user_team_daily": [
                membership("2026-01-01", 1, 10, "alpha"),
                membership("2026-01-01", 1, 20, "beta"),
            ],
        }
    )

    rows = batch.rows["team_adoption_daily"]
    assert [(row["team_id"], row["active_users"]) for row in rows] == [(10, 1), (20, 1)]
    assert all(row["is_additive"] is False for row in rows)
    assert all(row["allocation_method"] == "all_memberships" for row in rows)


def test_primary_team_mapping_produces_additive_team_rows():
    batch = build(
        {
            "user_daily": [
                user("2026-01-01", 1, interactions=1),
                user("2026-01-01", 2, interactions=1),
            ],
            "user_team_daily": [
                membership("2026-01-01", 1, 10, "alpha"),
                membership("2026-01-01", 1, 20, "beta"),
                membership("2026-01-01", 2, 20, "beta"),
            ],
        },
        primary_team_mapping={1: 10, 2: 20},
    )

    primary = [
        row
        for row in batch.rows["team_adoption_daily"]
        if row["allocation_method"] == "primary_team"
    ]
    assert [(row["team_id"], row["active_users"]) for row in primary] == [
        (10, 1),
        (20, 1),
    ]
    assert all(row["is_additive"] is True for row in primary)


def test_late_correction_replaces_earlier_silver_row():
    early = user(
        "2026-01-01",
        1,
        generations=10,
        acceptances=2,
        ingested_at="2026-01-02T00:00:00Z",
    )
    corrected = user(
        "2026-01-01",
        1,
        generations=10,
        acceptances=8,
        ingested_at="2026-01-04T00:00:00Z",
    )
    batch = build({"user_daily": [early, corrected]})

    row = batch.rows["entity_adoption_daily"][0]
    assert row["acceptance_rate"] == pytest.approx(0.8)
    assert row["source_correction_count"] == 1


def test_freshness_handles_no_data_lag_missing_and_sparse_metrics():
    batch = build_gold(
        {
            "user_daily": [
                {
                    **user("2026-01-08", 1, interactions=1),
                    "code_generation_activity_count": None,
                }
            ]
        },
        options=GoldBuildOptions(
            as_of_day=date(2026, 1, 10),
            calendar_start="2026-01-07",
            calendar_end="2026-01-10",
            telemetry_lag_days=2,
            expected_report_types=("users",),
            built_at="fixed",
        ),
        report_status_rows=[
            {
                "scope_kind": "organization",
                "scope_slug": "example",
                "day": "2026-01-07",
                "report_type": "users",
                "status": "no_data",
            }
        ],
    )

    rows = {row["day"]: row for row in batch.rows["data_freshness_daily"]}
    assert rows["2026-01-07"]["is_no_data"] is True
    assert rows["2026-01-07"]["is_complete"] is True
    assert rows["2026-01-08"]["sparse_metric_count"] > 0
    assert rows["2026-01-09"]["availability_status"] == "within_lag"
    assert rows["2026-01-10"]["availability_status"] == "within_lag"


def test_configured_report_subset_controls_freshness_and_no_data():
    batch = build_gold(
        {},
        options=GoldBuildOptions(
            as_of_day="2026-01-01",
            calendar_start="2026-01-01",
            calendar_end="2026-01-01",
            expected_report_types=normalize_report_types(" users, repositories,users "),
            built_at="fixed",
        ),
        report_status_rows=[
            {
                "scope_kind": "organization",
                "scope_slug": "empty",
                "day": "2026-01-01",
                "report_type": "users",
                "status": "no_data",
            }
        ],
    )

    rows = {row["report_type"]: row for row in batch.rows["data_freshness_daily"]}
    assert set(rows) == {"users", "repositories"}
    assert rows["users"]["availability_status"] == "no_data"
    assert rows["users"]["is_complete"] is True
    assert rows["repositories"]["availability_status"] == "within_lag"


def test_contracts_are_stable_and_pyspark_is_optional():
    exported = contracts()

    assert set(exported) == set(GOLD_CONTRACTS)
    assert exported["team_adoption_daily"]["upsert_key"][-1] == "allocation_method"
    with pytest.raises(RuntimeError, match="PySpark"):
        spark_struct_type("entity_adoption_daily")


def test_latest_manifest_snapshot_removes_obsolete_gold_rows():
    def lineage(row, report_type, ingestion_id):
        return {
            **row,
            "source_report_type": report_type,
            "source_report_day": "2026-01-01",
            "source_ingestion_id": ingestion_id,
            "source_ingested_at": (
                "2026-01-02T00:00:00Z"
                if ingestion_id == "old"
                else "2026-01-04T00:00:00Z"
            ),
        }

    batch = build(
        {
            "user_daily": [
                lineage(user("2026-01-01", 1, interactions=1), "users", "old"),
                lineage(user("2026-01-01", 2, interactions=1), "users", "old"),
                lineage(user("2026-01-01", 1, interactions=2), "users", "new"),
            ],
            "feature_daily": [
                lineage(
                    {
                        **user("2026-01-01", 1),
                        "feature": "chat",
                        "interaction_count": 1,
                    },
                    "users",
                    "new",
                ),
                lineage(
                    {
                        **user("2026-01-01", 2),
                        "feature": "completion",
                        "interaction_count": 1,
                    },
                    "users",
                    "old",
                ),
            ],
            "user_team_daily": [
                lineage(membership("2026-01-01", 1, 10, "alpha"), "user-teams", "new"),
                lineage(membership("2026-01-01", 2, 20, "beta"), "user-teams", "old"),
            ],
            "repository_daily": [
                lineage(
                    {
                        "scope_kind": "organization",
                        "scope_slug": "example",
                        "day": "2026-01-01",
                        "repo_id": 10,
                    },
                    "repositories",
                    "new",
                ),
                lineage(
                    {
                        "scope_kind": "organization",
                        "scope_slug": "example",
                        "day": "2026-01-01",
                        "repo_id": 20,
                    },
                    "repositories",
                    "old",
                ),
            ],
        },
        report_status_rows=[
            {
                "scope_kind": "organization",
                "scope_slug": "example",
                "day": "2026-01-01",
                "report_type": report_type,
                "status": "success",
                "ingestion_id": "new",
                "source_ingested_at": "2026-01-04T00:00:00Z",
            }
            for report_type in ("users", "user-teams", "repositories")
        ],
    )

    assert [row["user_id"] for row in batch.rows["user_adoption_daily"]] == [1]
    assert [row["feature"] for row in batch.rows["feature_usage_daily"]] == ["chat"]
    assert [row["team_id"] for row in batch.rows["team_adoption_daily"]] == [10]
    assert [
        row["repo_id"] for row in batch.rows["repository_copilot_impact_daily"]
    ] == [10]


def test_no_data_only_scope_is_complete_without_data():
    batch = build_gold(
        {},
        options=GoldBuildOptions(
            as_of_day="2026-01-01",
            calendar_start="2026-01-01",
            calendar_end="2026-01-01",
            expected_report_types=("users",),
            built_at="fixed",
        ),
        report_status_rows=[
            {
                "scope_kind": "organization",
                "scope_slug": "empty",
                "day": "2026-01-01",
                "report_type": "users",
                "status": "no_data",
                "ingestion_id": "latest",
            }
        ],
    )

    row = batch.rows["data_freshness_daily"][0]
    assert row["availability_status"] == "no_data"
    assert row["has_data"] is False
    assert row["is_no_data"] is True
    assert row["is_complete"] is True


def test_gold_replacement_is_scope_and_day_bounded():
    condition = _replacement_condition(
        GOLD_CONTRACTS["entity_adoption_daily"],
        [GoldReplacementWindow("organization", "example", "2026-01-01", "2026-01-03")],
    )

    assert "`scope_slug` = 'example'" in condition
    assert "`day` BETWEEN DATE '2026-01-01' AND DATE '2026-01-03'" in condition


def test_historical_backfill_uses_all_history_for_current_user_state():
    batch = build_gold(
        {"user_daily": [user("2026-01-01", 1, interactions=1)]},
        current_user_rows=[
            user("2026-01-01", 1, interactions=1),
            user("2026-02-01", 1, interactions=2),
            user("2026-02-01", 2, interactions=1),
        ],
        options=GoldBuildOptions(
            as_of_day="2026-01-01",
            calendar_start="2026-01-01",
            calendar_end="2026-01-01",
            built_at="fixed",
        ),
    )

    assert [row["day"] for row in batch.rows["user_adoption_daily"]] == ["2026-01-01"]
    current = {row["user_id"]: row for row in batch.rows["user_adoption_current"]}
    assert set(current) == {1, 2}
    assert current[1]["as_of_day"] == "2026-02-01"
    assert current[1]["days_since_activity"] == 0


def test_overlapping_gold_windows_are_coalesced_and_validated():
    condition = _replacement_condition(
        GOLD_CONTRACTS["entity_adoption_daily"],
        [
            GoldReplacementWindow(
                "organization", "example", "2026-01-01", "2026-01-03"
            ),
            GoldReplacementWindow(
                "organization", "example", "2026-01-03", "2026-01-05"
            ),
        ],
    )

    assert condition.count("`scope_slug` = 'example'") == 1
    assert "DATE '2026-01-01' AND DATE '2026-01-05'" in condition
    with pytest.raises(ValueError, match="scope_slug"):
        GoldReplacementWindow("organization", "", "2026-01-01", "2026-01-02")


def test_gold_bounded_replacement_uses_one_atomic_merge(monkeypatch):
    calls = []

    class Merge:
        def alias(self, value):
            calls.append(("alias", value))
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
    merge_dataframes(
        spark,
        {"entity_adoption_daily": Frame()},
        replacement_windows=[
            GoldReplacementWindow(
                "organization", "example", "2026-01-01", "2026-01-03"
            )
        ],
    )

    assert [call[0] for call in calls].count("execute") == 1
    assert any(call[0] == "delete-missing" for call in calls)
