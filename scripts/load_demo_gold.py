"""Create and run a Fabric notebook that loads synthetic Gold demo data."""

from __future__ import annotations

import argparse
import base64
import inspect
import json
from datetime import date
from typing import Any
from uuid import UUID

from copilot_metrics_fabric.deployment import FabricClient

NOTEBOOK_NAME = "load_demo_gold"


def validate_demo_target(spark, allowed_tables):
    """Refuse existing tables unless they belong exclusively to this demo."""
    existing = set()
    for database in spark.catalog.listDatabases():
        for table in spark.catalog.listTables(database.name):
            if table.isTemporary:
                raise RuntimeError("Temporary tables are not permitted in the demo")
            existing.add((database.name, table.name))
    allowed = {("gold", name) for name in allowed_tables}
    if existing - allowed:
        raise RuntimeError("Use an isolated Lakehouse without unrelated tables")
    if not existing:
        return
    if ("gold", "demo_metadata") not in existing:
        raise RuntimeError("Existing tables have no demo ownership marker")
    markers = spark.table("gold.demo_metadata").collect()
    if len(markers) != 1 or markers[0].asDict().get("demo_owner") != (
        "github-copilot-metrics-fabric/synthetic-demo/v1"
    ):
        raise RuntimeError("Existing tables are not owned by this synthetic demo")
    for schema, name in existing:
        if name in {"demo_metadata", "date_dimension"}:
            continue
        if (
            spark.table(f"{schema}.{name}")
            .where("scope_slug IS NULL OR scope_slug <> 'synthetic-demo'")
            .limit(1)
            .count()
        ):
            raise RuntimeError("Non-demo scope detected; refusing to overwrite")


def notebook_code(
    start_date: date = date(2026, 7, 9),
    end_date: date = date(2026, 10, 6),
) -> str:
    if not date(2020, 1, 1) <= start_date <= end_date <= date(2035, 12, 31):
        raise ValueError("Dates must be ordered within the report calendar 2020-2035")
    if (end_date - start_date).days >= 366:
        raise ValueError("Demo windows must not exceed 366 days")
    prelude = (
        inspect.getsource(validate_demo_target)
        + f"\nstart_day = date.fromisoformat({start_date.isoformat()!r})\n"
        + f"end_day = date.fromisoformat({end_date.isoformat()!r})\n"
    )
    return (
        "from datetime import date\n"
        + prelude
        + r'''
import math
import random
from datetime import date, datetime, timedelta, timezone

from copilot_metrics_fabric.gold import GoldBatch
from copilot_metrics_fabric.gold_spark import create_dataframes

random.seed(42)
scope_kind = "enterprise"
scope_slug = "synthetic-demo"
built_at = datetime.now(timezone.utc).isoformat()

spark.conf.set("spark.sql.parquet.vorder.default", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

batch = GoldBatch()

def base(day):
    return {
        "scope_kind": scope_kind,
        "scope_slug": scope_slug,
        "day": day.isoformat(),
        "day_key": int(day.strftime("%Y%m%d")),
        "gold_built_at": built_at,
        "source_correction_count": 0,
    }

def adoption(
    observed,
    active,
    engaged,
    interactions,
    generations,
    acceptances,
    suggested,
    accepted,
    credits,
):
    return {
        "observed_users": observed,
        "active_users": active,
        "engaged_users": engaged,
        "interaction_count": interactions,
        "generation_count": generations,
        "acceptance_count": acceptances,
        "acceptance_rate": acceptances / generations if generations else None,
        "loc_suggested": suggested,
        "loc_accepted": accepted,
        "loc_acceptance_rate": accepted / suggested if suggested else None,
        "ai_credits_used": float(credits),
    }

teams = [
    (101, "platform-engineering", 82),
    (102, "application-engineering", 96),
    (103, "data-and-ai", 68),
    (104, "security-engineering", 54),
]
features = [
    ("code_completion", 0.48),
    ("chat", 0.31),
    ("agent", 0.21),
]
languages = [
    ("python", 0.34),
    ("typescript", 0.27),
    ("csharp", 0.18),
    ("java", 0.12),
    ("sql", 0.09),
]
ides = [("vscode", 0.68), ("visualstudio", 0.21), ("jetbrains", 0.11)]
repos = [
    (2001, "commerce-api", "private"),
    (2002, "customer-portal", "private"),
    (2003, "data-platform", "private"),
    (2004, "security-automation", "private"),
    (2005, "developer-tooling", "internal"),
]

days = (end_day - start_day).days + 1
for index in range(days):
    day = start_day + timedelta(days=index)
    weekday_factor = 0.76 if day.weekday() >= 5 else 1.0
    trend = index / max(days - 1, 1)
    seasonal = 1.0 + 0.07 * math.sin(index / 6.0)
    observed = 300
    active = int((142 + 96 * trend) * weekday_factor * seasonal)
    engaged = int(active * (0.72 + 0.06 * trend))
    interactions = int(engaged * (5.2 + 2.0 * trend))
    generations = int(engaged * (8.8 + 2.5 * trend))
    acceptance_rate = 0.31 + 0.11 * trend
    acceptances = int(generations * acceptance_rate)
    suggested = generations * 7
    accepted = int(suggested * (0.35 + 0.10 * trend))
    credits = round(interactions * 0.19 + index % 7, 2)
    metrics = adoption(
        observed, active, engaged, interactions, generations,
        acceptances, suggested, accepted, credits
    )
    batch.add(
        "entity_adoption_daily",
        {
            **base(day),
            **metrics,
            "source_reported_daily_active_users": active,
            "source_reported_weekly_active_users": min(observed, int(active * 1.72)),
            "source_reported_monthly_active_users": min(observed, int(active * 2.05)),
        },
    )

    for team_id, team_slug, membership in teams:
        share = membership / sum(item[2] for item in teams)
        team_active = min(membership, max(4, int(active * share)))
        team_engaged = min(team_active, int(engaged * share))
        team_metrics = adoption(
            membership,
            team_active,
            team_engaged,
            int(interactions * share),
            int(generations * share),
            int(acceptances * share),
            int(suggested * share),
            int(accepted * share),
            credits * share,
        )
        batch.add(
            "team_adoption_daily",
            {
                **base(day),
                "team_id": team_id,
                "team_slug": team_slug,
                **team_metrics,
                "membership_count": membership,
                "allocation_method": "primary_team",
                "is_additive": True,
            },
        )

    for feature, share in features:
        feature_active = max(1, int(active * share))
        feature_engaged = max(1, int(engaged * share))
        batch.add(
            "feature_usage_daily",
            {
                **base(day),
                "feature": feature,
                **adoption(
                    observed,
                    feature_active,
                    feature_engaged,
                    int(interactions * share),
                    int(generations * share),
                    int(acceptances * share),
                    int(suggested * share),
                    int(accepted * share),
                    credits * share,
                ),
            },
        )

    for language, share in languages:
        batch.add(
            "language_usage_daily",
            {
                **base(day),
                "language": language,
                **adoption(
                    observed,
                    max(1, int(active * share)),
                    max(1, int(engaged * share)),
                    int(interactions * share),
                    int(generations * share),
                    int(acceptances * share),
                    int(suggested * share),
                    int(accepted * share),
                    credits * share,
                ),
            },
        )

    for ide, share in ides:
        batch.add(
            "ide_usage_daily",
            {
                **base(day),
                "ide": ide,
                **adoption(
                    observed,
                    max(1, int(active * share)),
                    max(1, int(engaged * share)),
                    int(interactions * share),
                    int(generations * share),
                    int(acceptances * share),
                    int(suggested * share),
                    int(accepted * share),
                    credits * share,
                ),
            },
        )

    for repo_index, (repo_id, repo_name, visibility) in enumerate(repos):
        created = max(1, int((6 + repo_index + trend * 5) * weekday_factor))
        authored = min(created, int(created * (0.18 + 0.14 * trend)))
        reviewed = created + 2 + repo_index
        reviewed_by_copilot = int(reviewed * (0.24 + 0.16 * trend))
        suggestions = reviewed_by_copilot * 4
        applied = int(suggestions * (0.48 + 0.08 * trend))
        batch.add(
            "repository_copilot_impact_daily",
            {
                **base(day),
                "repo_id": repo_id,
                "repo_owner_name": "synthetic-demo",
                "repo_name": repo_name,
                "repo_visibility": visibility,
                "pull_requests_created": created,
                "pull_requests_created_by_copilot": authored,
                "copilot_authored_pr_rate": authored / created,
                "pull_requests_reviewed": reviewed,
                "pull_requests_reviewed_by_copilot": reviewed_by_copilot,
                "copilot_reviewed_pr_rate": reviewed_by_copilot / reviewed,
                "suggestions": suggestions,
                "applied_suggestions": applied,
                "suggestion_apply_rate": applied / suggestions if suggestions else None,
                "pull_requests_merged": max(0, created - 1),
                "median_minutes_to_merge": float(920 - 280 * trend + repo_index * 18),
            },
        )

    for window_days, multiplier in ((7, 1.55), (28, 1.95)):
        batch.add(
            "adoption_rolling_daily",
            {
                **base(day),
                "dimension_type": "enterprise",
                "dimension_id": scope_slug,
                "dimension_name": "Synthetic demonstration",
                "window_days": window_days,
                "distinct_active_users": min(observed, int(active * multiplier)),
                "allocation_method": "entity",
                "is_additive": True,
            },
        )

    for report_type in ("entity", "users", "user-teams", "repositories"):
        batch.add(
            "data_freshness_daily",
            {
                **base(day),
                "report_type": report_type,
                "availability_status": "complete",
                "has_data": True,
                "is_no_data": False,
                "is_complete": True,
                "is_within_telemetry_lag": False,
                "days_late": 0,
                "latest_source_ingested_at": built_at,
                "sparse_metric_count": 0,
            },
        )

for user_id in range(1, 301):
    activity_offset = user_id % 11
    as_of = end_day
    status = "engaged" if user_id % 4 else "active"
    phase = f"Phase {1 + (user_id % 4)}"
    batch.add(
        "user_adoption_current",
        {
            "scope_kind": scope_kind,
            "scope_slug": scope_slug,
            "user_id": user_id,
            "user_login": f"developer-{user_id:03d}",
            "as_of_day": as_of.isoformat(),
            "as_of_day_key": int(as_of.strftime("%Y%m%d")),
            "adoption_status": status,
            "adoption_phase": phase,
            "days_since_activity": activity_offset,
            "is_stale": False,
            "gold_built_at": built_at,
            "source_correction_count": 0,
        },
    )

frames = create_dataframes(spark, batch.finalize())
validate_demo_target(spark, set(frames) | {"date_dimension", "demo_metadata"})
spark.sql("CREATE SCHEMA IF NOT EXISTS `gold`")
for name, frame in frames.items():
    frame.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"`gold`.`{name}`")
    print(f"{name}: {frame.count()} rows")

date_rows = []
calendar_day = date(2020, 1, 1)
calendar_end = date(2035, 12, 31)
while calendar_day <= calendar_end:
    date_rows.append(
        (
            calendar_day,
            calendar_day.year,
            calendar_day.month,
            calendar_day.strftime("%b"),
            calendar_day.strftime("%Y-%b"),
            calendar_day.year * 100 + calendar_day.month,
        )
    )
    calendar_day += timedelta(days=1)
date_frame = spark.createDataFrame(
    date_rows,
    [
        "date",
        "year",
        "month_number",
        "month",
        "year_month",
        "year_month_number",
    ],
)
date_frame.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("`gold`.`date_dimension`")
print(f"date_dimension: {date_frame.count()} rows")

spark.sql("""
CREATE OR REPLACE TABLE gold.demo_metadata
USING DELTA
AS SELECT
  'synthetic' AS data_classification,
  'Synthetic demonstration data - not customer evidence' AS description,
  'github-copilot-metrics-fabric/synthetic-demo/v1' AS demo_owner,
  current_timestamp() AS loaded_at
""")
print("Synthetic Gold demo data loaded successfully.")
'''
    )


def list_items(
    client: FabricClient, workspace_id: str, item_type: str
) -> list[dict[str, Any]]:
    payload = client.request(
        "GET",
        f"workspaces/{workspace_id}/items?type={item_type}",
        expected=(200,),
    )
    if payload.get("continuationToken") or payload.get("continuationUri"):
        raise RuntimeError(
            "Cannot safely discover demo notebook on a paginated listing"
        )
    return payload.get("value", [])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-id", type=UUID, required=True)
    parser.add_argument("--lakehouse-id", type=UUID, required=True)
    parser.add_argument("--lakehouse-name", required=True)
    parser.add_argument(
        "--start-date", type=date.fromisoformat, default=date(2026, 7, 9)
    )
    parser.add_argument(
        "--end-date", type=date.fromisoformat, default=date(2026, 10, 6)
    )
    parser.add_argument("--confirm-isolated-demo", action="store_true")
    parser.add_argument(
        "--yes", action="store_true", help="Apply; otherwise plan offline"
    )
    args = parser.parse_args(argv)
    try:
        code = notebook_code(args.start_date, args.end_date)
    except ValueError as exc:
        parser.error(str(exc))
    workspace_id = str(args.workspace_id)
    lakehouse_id = str(args.lakehouse_id)
    lakehouse_name = args.lakehouse_name
    print(
        "SYNTHETIC DEMONSTRATION DATA: bypasses Bronze/Silver; replaces demo Gold "
        f"tables only. Target: {workspace_id}/{lakehouse_id} ({lakehouse_name}). "
        f"Dates: {args.start_date} through {args.end_date}. "
        "Use a dedicated Lakehouse with no live ingestion or concurrent writers."
    )
    if not args.yes:
        print("Plan only: no authentication, network requests, or writes performed.")
        return 0
    if not args.confirm_isolated_demo:
        parser.error("--yes requires --confirm-isolated-demo")
    client = FabricClient(poll_interval=15, max_lro_polls=240)
    target = client.request(
        "GET", f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}", expected=(200,)
    )
    if target.get("displayName") != lakehouse_name:
        raise RuntimeError("Lakehouse name does not match the selected target")
    matches = [
        item
        for item in list_items(client, workspace_id, "Notebook")
        if item.get("displayName") == NOTEBOOK_NAME
    ]
    if len(matches) > 1:
        raise RuntimeError("Multiple load_demo_gold notebooks exist")
    if matches:
        notebook_id = matches[0]["id"]
        recent = client.request(
            "GET",
            f"workspaces/{workspace_id}/items/{notebook_id}/jobs/instances",
            expected=(200,),
        )
        if recent.get("continuationToken") or recent.get("continuationUri"):
            raise RuntimeError("Cannot safely inspect paginated notebook jobs")
        if any(
            str(job.get("status", "")).lower()
            not in {"completed", "failed", "cancelled", "deduped"}
            for job in recent.get("value", [])
        ):
            raise RuntimeError("Demo notebook has an active or unknown job state")
    else:
        created = client.request(
            "POST",
            f"workspaces/{workspace_id}/items",
            body={"displayName": NOTEBOOK_NAME, "type": "Notebook"},
            expected=(201, 202),
        )
        notebook_id = created.get("id")
        if not notebook_id:
            matches = [
                item
                for item in list_items(client, workspace_id, "Notebook")
                if item.get("displayName") == NOTEBOOK_NAME
            ]
            if len(matches) != 1:
                raise RuntimeError("Could not uniquely resolve created demo notebook")
            notebook_id = matches[0]["id"]

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "Python",
                "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
            "dependencies": {
                "lakehouse": {
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_name": lakehouse_name,
                    "default_lakehouse_workspace_id": workspace_id,
                    "known_lakehouses": [{"id": lakehouse_id}],
                }
            },
        },
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# Synthetic demonstration data - not customer evidence\n",
                    "\n",
                    "Replaces only owned demo Gold tables in an isolated Lakehouse.\n",
                    "Primary-team rows are synthetic; live mapping is not deployed.\n",
                ],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "outputs": [],
                "source": [line + "\n" for line in code.strip().splitlines()],
            },
        ],
    }
    payload = base64.b64encode(json.dumps(notebook).encode("utf-8")).decode("ascii")
    client.request(
        "POST",
        (f"workspaces/{workspace_id}/items/{notebook_id}/updateDefinition"),
        body={
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "artifact.content.ipynb",
                        "payload": payload,
                        "payloadType": "InlineBase64",
                    }
                ],
            }
        },
        expected=(200, 202),
    )

    response = client.request_response(
        "POST",
        (
            f"workspaces/{workspace_id}/items/{notebook_id}/"
            "jobs/instances?jobType=RunNotebook"
        ),
        body={
            "executionData": {
                "configuration": {
                    "defaultLakehouse": {
                        "id": lakehouse_id,
                        "name": lakehouse_name,
                        "workspaceId": workspace_id,
                    }
                }
            }
        },
        expected=(202,),
    )
    location = response.headers.get("Location")
    if not location:
        raise RuntimeError("Notebook run returned no Location header")
    result = client.wait_for_job(location, initial_response=response)
    print(f"Demo job status: {result.get('status', 'unknown')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
