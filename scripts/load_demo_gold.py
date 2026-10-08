"""Create and run a Fabric notebook that loads synthetic Gold demo data."""

from __future__ import annotations

import base64
import json
from typing import Any

from copilot_metrics_fabric.deployment import FabricClient

WORKSPACE_ID = "5fd86aef-ea0c-4916-9816-19a3098fadb3"
LAKEHOUSE_ID = "f5112f44-505e-4229-8491-b3018117069a"
LAKEHOUSE_NAME = "GitHubCopilotMetrics"
NOTEBOOK_NAME = "load_demo_gold"


def notebook_code() -> str:
    return r'''
import math
import random
from datetime import date, datetime, timedelta, timezone

from copilot_metrics_fabric.gold import GoldBatch
from copilot_metrics_fabric.gold_spark import create_dataframes

random.seed(42)
scope_kind = "enterprise"
scope_slug = "avocado-corp"
start_day = date(2026, 7, 9)
end_day = date(2026, 10, 6)
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
                "repo_owner_name": "avocado-corp",
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
                "dimension_name": "Avocado Corp",
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
  'Avocado Corp GHCP dashboard POC' AS description,
  current_timestamp() AS loaded_at
""")
print("Synthetic Gold demo data loaded successfully.")
'''


def list_items(client: FabricClient, item_type: str) -> list[dict[str, Any]]:
    payload = client.request(
        "GET",
        f"workspaces/{WORKSPACE_ID}/items?type={item_type}",
        expected=(200,),
    )
    return payload.get("value", [])


def main() -> int:
    client = FabricClient(poll_interval=15, max_lro_polls=240)
    matches = [
        item
        for item in list_items(client, "Notebook")
        if item.get("displayName") == NOTEBOOK_NAME
    ]
    if len(matches) > 1:
        raise RuntimeError("Multiple load_demo_gold notebooks exist")
    if matches:
        notebook_id = matches[0]["id"]
    else:
        created = client.request(
            "POST",
            f"workspaces/{WORKSPACE_ID}/items",
            body={"displayName": NOTEBOOK_NAME, "type": "Notebook"},
            expected=(201, 202),
        )
        notebook_id = created.get("id")
        if not notebook_id:
            matches = [
                item
                for item in list_items(client, "Notebook")
                if item.get("displayName") == NOTEBOOK_NAME
            ]
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
                    "default_lakehouse": LAKEHOUSE_ID,
                    "default_lakehouse_name": LAKEHOUSE_NAME,
                    "default_lakehouse_workspace_id": WORKSPACE_ID,
                    "known_lakehouses": [{"id": LAKEHOUSE_ID}],
                }
            },
        },
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# Synthetic Avocado GHCP Gold data\n",
                    "\n",
                    "Creates deterministic demo-only Gold Delta tables.\n",
                ],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "execution_count": None,
                "outputs": [],
                "source": [
                    line + "\n"
                    for line in notebook_code().strip().splitlines()
                ],
            },
        ],
    }
    payload = base64.b64encode(
        json.dumps(notebook).encode("utf-8")
    ).decode("ascii")
    client.request(
        "POST",
        (
            f"workspaces/{WORKSPACE_ID}/items/{notebook_id}/"
            "updateDefinition"
        ),
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

    recent = client.request(
        "GET",
        (
            f"workspaces/{WORKSPACE_ID}/items/{notebook_id}/"
            "jobs/instances?jobType=RunNotebook"
        ),
        expected=(200,),
    )
    active = [
        job
        for job in recent.get("value", [])
        if str(job.get("status", "")).lower()
        in {"notstarted", "inprogress", "running"}
    ]
    if active:
        raise RuntimeError(
            f"Demo notebook already has active job {active[0].get('id')}"
        )

    response = client.request_response(
        "POST",
        (
            f"workspaces/{WORKSPACE_ID}/items/{notebook_id}/"
            "jobs/instances?jobType=RunNotebook"
        ),
        body={
            "executionData": {
                "configuration": {
                    "defaultLakehouse": {
                        "id": LAKEHOUSE_ID,
                        "name": LAKEHOUSE_NAME,
                        "workspaceId": WORKSPACE_ID,
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
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
