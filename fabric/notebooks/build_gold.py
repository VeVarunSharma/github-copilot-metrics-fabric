# Fabric notebook source: Gold analytics materialization.
#
# Attach a Fabric Environment containing this package, then run after Silver is
# available. This is an integration contract, not an orchestration pipeline.

from datetime import date, timedelta

from pyspark.sql import functions as F

from copilot_metrics_fabric.gold import (
    GoldBuildOptions,
    GoldReplacementWindow,
    normalize_report_types,
)
from copilot_metrics_fabric.gold_spark import build_dataframes, merge_dataframes
from copilot_metrics_fabric.silver import (
    CONTRACTS as SILVER_CONTRACTS,
)
from copilot_metrics_fabric.silver import (
    select_latest_manifests,
)

bronze_manifests: list[dict[str, object]] = []
scope_kind = "organization"
scope_slug = "example"
bronze_folder = "bronze"
report_types = "entity,users,user-teams,repositories"
today = date.today()
replacement_start_day = today - timedelta(days=35)
input_start_day = replacement_start_day - timedelta(days=27)

silver_frames = {}
current_user_frame = None
for name in SILVER_CONTRACTS:
    if name in {"silver_quarantine", "silver_schema_drift", "silver_quality_issues"}:
        continue
    if not spark.catalog.tableExists(f"silver.{name}"):  # noqa: F821
        continue
    frame = spark.table(f"silver.{name}")  # noqa: F821
    if "scope_kind" in frame.columns:
        frame = frame.filter(
            (F.col("scope_kind") == scope_kind) & (F.col("scope_slug") == scope_slug)
        )
    if name == "user_daily":
        current_user_frame = frame
    if "day" in frame.columns:
        frame = frame.filter(
            (F.col("day") >= F.lit(input_start_day))
            & (F.col("day") <= F.lit(today))
        )
    silver_frames[name] = frame
if current_user_frame is None:
    raise RuntimeError(
        "silver.user_daily is required to recompute current user state"
    )

# Optional hooks:
# - report_status_rows: manifest-derived complete/no_data rows
# - primary_team_mapping: user_id (or scope_kind, scope_slug, user_id) -> team_id
report_status_rows = [
    {
        "scope_kind": manifest["scope"]["kind"],
        "scope_slug": manifest["scope"]["slug"],
        "day": manifest["report_day"],
        "report_type": manifest["report_type"],
        "status": (
            "complete" if manifest["status"] == "success" else "no_data"
        ),
        "ingestion_id": manifest.get("ingestion_id"),
        "source_ingested_at": manifest.get("ingested_at"),
    }
    for manifest in select_latest_manifests(bronze_manifests)
]
primary_team_mapping = {}

options = GoldBuildOptions(
    as_of_day=today,
    calendar_start=replacement_start_day,
    calendar_end=today,
    telemetry_lag_days=2,
    expected_report_types=normalize_report_types(report_types),
)
gold_frames = build_dataframes(
    spark,  # noqa: F821
    silver_frames,
    options=options,
    report_status_rows=report_status_rows,
    primary_team_mapping=primary_team_mapping,
    current_user_frame=current_user_frame,
)
merge_dataframes(  # noqa: F821
    spark,  # noqa: F821
    gold_frames,
    schema="gold",
    replacement_windows=[
        GoldReplacementWindow(
            scope_kind, scope_slug, options.calendar_start, options.calendar_end
        )
    ],
)
