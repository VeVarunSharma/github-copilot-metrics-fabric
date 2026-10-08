# Fabric notebook source
#
from pathlib import Path

from copilot_metrics_fabric.silver import (
    NormalizedBatch,
    SourceContext,
    normalize_ndjson,
    select_latest_manifests,
)
from copilot_metrics_fabric.silver_spark import create_dataframes, merge_dataframes

# Parameter cell
bronze_files: list[dict[str, str]] = []
silver_schema = "silver"

# Install this repository as a wheel or attach it through a Fabric Environment.
combined = NormalizedBatch()
for item in select_latest_manifests(bronze_files):
    context = SourceContext(
        scope_kind=item["scope_kind"],
        scope_slug=item["scope_slug"],
        report_type=item["report_type"],
        report_day=item.get("report_day"),
        ingestion_id=item.get("ingestion_id"),
        ingested_at=item.get("ingested_at"),
        source_path=item.get("path"),
    )
    if item.get("status") == "no_data":
        combined.record_snapshot(context, status="no_data")
        continue
    content = Path(item["path"]).read_bytes()
    combined.extend(normalize_ndjson(content, context))

if combined.snapshots:
    combined.finalize()
    silver_frames = create_dataframes(spark, combined)  # noqa: F821
    merge_dataframes(  # noqa: F821
        spark,  # noqa: F821
        silver_frames,
        schema=silver_schema,
        snapshots=combined.snapshots,
    )
    display(  # noqa: F821
        spark.createDataFrame(  # noqa: F821
            [(name, frame.count()) for name, frame in silver_frames.items()],
            ["table", "row_count"],
        )
    )
