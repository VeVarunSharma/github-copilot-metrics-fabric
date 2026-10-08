"""Date-window contract shared by Fabric pipeline assets and tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

GOLD_START_EXPRESSION = (
    "@if(equals(pipeline().parameters.run_mode, 'backfill'), "
    "pipeline().parameters.start_date, "
    "formatDateTime(addDays(utcNow(), "
    "mul(-1, pipeline().parameters.trailing_days)), 'yyyy-MM-dd'))"
)
GOLD_END_EXPRESSION = (
    "@if(equals(pipeline().parameters.run_mode, 'backfill'), "
    "pipeline().parameters.end_date, "
    "formatDateTime(addDays(utcNow(), -1), 'yyyy-MM-dd'))"
)
_INGESTION_START_CANDIDATE = (
    "if(equals(pipeline().parameters.run_mode, 'backfill'), "
    "formatDateTime(addDays(pipeline().parameters.start_date, "
    "mul(-1, pipeline().parameters.calculation_lookback_days)), 'yyyy-MM-dd'), "
    "formatDateTime(addDays(utcNow(), mul(-1, "
    "add(pipeline().parameters.trailing_days, "
    "pipeline().parameters.calculation_lookback_days))), 'yyyy-MM-dd'))"
)
INGESTION_START_EXPRESSION = (
    "@if(and(not(empty(pipeline().parameters.earliest_date)), "
    f"greater(pipeline().parameters.earliest_date, {_INGESTION_START_CANDIDATE})), "
    "pipeline().parameters.earliest_date, "
    f"{_INGESTION_START_CANDIDATE})"
)


@dataclass(frozen=True, slots=True)
class PipelineBounds:
    ingestion_start: date
    ingestion_end: date
    replacement_start: date
    replacement_end: date


def resolve_pipeline_bounds(
    *,
    run_mode: str,
    today: date,
    trailing_days: int = 28,
    start_date: date | None = None,
    end_date: date | None = None,
    calculation_lookback_days: int = 27,
    earliest_date: date | None = None,
) -> PipelineBounds:
    """Resolve Bronze/Silver input and Gold replacement bounds."""
    if run_mode == "backfill":
        if start_date is None or end_date is None:
            raise ValueError("backfill bounds are required")
        replacement_start = start_date
        replacement_end = end_date
    elif run_mode == "daily":
        replacement_end = today - timedelta(days=1)
        replacement_start = today - timedelta(days=trailing_days)
    else:
        raise ValueError("run_mode must be daily or backfill")
    if replacement_start > replacement_end:
        raise ValueError("replacement start must not be after end")
    ingestion_start = replacement_start - timedelta(
        days=calculation_lookback_days
    )
    if earliest_date is not None:
        ingestion_start = max(ingestion_start, earliest_date)
    return PipelineBounds(
        ingestion_start,
        replacement_end,
        replacement_start,
        replacement_end,
    )
