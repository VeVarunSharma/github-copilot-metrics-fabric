"""Lazy Fabric Spark/Delta adapter for Gold analytics."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date, timedelta
from typing import Any

from copilot_metrics_fabric.gold import (
    GOLD_CONTRACTS,
    GoldBatch,
    GoldBuildOptions,
    GoldReplacementWindow,
    build_gold,
)
from copilot_metrics_fabric.silver import DataType, TableContract


def spark_struct_type(contract: str | TableContract) -> Any:
    """Build a PySpark schema without importing PySpark at package import time."""

    try:
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )
    except ImportError as error:
        raise RuntimeError(
            "PySpark is required only when materializing Gold Spark DataFrames"
        ) from error

    selected = GOLD_CONTRACTS[contract] if isinstance(contract, str) else contract
    types = {
        DataType.STRING: StringType,
        DataType.INTEGER: LongType,
        DataType.NUMBER: DoubleType,
        DataType.BOOLEAN: BooleanType,
        DataType.DATE: DateType,
    }
    return StructType(
        [
            StructField(
                column.name, types[column.data_type](), nullable=column.nullable
            )
            for column in selected.columns
        ]
    )


def build_dataframes(
    spark: Any,
    silver_frames: Mapping[str, Any],
    *,
    options: GoldBuildOptions | None = None,
    report_status_rows: list[dict[str, Any]] | None = None,
    primary_team_mapping: Mapping[Any, Any] | None = None,
    current_user_frame: Any | None = None,
) -> dict[str, Any]:
    """Collect bounded Silver inputs and full user history for current state."""

    silver_rows = {
        name: [row.asDict(recursive=True) for row in frame.collect()]
        for name, frame in silver_frames.items()
    }
    current_user_rows = (
        [row.asDict(recursive=True) for row in current_user_frame.collect()]
        if current_user_frame is not None
        else None
    )
    batch = build_gold(
        silver_rows,
        options=options,
        report_status_rows=report_status_rows or (),
        primary_team_mapping=primary_team_mapping,
        current_user_rows=current_user_rows,
    )
    return create_dataframes(spark, batch)


def create_dataframes(spark: Any, batch: GoldBatch) -> dict[str, Any]:
    """Create one explicitly-schemaed Spark DataFrame per Gold contract."""

    frames = {}
    for name, contract in GOLD_CONTRACTS.items():
        values = [_spark_values(row, contract) for row in batch.rows[name]]
        frames[name] = spark.createDataFrame(values, spark_struct_type(contract))
    return frames


def merge_dataframes(
    spark: Any,
    frames: Mapping[str, Any],
    *,
    schema: str = "gold",
    replacement_windows: tuple[GoldReplacementWindow, ...]
    | list[GoldReplacementWindow] = (),
) -> None:
    """Atomically replace bounded windows, or upsert without them."""

    try:
        from delta.tables import DeltaTable
    except ImportError as error:
        raise RuntimeError("Delta Lake support is required for Gold upserts") from error

    schema = _schema_identifier(schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
    for name, frame in frames.items():
        target = f"`{schema}`.`{name}`"
        if not spark.catalog.tableExists(f"{schema}.{name}"):
            frame.write.format("delta").mode("overwrite").saveAsTable(target)
            continue
        contract = GOLD_CONTRACTS[name]
        replacement = _replacement_condition(contract, replacement_windows)
        condition = " AND ".join(
            f"target.`{column}` <=> source.`{column}`"
            for column in contract.upsert_key
        )
        merge = (
            DeltaTable.forName(spark, f"{schema}.{name}")
            .alias("target")
            .merge(frame.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )
        if replacement:
            _assert_frame_within(frame, replacement, target)
            merge = merge.whenNotMatchedBySourceDelete(condition=replacement)
        merge.execute()


def _replacement_condition(
    contract: TableContract,
    windows: tuple[GoldReplacementWindow, ...] | list[GoldReplacementWindow],
) -> str:
    columns = {column.name for column in contract.columns}
    clauses = []
    for window in _normalized_windows(windows):
        scope = (
            f"`scope_kind` = '{_sql_literal(window.scope_kind)}' AND "
            f"`scope_slug` = '{_sql_literal(window.scope_slug)}'"
        )
        if "day" in columns:
            start = _date_literal(window.start_day)
            end = _date_literal(window.end_day)
            scope += f" AND `day` BETWEEN DATE '{start}' AND DATE '{end}'"
        clauses.append(f"({scope})")
    return " OR ".join(sorted(set(clauses)))


def _normalized_windows(
    windows: tuple[GoldReplacementWindow, ...] | list[GoldReplacementWindow],
) -> list[GoldReplacementWindow]:
    grouped: dict[tuple[str, str], list[tuple[date, date]]] = {}
    for window in windows:
        validated = GoldReplacementWindow(
            window.scope_kind, window.scope_slug, window.start_day, window.end_day
        )
        grouped.setdefault((validated.scope_kind, validated.scope_slug), []).append(
            (_day(validated.start_day), _day(validated.end_day))
        )

    normalized = []
    for (scope_kind, scope_slug), ranges in sorted(grouped.items()):
        merged: list[list[date]] = []
        for start, end in sorted(ranges):
            if merged and start <= merged[-1][1] + timedelta(days=1):
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        normalized.extend(
            GoldReplacementWindow(scope_kind, scope_slug, start, end)
            for start, end in merged
        )
    return normalized


def _assert_frame_within(frame: Any, predicate: str, target: str) -> None:
    if frame.filter(f"NOT ({predicate})").limit(1).count():
        raise ValueError(
            f"source rows for {target} fall outside the bounded replacement window"
        )


def _day(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _date_literal(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(value).isoformat()


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _schema_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("schema must be a simple SQL identifier")
    return value


def _spark_values(row: Mapping[str, Any], contract: TableContract) -> tuple[Any, ...]:
    values = []
    for column in contract.columns:
        value = row.get(column.name)
        if column.data_type == DataType.DATE and isinstance(value, str):
            value = date.fromisoformat(value)
        elif column.data_type == DataType.NUMBER and isinstance(value, int):
            value = float(value)
        values.append(value)
    return tuple(values)

