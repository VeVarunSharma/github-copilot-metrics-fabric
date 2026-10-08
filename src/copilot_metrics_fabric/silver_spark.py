"""Optional Fabric Spark adapter for the pure-Python Silver contracts."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date
from typing import Any

from copilot_metrics_fabric.silver import (
    CONTRACTS,
    DataType,
    NormalizedBatch,
    Snapshot,
    TableContract,
)


def spark_struct_type(contract: str | TableContract) -> Any:
    """Build a PySpark StructType lazily so unit tests do not require Spark."""

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
            "PySpark is required only when materializing Silver Spark DataFrames"
        ) from error

    selected = CONTRACTS[contract] if isinstance(contract, str) else contract
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


def create_dataframes(spark: Any, batch: NormalizedBatch) -> dict[str, Any]:
    """Create one explicitly-schemaed DataFrame per Silver contract."""

    frames = {}
    for name, contract in CONTRACTS.items():
        rows = [_spark_values(row, contract) for row in batch.rows[name]]
        frames[name] = spark.createDataFrame(rows, spark_struct_type(contract))
    return frames


def merge_dataframes(
    spark: Any,
    frames: Mapping[str, Any],
    *,
    schema: str = "silver",
    snapshots: list[Snapshot] | tuple[Snapshot, ...] = (),
) -> None:
    """Atomically replace bounded snapshots, or upsert without metadata."""

    try:
        from delta.tables import DeltaTable
    except ImportError as error:
        raise RuntimeError(
            "Delta Lake support is required for Silver upserts"
        ) from error

    schema = _schema_identifier(schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
    for name, frame in frames.items():
        target = f"`{schema}`.`{name}`"
        if not spark.catalog.tableExists(f"{schema}.{name}"):
            frame.write.format("delta").mode("overwrite").saveAsTable(target)
            continue
        contract = CONTRACTS[name]
        replacement = _snapshot_condition(contract, snapshots)
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


def _snapshot_condition(
    contract: TableContract, snapshots: list[Snapshot] | tuple[Snapshot, ...]
) -> str:
    columns = {column.name for column in contract.columns}
    identity = {
        "scope_kind",
        "scope_slug",
        "source_report_type",
        "source_report_day",
    }
    if not identity <= columns:
        return ""
    clauses = []
    for snapshot in snapshots:
        if snapshot.scope_kind not in {"enterprise", "organization"}:
            raise ValueError("snapshot scope_kind must be enterprise or organization")
        if not snapshot.scope_slug.strip():
            raise ValueError("snapshot scope_slug must not be empty")
        report_day = date.fromisoformat(snapshot.report_day).isoformat()
        clauses.append(
            "("
            f"`scope_kind` = '{_sql_literal(snapshot.scope_kind)}' AND "
            f"`scope_slug` = '{_sql_literal(snapshot.scope_slug)}' AND "
            f"`source_report_type` = '{_sql_literal(snapshot.report_type)}' AND "
            f"`source_report_day` = DATE '{report_day}'"
            ")"
        )
    return " OR ".join(sorted(set(clauses)))


def _assert_frame_within(frame: Any, predicate: str, target: str) -> None:
    if frame.filter(f"NOT ({predicate})").limit(1).count():
        raise ValueError(
            f"source rows for {target} fall outside the bounded replacement snapshots"
        )


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
