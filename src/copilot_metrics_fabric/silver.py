"""Pure-Python Silver normalization contracts for retained Bronze NDJSON."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any


class SilverValidationError(ValueError):
    """Raised when a Silver normalization request is invalid."""


class DataType(str, Enum):
    STRING = "string"
    INTEGER = "long"
    NUMBER = "double"
    BOOLEAN = "boolean"
    DATE = "date"


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    data_type: DataType
    nullable: bool = True


@dataclass(frozen=True, slots=True)
class TableContract:
    name: str
    columns: tuple[Column, ...]
    upsert_key: tuple[str, ...]

    def row(self, values: Mapping[str, Any]) -> dict[str, Any]:
        return {column.name: values.get(column.name) for column in self.columns}

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "upsert_key": list(self.upsert_key),
            "columns": [
                {
                    "name": column.name,
                    "type": column.data_type.value,
                    "nullable": column.nullable,
                }
                for column in self.columns
            ],
        }


@dataclass(frozen=True, slots=True)
class SourceContext:
    scope_kind: str
    scope_slug: str
    report_type: str
    report_day: str | date | None = None
    ingestion_id: str | None = None
    ingested_at: str | None = None
    source_path: str | None = None

    def __post_init__(self) -> None:
        if self.scope_kind not in {"enterprise", "organization"}:
            raise SilverValidationError(
                "scope_kind must be 'enterprise' or 'organization'"
            )
        if not self.scope_slug:
            raise SilverValidationError("scope_slug is required")
        if _report_type(self.report_type) not in NORMALIZERS:
            raise SilverValidationError(f"unsupported report type: {self.report_type}")


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One complete Bronze snapshot selected for Silver replacement."""

    scope_kind: str
    scope_slug: str
    report_type: str
    report_day: str
    ingestion_id: str | None
    ingested_at: str | None
    status: str = "success"

    def __post_init__(self) -> None:
        if self.scope_kind not in {"enterprise", "organization"}:
            raise SilverValidationError(
                "snapshot scope_kind must be 'enterprise' or 'organization'"
            )
        if not self.scope_slug.strip():
            raise SilverValidationError("snapshot scope_slug is required")
        report_type = _report_type(self.report_type)
        if report_type not in NORMALIZERS:
            raise SilverValidationError(
                f"unsupported snapshot report type: {self.report_type}"
            )
        report_day = _day(self.report_day)
        if report_day is None:
            raise SilverValidationError("snapshot report_day must be an ISO date")
        if self.status not in {"success", "no_data"}:
            raise SilverValidationError("snapshot status must be success or no_data")
        object.__setattr__(self, "report_type", report_type)
        object.__setattr__(self, "report_day", report_day)

    @classmethod
    def from_context(
        cls, context: SourceContext, *, status: str = "success"
    ) -> Snapshot:
        report_day = _day(context.report_day)
        if report_day is None:
            raise SilverValidationError(
                "report_day is required for snapshot replacement"
            )
        if status not in {"success", "no_data"}:
            raise SilverValidationError("snapshot status must be success or no_data")
        return cls(
            context.scope_kind,
            context.scope_slug,
            _report_type(context.report_type),
            report_day,
            context.ingestion_id,
            context.ingested_at,
            status,
        )

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (
            self.scope_kind,
            self.scope_slug,
            self.report_type,
            self.report_day,
        )

    @property
    def rank(self) -> tuple[str, str]:
        return (self.ingested_at or "", self.ingestion_id or "")


@dataclass(slots=True)
class NormalizedBatch:
    rows: dict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    snapshots: list[Snapshot] = field(default_factory=list)

    def add(self, table: str, row: Mapping[str, Any]) -> None:
        self.rows[table].append(CONTRACTS[table].row(row))

    def record_snapshot(
        self, context: SourceContext, *, status: str = "success"
    ) -> None:
        self.snapshots.append(Snapshot.from_context(context, status=status))

    def extend(self, other: NormalizedBatch) -> None:
        for table, rows in other.rows.items():
            self.rows[table].extend(rows)
        self.snapshots.extend(other.snapshots)

    def finalize(self) -> NormalizedBatch:
        latest_snapshots = _latest_snapshots(self.snapshots)
        if latest_snapshots:
            for name, contract in CONTRACTS.items():
                if not any(
                    column.name == "source_report_type"
                    for column in contract.columns
                ):
                    continue
                self.rows[name] = [
                    row
                    for row in self.rows[name]
                    if _row_is_in_latest_snapshot(row, latest_snapshots)
                ]
            self.snapshots = sorted(
                latest_snapshots.values(), key=lambda snapshot: snapshot.key
            )
        quality = self.rows["silver_quality_issues"]
        for name, contract in CONTRACTS.items():
            if name in {
                "silver_quarantine",
                "silver_schema_drift",
                "silver_quality_issues",
            }:
                continue
            winners: dict[tuple[Any, ...], dict[str, Any]] = {}
            counts: dict[tuple[Any, ...], int] = defaultdict(int)
            for row in self.rows[name]:
                key = tuple(row[column] for column in contract.upsert_key)
                counts[key] += 1
                current = winners.get(key)
                if current is None or _source_rank(row) > _source_rank(current):
                    winners[key] = row
            for key, count in counts.items():
                if count > 1:
                    quality.append(
                        CONTRACTS["silver_quality_issues"].row(
                            {
                                "target_table": name,
                                "upsert_key_json": _json(list(key)),
                                "check_name": "duplicate_upsert_key",
                                "severity": "warning",
                                "message": (
                                    f"{count} rows shared an upsert key; "
                                    "the deterministic latest row was retained"
                                ),
                            }
                        )
                    )
            self.rows[name] = [
                winners[key] for key in sorted(winners, key=lambda value: repr(value))
            ]
        for name in CONTRACTS:
            self.rows[name] = list(self.rows.get(name, ()))
        return self


BASE_COLUMNS = (
    Column("scope_kind", DataType.STRING, False),
    Column("scope_slug", DataType.STRING, False),
    Column("day", DataType.DATE, False),
    Column("day_key", DataType.INTEGER, False),
    Column("enterprise_id", DataType.STRING),
    Column("organization_id", DataType.STRING),
)
LINEAGE_COLUMNS = (
    Column("source_report_type", DataType.STRING, False),
    Column("source_report_day", DataType.DATE, False),
    Column("source_ingestion_id", DataType.STRING),
    Column("source_ingested_at", DataType.STRING),
    Column("source_path", DataType.STRING),
    Column("source_record_hash", DataType.STRING, False),
)

COUNT_FIELDS = (
    "ai_credits_used",
    "user_initiated_interaction_count",
    "code_generation_activity_count",
    "code_acceptance_activity_count",
    "loc_suggested_to_add_sum",
    "loc_suggested_to_delete_sum",
    "loc_added_sum",
    "loc_deleted_sum",
    "distinct_custom_agent_use_count",
    "distinct_mcp_use_count",
    "distinct_plugin_use_count",
    "distinct_skill_use_count",
    "distinct_slash_cmd_use_count",
    "daily_active_users",
    "daily_active_cli_users",
    "daily_active_copilot_app_users",
    "daily_active_copilot_cloud_agent_users",
    "daily_active_copilot_code_review_users",
    "daily_active_vscode_agent_users",
    "daily_passive_copilot_code_review_users",
    "weekly_active_users",
    "weekly_active_copilot_cloud_agent_users",
    "weekly_active_copilot_code_review_users",
    "weekly_active_vscode_agent_users",
    "weekly_passive_copilot_code_review_users",
    "monthly_active_users",
    "monthly_active_agent_users",
    "monthly_active_chat_users",
    "monthly_active_copilot_cloud_agent_users",
    "monthly_active_copilot_code_review_users",
    "monthly_active_vscode_agent_users",
    "monthly_passive_copilot_code_review_users",
)
USAGE_COLUMNS = tuple(
    Column(name, DataType.NUMBER if name == "ai_credits_used" else DataType.INTEGER)
    for name in COUNT_FIELDS
)
USED_FIELDS = (
    "used_agent",
    "used_chat",
    "used_cli",
    "used_copilot_app",
    "used_copilot_coding_agent",
    "used_copilot_cloud_agent",
    "used_copilot_code_review_active",
    "used_copilot_code_review_passive",
    "used_vscode_agent",
)
USED_COLUMNS = tuple(Column(name, DataType.BOOLEAN) for name in USED_FIELDS)
PR_FIELDS = (
    "total_reviewed",
    "total_created",
    "total_created_by_copilot",
    "total_reviewed_by_copilot",
    "total_merged",
    "median_minutes_to_merge",
    "total_suggestions",
    "total_applied_suggestions",
    "total_merged_created_by_copilot",
    "median_minutes_to_merge_copilot_authored",
    "total_copilot_suggestions",
    "total_copilot_applied_suggestions",
    "total_merged_reviewed_by_copilot",
    "median_minutes_to_merge_copilot_reviewed",
)
PR_COLUMNS = tuple(
    Column(
        f"pr_{name}",
        DataType.NUMBER if name.startswith("median_") else DataType.INTEGER,
    )
    for name in PR_FIELDS
)

BREAKDOWN_METRICS = (
    Column("user_initiated_interaction_count", DataType.INTEGER),
    Column("interaction_count", DataType.INTEGER),
    Column("session_count", DataType.INTEGER),
    Column("code_generation_activity_count", DataType.INTEGER),
    Column("code_acceptance_activity_count", DataType.INTEGER),
    Column("loc_suggested_to_add_sum", DataType.INTEGER),
    Column("loc_suggested_to_delete_sum", DataType.INTEGER),
    Column("loc_added_sum", DataType.INTEGER),
    Column("loc_deleted_sum", DataType.INTEGER),
)


def _contract(
    name: str,
    columns: tuple[Column, ...],
    key: tuple[str, ...],
) -> TableContract:
    return TableContract(name, BASE_COLUMNS + columns + LINEAGE_COLUMNS, key)


ENTITY_KEY = ("scope_kind", "scope_slug", "day")
USER_KEY = ENTITY_KEY + ("user_id",)
REPOSITORY_KEY = ENTITY_KEY + ("repo_id",)

CONTRACTS: dict[str, TableContract] = {
    "entity_daily": _contract(
        "entity_daily",
        USAGE_COLUMNS
        + PR_COLUMNS
        + (
            Column("report_start_day", DataType.DATE),
            Column("report_end_day", DataType.DATE),
        ),
        ENTITY_KEY,
    ),
    "user_daily": _contract(
        "user_daily",
        (
            Column("user_id", DataType.INTEGER, False),
            Column("user_login", DataType.STRING),
        )
        + USAGE_COLUMNS
        + USED_COLUMNS
        + PR_COLUMNS
        + (
            Column("adoption_phase", DataType.STRING),
            Column("adoption_phase_number", DataType.INTEGER),
            Column("adoption_phase_version", DataType.STRING),
        ),
        USER_KEY,
    ),
    "user_team_daily": _contract(
        "user_team_daily",
        (
            Column("user_id", DataType.INTEGER, False),
            Column("user_login", DataType.STRING),
            Column("team_id", DataType.INTEGER, False),
            Column("team_slug", DataType.STRING),
        ),
        USER_KEY + ("team_id",),
    ),
    "repository_daily": _contract(
        "repository_daily",
        (
            Column("repo_id", DataType.INTEGER, False),
            Column("repo_owner_name", DataType.STRING),
            Column("repo_name", DataType.STRING),
            Column("repo_visibility", DataType.STRING),
        )
        + PR_COLUMNS,
        REPOSITORY_KEY,
    ),
}


def _breakdown_contract(
    table: str, dimensions: tuple[Column, ...]
) -> TableContract:
    dimension_names = tuple(column.name for column in dimensions)
    return _contract(
        table,
        (Column("user_id", DataType.INTEGER),) + dimensions + BREAKDOWN_METRICS,
        ENTITY_KEY + ("user_id",) + dimension_names,
    )


CONTRACTS.update(
    {
        "feature_daily": _breakdown_contract(
            "feature_daily", (Column("feature", DataType.STRING, False),)
        ),
        "ide_daily": _contract(
            "ide_daily",
            (
                Column("user_id", DataType.INTEGER),
                Column("ide", DataType.STRING, False),
                Column("ide_version", DataType.STRING),
                Column("ide_version_sampled_at", DataType.STRING),
                Column("plugin", DataType.STRING),
                Column("plugin_version", DataType.STRING),
                Column("plugin_version_sampled_at", DataType.STRING),
            )
            + BREAKDOWN_METRICS,
            ENTITY_KEY + ("user_id", "ide"),
        ),
        "language_feature_daily": _breakdown_contract(
            "language_feature_daily",
            (
                Column("language", DataType.STRING, False),
                Column("feature", DataType.STRING, False),
            ),
        ),
        "language_model_daily": _breakdown_contract(
            "language_model_daily",
            (
                Column("language", DataType.STRING, False),
                Column("model", DataType.STRING, False),
            ),
        ),
        "model_feature_daily": _breakdown_contract(
            "model_feature_daily",
            (
                Column("model", DataType.STRING, False),
                Column("feature", DataType.STRING, False),
            ),
        ),
        "custom_agent_daily": _breakdown_contract(
            "custom_agent_daily",
            (Column("custom_agent", DataType.STRING, False),),
        ),
        "third_party_agent_daily": _breakdown_contract(
            "third_party_agent_daily",
            (
                Column("agent_id", DataType.STRING, False),
                Column("agent_name", DataType.STRING),
            ),
        ),
        "mcp_daily": _breakdown_contract(
            "mcp_daily", (Column("mcp", DataType.STRING, False),)
        ),
        "plugin_daily": _breakdown_contract(
            "plugin_daily", (Column("plugin", DataType.STRING, False),)
        ),
        "skill_daily": _breakdown_contract(
            "skill_daily", (Column("skill", DataType.STRING, False),)
        ),
        "slash_command_daily": _breakdown_contract(
            "slash_command_daily", (Column("slash_command", DataType.STRING, False),)
        ),
        "adoption_phase_daily": _contract(
            "adoption_phase_daily",
            (
                Column("user_id", DataType.INTEGER),
                Column("phase", DataType.STRING, False),
                Column("phase_number", DataType.INTEGER),
                Column("total_engaged_users", DataType.INTEGER),
                Column("users_in_phase_28d", DataType.INTEGER),
                Column("total_pull_requests_merged", DataType.INTEGER),
                Column("avg_code_acceptance_activities", DataType.NUMBER),
                Column("avg_code_generation_activities", DataType.NUMBER),
                Column("avg_loc_added", DataType.NUMBER),
                Column("avg_loc_deleted", DataType.NUMBER),
                Column("avg_pull_requests_created", DataType.NUMBER),
                Column("avg_pull_requests_median_minutes_to_merge", DataType.NUMBER),
                Column("avg_pull_requests_merged", DataType.NUMBER),
                Column("avg_pull_requests_minutes_to_review", DataType.NUMBER),
                Column("avg_pull_requests_review_cycles", DataType.NUMBER),
                Column("avg_pull_requests_reviewed", DataType.NUMBER),
                Column("avg_user_initiated_interactions", DataType.NUMBER),
            ),
            ENTITY_KEY + ("user_id", "phase"),
        ),
        "cli_daily": _contract(
            "cli_daily",
            (
                Column("user_id", DataType.INTEGER),
                Column("prompt_count", DataType.INTEGER),
                Column("request_count", DataType.INTEGER),
                Column("session_count", DataType.INTEGER),
                Column("avg_tokens_per_request", DataType.NUMBER),
                Column("output_tokens_sum", DataType.INTEGER),
                Column("prompt_tokens_sum", DataType.INTEGER),
                Column("last_known_cli_version", DataType.STRING),
                Column("last_known_cli_version_sampled_at", DataType.STRING),
            ),
            ENTITY_KEY + ("user_id",),
        ),
        "copilot_app_daily": _contract(
            "copilot_app_daily",
            (
                Column("user_id", DataType.INTEGER),
                Column("prompt_count", DataType.INTEGER),
                Column("request_count", DataType.INTEGER),
                Column("session_count", DataType.INTEGER),
                Column("avg_tokens_per_request", DataType.NUMBER),
                Column("output_tokens_sum", DataType.INTEGER),
                Column("prompt_tokens_sum", DataType.INTEGER),
            ),
            ENTITY_KEY + ("user_id",),
        ),
        "vscode_agent_daily": _contract(
            "vscode_agent_daily",
            (
                Column("user_id", DataType.INTEGER),
                Column("session_count", DataType.INTEGER),
                Column("total_user_messages", DataType.INTEGER),
            ),
            ENTITY_KEY + ("user_id",),
        ),
        "feature_engagement_daily": _contract(
            "feature_engagement_daily",
            (
                Column("feature", DataType.STRING, False),
                Column("active_user_count", DataType.INTEGER),
                Column("engaged_user_count", DataType.INTEGER),
            ),
            ENTITY_KEY + ("feature",),
        ),
        "repository_pr_comment_type_daily": _contract(
            "repository_pr_comment_type_daily",
            (
                Column("repo_id", DataType.INTEGER, False),
                Column("comment_type", DataType.STRING, False),
                Column("total_copilot_suggestions", DataType.INTEGER),
                Column("total_copilot_applied_suggestions", DataType.INTEGER),
            ),
            REPOSITORY_KEY + ("comment_type",),
        ),
        "repository_pr_review_time_daily": _contract(
            "repository_pr_review_time_daily",
            (
                Column("repo_id", DataType.INTEGER, False),
                Column("authored_by", DataType.STRING, False),
                Column("reviewed_by", DataType.STRING, False),
                Column("total_merged", DataType.INTEGER),
                Column("median_minutes_ready_to_first_review", DataType.NUMBER),
                Column("p90_minutes_ready_to_first_review", DataType.NUMBER),
                Column("median_minutes_first_to_final_review", DataType.NUMBER),
                Column("p90_minutes_first_to_final_review", DataType.NUMBER),
                Column("median_minutes_final_review_to_merge", DataType.NUMBER),
                Column("p90_minutes_final_review_to_merge", DataType.NUMBER),
            ),
            REPOSITORY_KEY + ("authored_by", "reviewed_by"),
        ),
    }
)

CONTRACTS.update(
    {
        "silver_quarantine": TableContract(
            "silver_quarantine",
            (
                Column("scope_kind", DataType.STRING, False),
                Column("scope_slug", DataType.STRING, False),
                Column("report_type", DataType.STRING, False),
                Column("target_table", DataType.STRING, False),
                Column("day", DataType.DATE),
                Column("source_ingestion_id", DataType.STRING),
                Column("source_path", DataType.STRING),
                Column("source_record_hash", DataType.STRING, False),
                Column("nested_field", DataType.STRING),
                Column("nested_index", DataType.INTEGER),
                Column("reason_codes_json", DataType.STRING, False),
                Column("raw_record_json", DataType.STRING, False),
            ),
            ("source_record_hash", "target_table", "nested_field", "nested_index"),
        ),
        "silver_schema_drift": TableContract(
            "silver_schema_drift",
            (
                Column("scope_kind", DataType.STRING, False),
                Column("scope_slug", DataType.STRING, False),
                Column("report_type", DataType.STRING, False),
                Column("day", DataType.DATE),
                Column("source_record_hash", DataType.STRING, False),
                Column("field_path", DataType.STRING, False),
                Column("observed_type", DataType.STRING, False),
                Column("sample_value_json", DataType.STRING, False),
            ),
            ("source_record_hash", "field_path"),
        ),
        "silver_quality_issues": TableContract(
            "silver_quality_issues",
            (
                Column("target_table", DataType.STRING, False),
                Column("upsert_key_json", DataType.STRING),
                Column("check_name", DataType.STRING, False),
                Column("severity", DataType.STRING, False),
                Column("message", DataType.STRING, False),
            ),
            ("target_table", "upsert_key_json", "check_name"),
        ),
    }
)

ARRAYS = {
    "totals_by_feature": ("feature_daily", ("feature",)),
    "totals_by_ide": ("ide_daily", ("ide",)),
    "totals_by_language_feature": (
        "language_feature_daily",
        ("language", "feature"),
    ),
    "totals_by_language_model": ("language_model_daily", ("language", "model")),
    "totals_by_model_feature": ("model_feature_daily", ("model", "feature")),
    "totals_by_custom_agent": ("custom_agent_daily", ("custom_agent",)),
    "totals_by_3rd_party_agent": (
        "third_party_agent_daily",
        ("agent_id",),
    ),
    "totals_by_mcp": ("mcp_daily", ("mcp",)),
    "totals_by_plugin": ("plugin_daily", ("plugin",)),
    "totals_by_skill": ("skill_daily", ("skill",)),
    "totals_by_slash_cmd": ("slash_command_daily", ("slash_command",)),
    "totals_by_ai_adoption_phase": ("adoption_phase_daily", ("phase",)),
}

COMMON_EXPECTED = {
    "day",
    "enterprise_id",
    "organization_id",
    *COUNT_FIELDS,
    *USED_FIELDS,
    *ARRAYS,
    "totals_by_cli",
    "totals_by_copilot_app",
    "totals_by_vscode_agent",
    "pull_requests",
    "pull_request_review_times",
    "ai_adoption_phase",
}


def contracts() -> dict[str, dict[str, Any]]:
    """Return JSON-compatible table contracts for notebooks and documentation."""

    return {name: contract.as_dict() for name, contract in CONTRACTS.items()}


def normalize_ndjson(
    content: str | bytes, context: SourceContext
) -> NormalizedBatch:
    """Normalize newline-delimited JSON without requiring Spark."""

    text = content.decode("utf-8") if isinstance(content, bytes) else content
    records: list[Mapping[str, Any]] = []
    batch = NormalizedBatch()
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            _quarantine(
                batch,
                context,
                {"raw_line": line},
                "unknown",
                ["invalid_json"],
                source_hash=hashlib.sha256(line.encode()).hexdigest(),
            )
            continue
        if not isinstance(value, dict):
            _quarantine(
                batch,
                context,
                {"value": value, "line_number": line_number},
                "unknown",
                ["record_must_be_object"],
            )
            continue
        records.append(value)
    normalized = normalize_records(records, context)
    for table, rows in normalized.rows.items():
        batch.rows[table].extend(rows)
    return batch.finalize()


def normalize_records(
    records: Iterable[Mapping[str, Any]], context: SourceContext
) -> NormalizedBatch:
    """Normalize Bronze records into explicit Silver table rows."""

    batch = NormalizedBatch()
    batch.record_snapshot(context)
    report_type = _report_type(context.report_type)
    normalizer = NORMALIZERS[report_type]
    for record in records:
        if not isinstance(record, Mapping):
            _quarantine(
                batch, context, {"value": record}, "unknown", ["record_must_be_object"]
            )
            continue
        if report_type == "entity" and isinstance(record.get("day_totals"), list):
            _normalize_entity_wrapper(batch, record, context)
        else:
            normalizer(batch, record, context)
    return batch.finalize()


def _normalize_entity_wrapper(
    batch: NormalizedBatch, record: Mapping[str, Any], context: SourceContext
) -> None:
    inherited = {
        "enterprise_id": record.get("enterprise_id"),
        "organization_id": record.get("organization_id"),
        "report_start_day": record.get("report_start_day"),
        "report_end_day": record.get("report_end_day"),
    }
    for item in record["day_totals"]:
        if not isinstance(item, Mapping):
            _quarantine(
                batch,
                context,
                {"value": item},
                "entity_daily",
                ["day_totals_entry_must_be_object"],
                nested_field="day_totals",
            )
            continue
        _normalize_entity(batch, {**inherited, **item}, context)
    engagement = record.get("copilot_feature_engagement")
    if isinstance(engagement, Mapping):
        day = record.get("report_end_day")
        base_record = {**inherited, "day": day}
        source_hash = _hash(record)
        base, errors = _base(base_record, context, source_hash)
        if errors:
            return
        entries = engagement.get("totals_by_feature", [])
        if isinstance(entries, list):
            for index, item in enumerate(entries):
                _add_nested(
                    batch,
                    context,
                    base,
                    source_hash,
                    "feature_engagement_daily",
                    "copilot_feature_engagement.totals_by_feature",
                    index,
                    item,
                    ("feature",),
                    extra={"active_user_count": engagement.get("active_user_count")},
                )
    _drift(
        batch,
        record,
        {
            "day_totals",
            "enterprise_id",
            "organization_id",
            "report_start_day",
            "report_end_day",
            "copilot_feature_engagement",
        },
        context,
        _hash(record),
    )


def _normalize_entity(
    batch: NormalizedBatch, record: Mapping[str, Any], context: SourceContext
) -> None:
    _normalize_usage(batch, record, context, "entity_daily", required=())


def _normalize_user(
    batch: NormalizedBatch, record: Mapping[str, Any], context: SourceContext
) -> None:
    _normalize_usage(batch, record, context, "user_daily", required=("user_id",))


def _normalize_usage(
    batch: NormalizedBatch,
    record: Mapping[str, Any],
    context: SourceContext,
    table: str,
    required: tuple[str, ...],
) -> None:
    source_hash = _hash(record)
    base, errors = _base(record, context, source_hash)
    values = dict(base)
    for name in COUNT_FIELDS:
        value = record.get(name)
        if value is not None:
            valid_type = (
                _number(value)
                if name == "ai_credits_used"
                else _integer(value)
            )
            if not valid_type or value < 0:
                errors.append(f"{name}_must_be_nonnegative_number")
            else:
                values[name] = value
    for name in USED_FIELDS:
        value = record.get(name)
        if value is not None and not isinstance(value, bool):
            errors.append(f"{name}_must_be_boolean_or_null")
        else:
            values[name] = value
    for name in required:
        value = record.get(name)
        if not _integer(value) or value < 0:
            errors.append(f"{name}_must_be_nonnegative_integer")
        else:
            values[name] = value
    if record.get("user_login") is not None and not isinstance(
        record.get("user_login"), str
    ):
        errors.append("user_login_must_be_string")
    values["user_login"] = _optional_string(record.get("user_login"))
    for name in ("report_start_day", "report_end_day"):
        if record.get(name) is not None:
            parsed = _day(record.get(name))
            if parsed is None:
                errors.append(f"{name}_must_be_iso_date")
            else:
                values[name] = parsed
    if (
        values.get("report_start_day") is not None
        and values.get("report_end_day") is not None
        and values["report_start_day"] > values["report_end_day"]
    ):
        errors.append("report_start_day_must_not_follow_report_end_day")
    adoption = record.get("ai_adoption_phase")
    if adoption is not None:
        if not isinstance(adoption, Mapping):
            errors.append("ai_adoption_phase_must_be_object")
        else:
            phase_number = adoption.get("phase_number")
            if phase_number is not None and (
                not _integer(phase_number) or phase_number < 0
            ):
                errors.append(
                    "ai_adoption_phase.phase_number_must_be_nonnegative_integer"
                )
            for name in ("phase", "name", "version"):
                if adoption.get(name) is not None and not isinstance(
                    adoption.get(name), str
                ):
                    errors.append(f"ai_adoption_phase.{name}_must_be_string")
            values["adoption_phase"] = _optional_string(
                adoption.get("phase", adoption.get("name"))
            )
            values["adoption_phase_number"] = phase_number
            values["adoption_phase_version"] = _optional_string(
                adoption.get("version")
            )
            _drift(
                batch,
                adoption,
                {"phase", "name", "phase_number", "version"},
                context,
                source_hash,
                "ai_adoption_phase",
                values.get("day"),
            )
    pull_requests = record.get("pull_requests")
    if pull_requests is not None:
        if not isinstance(pull_requests, Mapping):
            errors.append("pull_requests_must_be_object")
        else:
            _flatten_pull_requests(values, pull_requests, errors)
            _drift(
                batch,
                pull_requests,
                set(PR_FIELDS) | {"copilot_suggestions_by_comment_type"},
                context,
                source_hash,
                "pull_requests",
                values.get("day"),
            )
    if (
        values.get("code_acceptance_activity_count") is not None
        and values.get("code_generation_activity_count") is not None
        and values["code_acceptance_activity_count"]
        > values["code_generation_activity_count"]
    ):
        errors.append("code_acceptance_count_exceeds_generation_count")
    if errors:
        _quarantine(batch, context, record, table, errors, source_hash=source_hash)
        return
    batch.add(table, values)
    _normalize_usage_children(batch, record, context, values, source_hash)
    expected = COMMON_EXPECTED | set(required) | {
        "user_login",
        "report_start_day",
        "report_end_day",
    }
    _drift(batch, record, expected, context, source_hash, day=values["day"])


def _normalize_usage_children(
    batch: NormalizedBatch,
    record: Mapping[str, Any],
    context: SourceContext,
    base: Mapping[str, Any],
    source_hash: str,
) -> None:
    for field_name, (table, dimensions) in ARRAYS.items():
        entries = record.get(field_name)
        if entries is None:
            continue
        if not isinstance(entries, list):
            _quarantine(
                batch,
                context,
                record,
                table,
                [f"{field_name}_must_be_array"],
                source_hash=source_hash,
                nested_field=field_name,
            )
            continue
        for index, item in enumerate(entries):
            _add_nested(
                batch,
                context,
                base,
                source_hash,
                table,
                field_name,
                index,
                item,
                dimensions,
            )
    for field_name, table in (
        ("totals_by_cli", "cli_daily"),
        ("totals_by_copilot_app", "copilot_app_daily"),
        ("totals_by_vscode_agent", "vscode_agent_daily"),
    ):
        value = record.get(field_name)
        if value is None:
            continue
        if not isinstance(value, Mapping):
            _quarantine(
                batch,
                context,
                record,
                table,
                [f"{field_name}_must_be_object"],
                source_hash=source_hash,
                nested_field=field_name,
            )
            continue
        _add_complex_scalar(batch, context, base, source_hash, table, field_name, value)


def _normalize_team(
    batch: NormalizedBatch, record: Mapping[str, Any], context: SourceContext
) -> None:
    source_hash = _hash(record)
    base, errors = _base(record, context, source_hash)
    values = dict(base)
    for name in ("user_id", "team_id"):
        value = record.get(name)
        if not _integer(value) or value < 0:
            errors.append(f"{name}_must_be_nonnegative_integer")
        else:
            values[name] = value
    for name in ("user_login", "slug"):
        if record.get(name) is not None and not isinstance(record.get(name), str):
            errors.append(f"{name}_must_be_string")
    values["user_login"] = _optional_string(record.get("user_login"))
    values["team_slug"] = _optional_string(record.get("slug"))
    if errors:
        _quarantine(
            batch, context, record, "user_team_daily", errors, source_hash=source_hash
        )
        return
    batch.add("user_team_daily", values)
    _drift(
        batch,
        record,
        {
            "day",
            "enterprise_id",
            "organization_id",
            "user_id",
            "user_login",
            "team_id",
            "slug",
        },
        context,
        source_hash,
        day=values["day"],
    )


def _normalize_repository(
    batch: NormalizedBatch, record: Mapping[str, Any], context: SourceContext
) -> None:
    source_hash = _hash(record)
    base, errors = _base(record, context, source_hash, require_organization=True)
    values = dict(base)
    repo_id = record.get("repo_id")
    if not _integer(repo_id) or repo_id < 0:
        errors.append("repo_id_must_be_nonnegative_integer")
    else:
        values["repo_id"] = repo_id
    for name in ("repo_owner_name", "repo_name", "repo_visibility"):
        if record.get(name) is not None and not isinstance(record.get(name), str):
            errors.append(f"{name}_must_be_string")
        values[name] = _optional_string(record.get(name))
    pull_requests = record.get("pull_requests")
    if pull_requests is not None and not isinstance(pull_requests, Mapping):
        errors.append("pull_requests_must_be_object")
    elif isinstance(pull_requests, Mapping):
        _flatten_pull_requests(values, pull_requests, errors)
        _drift(
            batch,
            pull_requests,
            set(PR_FIELDS) | {"copilot_suggestions_by_comment_type"},
            context,
            source_hash,
            "pull_requests",
            values.get("day"),
        )
    if errors:
        _quarantine(
            batch, context, record, "repository_daily", errors, source_hash=source_hash
        )
        return
    batch.add("repository_daily", values)
    if isinstance(pull_requests, Mapping):
        comments = pull_requests.get("copilot_suggestions_by_comment_type", [])
        if isinstance(comments, list):
            for index, item in enumerate(comments):
                _add_nested(
                    batch,
                    context,
                    values,
                    source_hash,
                    "repository_pr_comment_type_daily",
                    "pull_requests.copilot_suggestions_by_comment_type",
                    index,
                    item,
                    ("comment_type",),
                    extra={"repo_id": repo_id},
                )
    review_times = record.get("pull_request_review_times", [])
    if isinstance(review_times, list):
        for index, item in enumerate(review_times):
            _add_nested(
                batch,
                context,
                values,
                source_hash,
                "repository_pr_review_time_daily",
                "pull_request_review_times",
                index,
                item,
                ("authored_by", "reviewed_by"),
                extra={"repo_id": repo_id},
            )
    elif review_times is not None:
        _quarantine(
            batch,
            context,
            record,
            "repository_pr_review_time_daily",
            ["pull_request_review_times_must_be_array"],
            source_hash=source_hash,
            nested_field="pull_request_review_times",
        )
    _drift(
        batch,
        record,
        {
            "day",
            "enterprise_id",
            "organization_id",
            "repo_id",
            "repo_owner_name",
            "repo_name",
            "repo_visibility",
            "pull_requests",
            "pull_request_review_times",
        },
        context,
        source_hash,
        day=values["day"],
    )


NORMALIZERS = {
    "entity": _normalize_entity,
    "users": _normalize_user,
    "user-teams": _normalize_team,
    "repositories": _normalize_repository,
}


def _base(
    record: Mapping[str, Any],
    context: SourceContext,
    source_hash: str,
    *,
    require_organization: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    parsed_day = _day(record.get("day"))
    if parsed_day is None:
        errors.append("day_must_be_iso_date")
    expected_day = _day(context.report_day)
    if (
        parsed_day is not None
        and expected_day is not None
        and parsed_day != expected_day
    ):
        errors.append("day_does_not_match_bronze_partition")
    enterprise_id = _optional_string(record.get("enterprise_id"))
    organization_id = _optional_string(record.get("organization_id"))
    for name in ("enterprise_id", "organization_id"):
        if record.get(name) is not None and not isinstance(record.get(name), str):
            errors.append(f"{name}_must_be_string")
    if context.scope_kind == "enterprise" and not enterprise_id:
        errors.append("enterprise_id_is_required_for_enterprise_scope")
    if context.scope_kind == "organization" and not organization_id:
        errors.append("organization_id_is_required_for_organization_scope")
    if require_organization and not organization_id:
        errors.append("organization_id_is_required_for_repository")
    values = {
        "scope_kind": context.scope_kind,
        "scope_slug": context.scope_slug,
        "day": parsed_day,
        "day_key": int(parsed_day.replace("-", "")) if parsed_day else None,
        "enterprise_id": enterprise_id,
        "organization_id": organization_id,
        "source_report_type": _report_type(context.report_type),
        "source_report_day": _day(context.report_day),
        "source_ingestion_id": context.ingestion_id,
        "source_ingested_at": context.ingested_at,
        "source_path": context.source_path,
        "source_record_hash": source_hash,
    }
    return values, errors


def _add_nested(
    batch: NormalizedBatch,
    context: SourceContext,
    base: Mapping[str, Any],
    source_hash: str,
    table: str,
    field_name: str,
    index: int,
    item: Any,
    dimensions: tuple[str, ...],
    *,
    extra: Mapping[str, Any] | None = None,
) -> None:
    if not isinstance(item, Mapping):
        _quarantine(
            batch,
            context,
            {"value": item},
            table,
            ["nested_entry_must_be_object"],
            source_hash=source_hash,
            nested_field=field_name,
            nested_index=index,
            day=base.get("day"),
        )
        return
    values = dict(base)
    if extra:
        values.update(extra)
    errors: list[str] = []
    aliases = {"slash_command": "slash_cmd"}
    for target_name in dimensions:
        source_name = aliases.get(target_name, target_name)
        value = _optional_string(item.get(source_name))
        if not value:
            errors.append(f"{source_name}_is_required")
        values[target_name] = value
    for column in CONTRACTS[table].columns:
        if column.name in values or column.name in dimensions:
            continue
        if column.name in item:
            value = item[column.name]
            if column.data_type in {DataType.INTEGER, DataType.NUMBER}:
                valid_type = (
                    _integer(value)
                    if column.data_type == DataType.INTEGER
                    else _number(value)
                )
                if value is not None and (not valid_type or value < 0):
                    errors.append(f"{column.name}_must_be_nonnegative_number")
                else:
                    values[column.name] = value
            else:
                values[column.name] = value
    if table == "ide_daily":
        _flatten_version(
            values,
            item.get("last_known_ide_version"),
            "ide_version",
            "ide_version",
        )
        _flatten_version(
            values,
            item.get("last_known_plugin_version"),
            "plugin_version",
            "plugin_version",
        )
        plugin_value = item.get("last_known_plugin_version")
        if isinstance(plugin_value, Mapping):
            values["plugin"] = _optional_string(plugin_value.get("plugin"))
    if errors:
        _quarantine(
            batch,
            context,
            item,
            table,
            errors,
            source_hash=source_hash,
            nested_field=field_name,
            nested_index=index,
            day=base.get("day"),
        )
        return
    batch.add(table, values)
    expected = set(dimensions) | {
        column.name for column in BREAKDOWN_METRICS
    }
    if table == "slash_command_daily":
        expected = (expected - {"slash_command"}) | {"slash_cmd"}
    if table == "ide_daily":
        expected |= {"last_known_ide_version", "last_known_plugin_version"}
    _drift(
        batch,
        item,
        expected,
        context,
        source_hash,
        f"{field_name}[]",
        base.get("day"),
    )


def _add_complex_scalar(
    batch: NormalizedBatch,
    context: SourceContext,
    base: Mapping[str, Any],
    source_hash: str,
    table: str,
    field_name: str,
    item: Mapping[str, Any],
) -> None:
    values = dict(base)
    errors: list[str] = []
    for name in (
        "prompt_count",
        "request_count",
        "session_count",
        "total_user_messages",
    ):
        value = item.get(name)
        if value is not None and (not _integer(value) or value < 0):
            errors.append(f"{name}_must_be_nonnegative_integer")
        else:
            values[name] = value
    token_usage = item.get("token_usage")
    if token_usage is not None:
        if not isinstance(token_usage, Mapping):
            errors.append("token_usage_must_be_object")
        else:
            for name in (
                "avg_tokens_per_request",
                "output_tokens_sum",
                "prompt_tokens_sum",
            ):
                value = token_usage.get(name)
                if value is not None and (not _number(value) or value < 0):
                    errors.append(f"token_usage.{name}_must_be_nonnegative_number")
                else:
                    values[name] = value
            _drift(
                batch,
                token_usage,
                {
                    "avg_tokens_per_request",
                    "output_tokens_sum",
                    "prompt_tokens_sum",
                },
                context,
                source_hash,
                f"{field_name}.token_usage",
                base.get("day"),
            )
    if table == "cli_daily":
        version = item.get("last_known_cli_version")
        if version is not None and not isinstance(version, Mapping):
            errors.append("last_known_cli_version_must_be_object")
        elif isinstance(version, Mapping):
            values["last_known_cli_version"] = _optional_string(
                version.get("cli_version")
            )
            values["last_known_cli_version_sampled_at"] = _optional_string(
                version.get("sampled_at")
            )
            _drift(
                batch,
                version,
                {"cli_version", "sampled_at"},
                context,
                source_hash,
                f"{field_name}.last_known_cli_version",
                base.get("day"),
            )
    if errors:
        _quarantine(
            batch,
            context,
            item,
            table,
            errors,
            source_hash=source_hash,
            nested_field=field_name,
            day=base.get("day"),
        )
        return
    batch.add(table, values)
    _drift(
        batch,
        item,
        {
            "prompt_count",
            "request_count",
            "session_count",
            "total_user_messages",
            "token_usage",
            "last_known_cli_version",
        },
        context,
        source_hash,
        field_name,
        base.get("day"),
    )


def _flatten_version(
    values: dict[str, Any],
    item: Any,
    target_prefix: str,
    version_field: str,
) -> None:
    if isinstance(item, Mapping):
        values[target_prefix] = _optional_string(item.get(version_field))
        values[f"{target_prefix}_sampled_at"] = _optional_string(
            item.get("sampled_at")
        )


def _flatten_pull_requests(
    values: dict[str, Any],
    pull_requests: Mapping[str, Any],
    errors: list[str],
) -> None:
    for name in PR_FIELDS:
        value = pull_requests.get(name)
        valid_type = _number(value) if name.startswith("median_") else _integer(value)
        if value is not None and (not valid_type or value < 0):
            errors.append(f"pull_requests.{name}_must_be_nonnegative_number")
        else:
            values[f"pr_{name}"] = value
    logical_pairs = (
        ("total_created_by_copilot", "total_created"),
        ("total_reviewed_by_copilot", "total_reviewed"),
        ("total_merged_created_by_copilot", "total_merged"),
        ("total_merged_reviewed_by_copilot", "total_merged"),
        ("total_copilot_suggestions", "total_suggestions"),
        ("total_copilot_applied_suggestions", "total_applied_suggestions"),
    )
    for child, total in logical_pairs:
        if (
            _integer(pull_requests.get(child))
            and _integer(pull_requests.get(total))
            and pull_requests[child] > pull_requests[total]
        ):
            errors.append(f"pull_requests.{child}_exceeds_{total}")


def _drift(
    batch: NormalizedBatch,
    record: Mapping[str, Any],
    expected: set[str],
    context: SourceContext,
    source_hash: str,
    prefix: str = "",
    day: str | None = None,
) -> None:
    for name in sorted(set(record) - expected):
        value = record[name]
        batch.add(
            "silver_schema_drift",
            {
                "scope_kind": context.scope_kind,
                "scope_slug": context.scope_slug,
                "report_type": _report_type(context.report_type),
                "day": day,
                "source_record_hash": source_hash,
                "field_path": f"{prefix}.{name}" if prefix else name,
                "observed_type": type(value).__name__,
                "sample_value_json": _json(value)[:1000],
            },
        )


def _quarantine(
    batch: NormalizedBatch,
    context: SourceContext,
    record: Mapping[str, Any],
    target_table: str,
    reasons: list[str],
    *,
    source_hash: str | None = None,
    nested_field: str | None = None,
    nested_index: int | None = None,
    day: str | None = None,
) -> None:
    batch.add(
        "silver_quarantine",
        {
            "scope_kind": context.scope_kind,
            "scope_slug": context.scope_slug,
            "report_type": _report_type(context.report_type),
            "target_table": target_table,
            "day": day or _day(record.get("day")),
            "source_ingestion_id": context.ingestion_id,
            "source_path": context.source_path,
            "source_record_hash": source_hash or _hash(record),
            "nested_field": nested_field,
            "nested_index": nested_index,
            "reason_codes_json": _json(sorted(set(reasons))),
            "raw_record_json": _json(record),
        },
    )


def _source_rank(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("source_ingested_at") or ""),
        str(row.get("source_ingestion_id") or ""),
        str(row.get("source_path") or ""),
        str(row.get("source_record_hash") or ""),
    )


def select_latest_manifests(
    manifests: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Select the latest successful or explicit no-data manifest per snapshot."""

    winners: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for source in manifests:
        manifest = dict(source)
        if manifest.get("status") not in {"success", "no_data"}:
            continue
        scope = manifest.get("scope")
        if not isinstance(scope, Mapping):
            scope = {
                "kind": manifest.get("scope_kind"),
                "slug": manifest.get("scope_slug"),
            }
        report_day = _day(manifest.get("report_day"))
        report_type = _report_type(str(manifest.get("report_type") or ""))
        key = (
            str(scope.get("kind") or ""),
            str(scope.get("slug") or ""),
            report_type,
            str(report_day or ""),
        )
        if not all(key):
            raise SilverValidationError(
                "Bronze manifest snapshot identity is incomplete"
            )
        current = winners.get(key)
        rank = (
            str(manifest.get("ingested_at") or ""),
            str(manifest.get("ingestion_id") or ""),
        )
        current_rank = (
            str(current.get("ingested_at") or ""),
            str(current.get("ingestion_id") or ""),
        ) if current else ("", "")
        if current is None or rank > current_rank:
            winners[key] = manifest
    return [winners[key] for key in sorted(winners)]


def _latest_snapshots(
    snapshots: Iterable[Snapshot],
) -> dict[tuple[str, str, str, str], Snapshot]:
    winners: dict[tuple[str, str, str, str], Snapshot] = {}
    for snapshot in snapshots:
        current = winners.get(snapshot.key)
        if current is None or snapshot.rank > current.rank:
            winners[snapshot.key] = snapshot
    return winners


def _row_is_in_latest_snapshot(
    row: Mapping[str, Any],
    snapshots: Mapping[tuple[str, str, str, str], Snapshot],
) -> bool:
    report_type = row.get("source_report_type") or row.get("report_type")
    report_day = row.get("source_report_day") or row.get("day")
    if not report_type or not report_day:
        return True
    key = (
        str(row.get("scope_kind") or ""),
        str(row.get("scope_slug") or ""),
        str(report_type),
        str(report_day),
    )
    snapshot = snapshots.get(key)
    if snapshot is None:
        return True
    return (
        snapshot.status == "success"
        and row.get("source_ingestion_id") == snapshot.ingestion_id
    )


def _report_type(value: str) -> str:
    aliases = {
        "entity": "entity",
        "users": "users",
        "user": "users",
        "user-teams": "user-teams",
        "user_teams": "user-teams",
        "repositories": "repositories",
        "repository": "repositories",
        "repos": "repositories",
    }
    return aliases.get(str(value), str(value))


def _day(value: Any) -> str | None:
    if isinstance(value, datetime):
        return None
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        return None


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(record: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(record).encode()).hexdigest()
