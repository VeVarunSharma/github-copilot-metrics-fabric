"""Pure-Python Gold analytics built from the explicit Silver contracts."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from copilot_metrics_fabric.silver import Column, DataType, TableContract


class GoldValidationError(ValueError):
    """Raised when a Gold build request is invalid."""


GOLD_REPORT_TYPES = ("entity", "users", "user-teams", "repositories")


def normalize_report_types(value: str | Iterable[str]) -> tuple[str, ...]:
    """Normalize a configured report subset for Gold freshness checks."""

    candidates = value.split(",") if isinstance(value, str) else value
    normalized = tuple(
        dict.fromkeys(
            str(candidate).strip()
            for candidate in candidates
            if str(candidate).strip()
        )
    )
    unsupported = sorted(set(normalized) - set(GOLD_REPORT_TYPES))
    if unsupported:
        raise GoldValidationError(
            "unsupported expected report types: " + ", ".join(unsupported)
        )
    if not normalized:
        raise GoldValidationError("expected_report_types must not be empty")
    return normalized


@dataclass(frozen=True, slots=True)
class GoldBuildOptions:
    """Calendar and freshness settings for a deterministic Gold build."""

    as_of_day: str | date | None = None
    calendar_start: str | date | None = None
    calendar_end: str | date | None = None
    telemetry_lag_days: int = 2
    expected_report_types: tuple[str, ...] = GOLD_REPORT_TYPES
    built_at: str | None = None

    def __post_init__(self) -> None:
        if self.telemetry_lag_days < 0:
            raise GoldValidationError("telemetry_lag_days must be non-negative")
        object.__setattr__(
            self,
            "expected_report_types",
            normalize_report_types(self.expected_report_types),
        )
        if (
            self.calendar_start is not None
            and self.calendar_end is not None
            and _day(self.calendar_start) > _day(self.calendar_end)
        ):
            raise GoldValidationError("calendar_start must not follow calendar_end")


@dataclass(frozen=True, slots=True)
class GoldReplacementWindow:
    """Bounded scope/day range whose Gold outputs are fully recomputed."""

    scope_kind: str
    scope_slug: str
    start_day: str | date
    end_day: str | date

    def __post_init__(self) -> None:
        if self.scope_kind not in {"enterprise", "organization"}:
            raise GoldValidationError(
                "replacement scope_kind must be 'enterprise' or 'organization'"
            )
        if not self.scope_slug.strip():
            raise GoldValidationError("replacement scope_slug must not be empty")
        if _day(self.start_day) > _day(self.end_day):
            raise GoldValidationError("replacement start_day must not follow end_day")


@dataclass(slots=True)
class GoldBatch:
    rows: dict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add(self, table: str, values: Mapping[str, Any]) -> None:
        self.rows[table].append(GOLD_CONTRACTS[table].row(values))

    def finalize(self) -> GoldBatch:
        for name in GOLD_CONTRACTS:
            self.rows[name] = sorted(
                self.rows.get(name, []),
                key=lambda row: tuple(
                    _sortable(row[column])
                    for column in GOLD_CONTRACTS[name].upsert_key
                ),
            )
        return self


BASE = (
    Column("scope_kind", DataType.STRING, False),
    Column("scope_slug", DataType.STRING, False),
    Column("day", DataType.DATE, False),
    Column("day_key", DataType.INTEGER, False),
)
BUILD = (
    Column("gold_built_at", DataType.STRING, False),
    Column("source_correction_count", DataType.INTEGER, False),
)
ADOPTION = (
    Column("observed_users", DataType.INTEGER, False),
    Column("active_users", DataType.INTEGER, False),
    Column("engaged_users", DataType.INTEGER, False),
    Column("interaction_count", DataType.INTEGER),
    Column("generation_count", DataType.INTEGER),
    Column("acceptance_count", DataType.INTEGER),
    Column("acceptance_rate", DataType.NUMBER),
    Column("loc_suggested", DataType.INTEGER),
    Column("loc_accepted", DataType.INTEGER),
    Column("loc_acceptance_rate", DataType.NUMBER),
    Column("ai_credits_used", DataType.NUMBER),
)


def _contract(
    name: str, columns: tuple[Column, ...], key: tuple[str, ...]
) -> TableContract:
    return TableContract(name, BASE + columns + BUILD, key)


SCOPE_DAY = ("scope_kind", "scope_slug", "day")
GOLD_CONTRACTS: dict[str, TableContract] = {
    "entity_adoption_daily": _contract(
        "entity_adoption_daily",
        ADOPTION
        + (
            Column("source_reported_daily_active_users", DataType.INTEGER),
            Column("source_reported_weekly_active_users", DataType.INTEGER),
            Column("source_reported_monthly_active_users", DataType.INTEGER),
        ),
        SCOPE_DAY,
    ),
    "team_adoption_daily": _contract(
        "team_adoption_daily",
        (
            Column("team_id", DataType.INTEGER, False),
            Column("team_slug", DataType.STRING),
        )
        + ADOPTION
        + (
            Column("membership_count", DataType.INTEGER, False),
            Column("allocation_method", DataType.STRING, False),
            Column("is_additive", DataType.BOOLEAN, False),
        ),
        SCOPE_DAY + ("team_id", "allocation_method"),
    ),
    "repository_copilot_impact_daily": _contract(
        "repository_copilot_impact_daily",
        (
            Column("repo_id", DataType.INTEGER, False),
            Column("repo_owner_name", DataType.STRING),
            Column("repo_name", DataType.STRING),
            Column("repo_visibility", DataType.STRING),
            Column("pull_requests_created", DataType.INTEGER),
            Column("pull_requests_created_by_copilot", DataType.INTEGER),
            Column("copilot_authored_pr_rate", DataType.NUMBER),
            Column("pull_requests_reviewed", DataType.INTEGER),
            Column("pull_requests_reviewed_by_copilot", DataType.INTEGER),
            Column("copilot_reviewed_pr_rate", DataType.NUMBER),
            Column("suggestions", DataType.INTEGER),
            Column("applied_suggestions", DataType.INTEGER),
            Column("suggestion_apply_rate", DataType.NUMBER),
            Column("pull_requests_merged", DataType.INTEGER),
            Column("median_minutes_to_merge", DataType.NUMBER),
        ),
        SCOPE_DAY + ("repo_id",),
    ),
    "feature_usage_daily": _contract(
        "feature_usage_daily",
        (Column("feature", DataType.STRING, False),) + ADOPTION,
        SCOPE_DAY + ("feature",),
    ),
    "language_usage_daily": _contract(
        "language_usage_daily",
        (Column("language", DataType.STRING, False),) + ADOPTION,
        SCOPE_DAY + ("language",),
    ),
    "ide_usage_daily": _contract(
        "ide_usage_daily",
        (Column("ide", DataType.STRING, False),) + ADOPTION,
        SCOPE_DAY + ("ide",),
    ),
    "user_adoption_daily": _contract(
        "user_adoption_daily",
        (
            Column("user_id", DataType.INTEGER, False),
            Column("user_login", DataType.STRING),
            Column("adoption_status", DataType.STRING, False),
            Column("adoption_phase", DataType.STRING),
        )
        + ADOPTION[3:],
        SCOPE_DAY + ("user_id",),
    ),
    "user_adoption_current": TableContract(
        "user_adoption_current",
        (
            Column("scope_kind", DataType.STRING, False),
            Column("scope_slug", DataType.STRING, False),
            Column("user_id", DataType.INTEGER, False),
            Column("user_login", DataType.STRING),
            Column("as_of_day", DataType.DATE, False),
            Column("as_of_day_key", DataType.INTEGER, False),
            Column("adoption_status", DataType.STRING, False),
            Column("adoption_phase", DataType.STRING),
            Column("days_since_activity", DataType.INTEGER),
            Column("is_stale", DataType.BOOLEAN, False),
        )
        + BUILD,
        ("scope_kind", "scope_slug", "user_id"),
    ),
    "adoption_rolling_daily": _contract(
        "adoption_rolling_daily",
        (
            Column("dimension_type", DataType.STRING, False),
            Column("dimension_id", DataType.STRING, False),
            Column("dimension_name", DataType.STRING),
            Column("window_days", DataType.INTEGER, False),
            Column("distinct_active_users", DataType.INTEGER, False),
            Column("allocation_method", DataType.STRING, False),
            Column("is_additive", DataType.BOOLEAN, False),
        ),
        SCOPE_DAY + ("dimension_type", "dimension_id", "window_days"),
    ),
    "data_freshness_daily": _contract(
        "data_freshness_daily",
        (
            Column("report_type", DataType.STRING, False),
            Column("availability_status", DataType.STRING, False),
            Column("has_data", DataType.BOOLEAN, False),
            Column("is_no_data", DataType.BOOLEAN, False),
            Column("is_complete", DataType.BOOLEAN, False),
            Column("is_within_telemetry_lag", DataType.BOOLEAN, False),
            Column("days_late", DataType.INTEGER, False),
            Column("latest_source_ingested_at", DataType.STRING),
            Column("sparse_metric_count", DataType.INTEGER, False),
        ),
        SCOPE_DAY + ("report_type",),
    ),
}


def contracts() -> dict[str, dict[str, Any]]:
    """Return JSON-compatible Gold table contracts."""

    return {name: contract.as_dict() for name, contract in GOLD_CONTRACTS.items()}


def build_gold(
    silver_rows: Mapping[str, Iterable[Mapping[str, Any]]],
    *,
    options: GoldBuildOptions | None = None,
    report_status_rows: Iterable[Mapping[str, Any]] = (),
    primary_team_mapping: Mapping[Any, Any] | None = None,
    current_user_rows: Iterable[Mapping[str, Any]] | None = None,
) -> GoldBatch:
    """Build all Gold outputs without importing Spark.

    ``report_status_rows`` is the optional Bronze/Silver manifest integration
    hook. Rows use scope_kind, scope_slug, day, report_type, and status
    (``complete`` or ``no_data``), plus optional source_ingested_at.

    ``current_user_rows`` must contain complete available Silver user history
    for every scope being replaced. When omitted, ``silver_rows["user_daily"]``
    is used, which is appropriate only when that input is already unbounded.
    """

    options = options or GoldBuildOptions()
    built_at = options.built_at or datetime.now(timezone.utc).isoformat()
    latest_status_rows = _latest_report_status_rows(report_status_rows)
    tables, corrections = _latest_silver_rows(silver_rows, latest_status_rows)
    if current_user_rows is None:
        current_users = tables["user_daily"]
        current_corrections = corrections
    else:
        current_tables, current_corrections = _latest_silver_rows(
            {"user_daily": current_user_rows}
        )
        current_users = current_tables["user_daily"]
    batch = GoldBatch()

    users = tables["user_daily"]
    entity = tables["entity_daily"]
    memberships = tables["user_team_daily"]

    user_by_scope_day: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in users:
        user_by_scope_day[_scope_day(row)].append(row)

    entity_by_scope_day = {_scope_day(row): row for row in entity}
    all_scope_days = set(user_by_scope_day) | set(entity_by_scope_day)
    for key in sorted(all_scope_days, key=repr):
        day_users = user_by_scope_day.get(key, [])
        metrics = _adoption_metrics(day_users)
        reported = entity_by_scope_day.get(key, {})
        if not day_users:
            metrics["active_users"] = _integer(reported.get("daily_active_users"))
            metrics.update(_activity_metrics([reported]))
        batch.add(
            "entity_adoption_daily",
            {
                **_base_values(key, built_at, corrections),
                **metrics,
                "source_reported_daily_active_users": reported.get(
                    "daily_active_users"
                ),
                "source_reported_weekly_active_users": reported.get(
                    "weekly_active_users"
                ),
                "source_reported_monthly_active_users": reported.get(
                    "monthly_active_users"
                ),
            },
        )

    membership_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for membership in memberships:
        key = _scope_day(membership) + (membership["team_id"],)
        membership_groups[key].append(membership)
    users_by_key = {
        _scope_day(row) + (row["user_id"],): row
        for row in users
    }
    _build_team_rows(
        batch,
        membership_groups,
        users_by_key,
        built_at,
        corrections,
        allocation_method="all_memberships",
    )

    primary_memberships: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    if primary_team_mapping:
        for membership in memberships:
            mapping_key = (
                membership["scope_kind"],
                membership["scope_slug"],
                membership["user_id"],
            )
            primary = primary_team_mapping.get(mapping_key)
            if primary is None:
                primary = primary_team_mapping.get(membership["user_id"])
            if primary == membership["team_id"]:
                key = _scope_day(membership) + (membership["team_id"],)
                primary_memberships[key].append(membership)
        _build_team_rows(
            batch,
            primary_memberships,
            users_by_key,
            built_at,
            corrections,
            allocation_method="primary_team",
        )

    _build_repositories(batch, tables["repository_daily"], built_at, corrections)
    _build_breakdown(
        batch,
        "feature_usage_daily",
        tables["feature_daily"],
        ("feature",),
        built_at,
        corrections,
    )
    _build_breakdown(
        batch,
        "language_usage_daily",
        tables["language_feature_daily"],
        ("language",),
        built_at,
        corrections,
    )
    _build_breakdown(
        batch,
        "ide_usage_daily",
        tables["ide_daily"],
        ("ide",),
        built_at,
        corrections,
    )
    _build_users(
        batch,
        users,
        current_users,
        options,
        built_at,
        corrections,
        current_corrections,
    )
    _build_rolling(
        batch,
        users,
        memberships,
        primary_team_mapping,
        options,
        built_at,
        corrections,
    )
    _build_freshness(
        batch,
        tables,
        latest_status_rows,
        options,
        built_at,
        corrections,
    )
    _limit_daily_outputs(batch, options)
    return batch.finalize()


def _limit_daily_outputs(batch: GoldBatch, options: GoldBuildOptions) -> None:
    """Keep lookback inputs available for calculations but not Gold writes."""

    start = _day(options.calendar_start) if options.calendar_start else None
    end = _day(options.calendar_end) if options.calendar_end else None
    if start is None and end is None:
        return
    for name, contract in GOLD_CONTRACTS.items():
        if not any(column.name == "day" for column in contract.columns):
            continue
        batch.rows[name] = [
            row
            for row in batch.rows.get(name, [])
            if (start is None or _day(row["day"]) >= start)
            and (end is None or _day(row["day"]) <= end)
        ]


def _latest_silver_rows(
    silver_rows: Mapping[str, Iterable[Mapping[str, Any]]],
    report_status_rows: Iterable[Mapping[str, Any]] = (),
) -> tuple[dict[str, list[dict[str, Any]]], dict[tuple[Any, ...], int]]:
    from copilot_metrics_fabric.silver import CONTRACTS

    snapshots = {
        (
            row["scope_kind"],
            row["scope_slug"],
            row["report_type"],
            _day(row.get("report_day", row.get("day"))).isoformat(),
        ): row
        for row in report_status_rows
    }
    output: dict[str, list[dict[str, Any]]] = {}
    corrections: dict[tuple[Any, ...], int] = defaultdict(int)
    for name, contract in CONTRACTS.items():
        winners: dict[tuple[Any, ...], dict[str, Any]] = {}
        counts: dict[tuple[Any, ...], int] = defaultdict(int)
        for source in silver_rows.get(name, ()):
            row = dict(source)
            snapshot_key = _silver_snapshot_key(row)
            snapshot = snapshots.get(snapshot_key) if snapshot_key else None
            if snapshot and (
                snapshot["status"] == "no_data"
                or (
                    snapshot.get("ingestion_id")
                    and row.get("source_ingestion_id") != snapshot["ingestion_id"]
                )
            ):
                continue
            key = tuple(row.get(column) for column in contract.upsert_key)
            counts[key] += 1
            if key not in winners or _source_rank(row) > _source_rank(winners[key]):
                winners[key] = row
        output[name] = list(winners.values())
        for key, count in counts.items():
            if count > 1:
                row = winners[key]
                corrections[_scope_day(row)] += count - 1
    return output, corrections


def _latest_report_status_rows(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    winners: dict[tuple[Any, ...], dict[str, Any]] = {}
    for source in rows:
        row = dict(source)
        row["day"] = _day(row.get("day", row.get("report_day"))).isoformat()
        status = row.get("status")
        if status == "success":
            status = "complete"
        if status not in {"complete", "no_data"}:
            continue
        row["status"] = status
        key = (
            row["scope_kind"],
            row["scope_slug"],
            row["day"],
            row["report_type"],
        )
        current = winners.get(key)
        if current is None or _status_rank(row) > _status_rank(current):
            winners[key] = row
    return [winners[key] for key in sorted(winners, key=repr)]


def _silver_snapshot_key(row: Mapping[str, Any]) -> tuple[Any, ...] | None:
    report_type = row.get("source_report_type")
    report_day = row.get("source_report_day")
    if not report_type or not report_day:
        return None
    return (
        row.get("scope_kind"),
        row.get("scope_slug"),
        report_type,
        _day(report_day).isoformat(),
    )


def _status_rank(row: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("source_ingested_at", row.get("ingested_at")) or ""),
        str(row.get("ingestion_id") or ""),
    )


def _build_team_rows(
    batch: GoldBatch,
    groups: Mapping[tuple[Any, ...], list[dict[str, Any]]],
    users_by_key: Mapping[tuple[Any, ...], dict[str, Any]],
    built_at: str,
    corrections: Mapping[tuple[Any, ...], int],
    *,
    allocation_method: str,
) -> None:
    for key, membership_rows in groups.items():
        scope_day = key[:3]
        team_users = [
            users_by_key[scope_day + (row["user_id"],)]
            for row in membership_rows
            if scope_day + (row["user_id"],) in users_by_key
        ]
        batch.add(
            "team_adoption_daily",
            {
                **_base_values(scope_day, built_at, corrections),
                "team_id": key[3],
                "team_slug": next(
                    (
                        row.get("team_slug")
                        for row in membership_rows
                        if row.get("team_slug")
                    ),
                    None,
                ),
                **_adoption_metrics(team_users),
                "membership_count": len({row["user_id"] for row in membership_rows}),
                "allocation_method": allocation_method,
                "is_additive": allocation_method == "primary_team",
            },
        )


def _build_repositories(
    batch: GoldBatch,
    rows: list[dict[str, Any]],
    built_at: str,
    corrections: Mapping[tuple[Any, ...], int],
) -> None:
    for row in rows:
        key = _scope_day(row)
        batch.add(
            "repository_copilot_impact_daily",
            {
                **_base_values(key, built_at, corrections),
                "repo_id": row["repo_id"],
                "repo_owner_name": row.get("repo_owner_name"),
                "repo_name": row.get("repo_name"),
                "repo_visibility": row.get("repo_visibility"),
                "pull_requests_created": row.get("pr_total_created"),
                "pull_requests_created_by_copilot": row.get(
                    "pr_total_created_by_copilot"
                ),
                "copilot_authored_pr_rate": _ratio(
                    row.get("pr_total_created_by_copilot"),
                    row.get("pr_total_created"),
                ),
                "pull_requests_reviewed": row.get("pr_total_reviewed"),
                "pull_requests_reviewed_by_copilot": row.get(
                    "pr_total_reviewed_by_copilot"
                ),
                "copilot_reviewed_pr_rate": _ratio(
                    row.get("pr_total_reviewed_by_copilot"),
                    row.get("pr_total_reviewed"),
                ),
                "suggestions": row.get("pr_total_copilot_suggestions"),
                "applied_suggestions": row.get(
                    "pr_total_copilot_applied_suggestions"
                ),
                "suggestion_apply_rate": _ratio(
                    row.get("pr_total_copilot_applied_suggestions"),
                    row.get("pr_total_copilot_suggestions"),
                ),
                "pull_requests_merged": row.get("pr_total_merged"),
                "median_minutes_to_merge": row.get("pr_median_minutes_to_merge"),
            },
        )


def _build_breakdown(
    batch: GoldBatch,
    target: str,
    rows: list[dict[str, Any]],
    dimensions: tuple[str, ...],
    built_at: str,
    corrections: Mapping[tuple[Any, ...], int],
) -> None:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[_scope_day(row) + tuple(row.get(dim) for dim in dimensions)].append(
            row
        )
    for key, group in groups.items():
        scope_day = key[:3]
        values = {
            **_base_values(scope_day, built_at, corrections),
            **dict(zip(dimensions, key[3:], strict=True)),
            **_adoption_metrics(group),
        }
        batch.add(target, values)


def _build_users(
    batch: GoldBatch,
    daily_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
    options: GoldBuildOptions,
    built_at: str,
    daily_corrections: Mapping[tuple[Any, ...], int],
    current_corrections: Mapping[tuple[Any, ...], int],
) -> None:
    for row in daily_rows:
        batch.add(
            "user_adoption_daily",
            {
                **_base_values(_scope_day(row), built_at, daily_corrections),
                "user_id": row["user_id"],
                "user_login": row.get("user_login"),
                "adoption_status": _user_status(row),
                "adoption_phase": row.get("adoption_phase"),
                **_activity_metrics([row]),
            },
        )

    latest: dict[tuple[Any, ...], dict[str, Any]] = {}
    last_active: dict[tuple[Any, ...], date] = {}
    scope_horizons: dict[tuple[str, str], date] = {}
    for row in sorted(current_rows, key=lambda item: _day(item["day"])):
        key = (row["scope_kind"], row["scope_slug"], row["user_id"])
        scope = (key[0], key[1])
        row_day = _day(row["day"])
        latest[key] = row
        if _is_active(row):
            last_active[key] = row_day
        scope_horizons[scope] = max(scope_horizons.get(scope, row_day), row_day)

    for key, row in latest.items():
        row_day = _day(row["day"])
        effective_as_of = scope_horizons[(key[0], key[1])]
        days_since = (
            (effective_as_of - last_active[key]).days if key in last_active else None
        )
        batch.add(
            "user_adoption_current",
            {
                "scope_kind": key[0],
                "scope_slug": key[1],
                "user_id": key[2],
                "user_login": row.get("user_login"),
                "as_of_day": effective_as_of.isoformat(),
                "as_of_day_key": _day_key(effective_as_of),
                "adoption_status": _user_status(row),
                "adoption_phase": row.get("adoption_phase"),
                "days_since_activity": days_since,
                "is_stale": (effective_as_of - row_day).days
                > options.telemetry_lag_days,
                "gold_built_at": built_at,
                "source_correction_count": current_corrections.get(
                    _scope_day(row), 0
                ),
            },
        )


def _build_rolling(
    batch: GoldBatch,
    users: list[dict[str, Any]],
    memberships: list[dict[str, Any]],
    primary_mapping: Mapping[Any, Any] | None,
    options: GoldBuildOptions,
    built_at: str,
    corrections: Mapping[tuple[Any, ...], int],
) -> None:
    active = [row for row in users if _is_active(row)]
    scopes = {(row["scope_kind"], row["scope_slug"]) for row in users}
    days = sorted({_day(row["day"]) for row in users})
    membership_lookup: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in memberships:
        membership_lookup[_scope_day(row) + (row["user_id"],)].append(row)

    for scope in sorted(scopes):
        scope_days = [day for day in days if any(
            row["scope_kind"] == scope[0]
            and row["scope_slug"] == scope[1]
            and _day(row["day"]) == day
            for row in users
        )]
        for end in scope_days:
            for window in (7, 28):
                start = end - timedelta(days=window - 1)
                window_rows = [
                    row
                    for row in active
                    if (row["scope_kind"], row["scope_slug"]) == scope
                    and start <= _day(row["day"]) <= end
                ]
                entity_users = {row["user_id"] for row in window_rows}
                scope_day = (scope[0], scope[1], end.isoformat())
                batch.add(
                    "adoption_rolling_daily",
                    {
                        **_base_values(scope_day, built_at, corrections),
                        "dimension_type": "entity",
                        "dimension_id": scope[1],
                        "dimension_name": scope[1],
                        "window_days": window,
                        "distinct_active_users": len(entity_users),
                        "allocation_method": "entity",
                        "is_additive": True,
                    },
                )
                teams: dict[Any, dict[str, Any]] = {}
                primary_teams: dict[Any, dict[str, Any]] = {}
                for row in window_rows:
                    row_day_key = _scope_day(row) + (row["user_id"],)
                    for membership in membership_lookup.get(row_day_key, []):
                        target = teams.setdefault(
                            membership["team_id"],
                            {"users": set(), "name": membership.get("team_slug")},
                        )
                        target["users"].add(row["user_id"])
                        if primary_mapping and _is_primary(membership, primary_mapping):
                            primary_target = primary_teams.setdefault(
                                membership["team_id"],
                                {
                                    "users": set(),
                                    "name": membership.get("team_slug"),
                                },
                            )
                            primary_target["users"].add(row["user_id"])
                _add_rolling_teams(
                    batch,
                    scope_day,
                    teams,
                    window,
                    built_at,
                    corrections,
                    "team",
                    "all_memberships",
                    False,
                )
                _add_rolling_teams(
                    batch,
                    scope_day,
                    primary_teams,
                    window,
                    built_at,
                    corrections,
                    "primary_team",
                    "primary_team",
                    True,
                )


def _add_rolling_teams(
    batch: GoldBatch,
    scope_day: tuple[Any, ...],
    teams: Mapping[Any, dict[str, Any]],
    window: int,
    built_at: str,
    corrections: Mapping[tuple[Any, ...], int],
    dimension_type: str,
    allocation_method: str,
    additive: bool,
) -> None:
    for team_id, value in teams.items():
        batch.add(
            "adoption_rolling_daily",
            {
                **_base_values(scope_day, built_at, corrections),
                "dimension_type": dimension_type,
                "dimension_id": str(team_id),
                "dimension_name": value["name"],
                "window_days": window,
                "distinct_active_users": len(value["users"]),
                "allocation_method": allocation_method,
                "is_additive": additive,
            },
        )


def _build_freshness(
    batch: GoldBatch,
    tables: Mapping[str, list[dict[str, Any]]],
    status_rows: Iterable[Mapping[str, Any]],
    options: GoldBuildOptions,
    built_at: str,
    corrections: Mapping[tuple[Any, ...], int],
) -> None:
    table_report = {
        "entity_daily": "entity",
        "user_daily": "users",
        "user_team_daily": "user-teams",
        "repository_daily": "repositories",
    }
    observed: dict[tuple[Any, ...], dict[str, Any]] = {}
    scopes: set[tuple[str, str]] = set()
    observed_days: list[date] = []
    for table, report_type in table_report.items():
        for row in tables[table]:
            key = _scope_day(row) + (report_type,)
            scopes.add((row["scope_kind"], row["scope_slug"]))
            observed_days.append(_day(row["day"]))
            current = observed.setdefault(key, {"status": "complete", "rows": []})
            current["rows"].append(row)
    for source in status_rows:
        key = (
            source["scope_kind"],
            source["scope_slug"],
            _day(source["day"]).isoformat(),
            source["report_type"],
        )
        scopes.add((key[0], key[1]))
        observed_days.append(_day(key[2]))
        current = observed.get(key, {"rows": []})
        observed[key] = {
            "status": source["status"],
            "rows": current["rows"] if source["status"] == "complete" else [],
            "source_ingested_at": source.get(
                "source_ingested_at", source.get("ingested_at")
            ),
        }
    if not scopes:
        return
    as_of = _day(options.as_of_day) if options.as_of_day else max(observed_days)
    start = (
        _day(options.calendar_start)
        if options.calendar_start
        else min(observed_days)
    )
    end = _day(options.calendar_end) if options.calendar_end else as_of
    for scope in sorted(scopes):
        current = start
        while current <= end:
            within_lag = (as_of - current).days <= options.telemetry_lag_days
            for report_type in options.expected_report_types:
                key = (scope[0], scope[1], current.isoformat(), report_type)
                entry = observed.get(key)
                status = entry["status"] if entry else (
                    "within_lag" if within_lag else "missing"
                )
                rows = entry.get("rows", []) if entry else []
                ingested = (
                    entry.get("source_ingested_at")
                    if entry
                    else None
                ) or _latest_ingested_at(rows)
                batch.add(
                    "data_freshness_daily",
                    {
                        **_base_values(key[:3], built_at, corrections),
                        "report_type": report_type,
                        "availability_status": status,
                        "has_data": bool(rows) or status == "complete",
                        "is_no_data": status == "no_data",
                        "is_complete": status in {"complete", "no_data"},
                        "is_within_telemetry_lag": within_lag,
                        "days_late": max(
                            0,
                            (as_of - current).days - options.telemetry_lag_days,
                        ),
                        "latest_source_ingested_at": ingested,
                        "sparse_metric_count": sum(
                            sum(value is None for value in row.values())
                            for row in rows
                        ),
                    },
                )
            current += timedelta(days=1)


def _adoption_metrics(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    materialized = list(rows)
    users = {
        row.get("user_id")
        for row in materialized
        if row.get("user_id") is not None
    }
    active_users = {
        row.get("user_id")
        for row in materialized
        if row.get("user_id") is not None and _is_active(row)
    }
    engaged_users = {
        row.get("user_id")
        for row in materialized
        if row.get("user_id") is not None and _is_engaged(row)
    }
    return {
        "observed_users": len(users),
        "active_users": len(active_users),
        "engaged_users": len(engaged_users),
        **_activity_metrics(materialized),
    }


def _activity_metrics(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    materialized = list(rows)
    interactions = _sum_present(materialized, "user_initiated_interaction_count")
    generations = _sum_present(materialized, "code_generation_activity_count")
    acceptances = _sum_present(materialized, "code_acceptance_activity_count")
    loc_suggested = _sum_present(materialized, "loc_suggested_to_add_sum")
    loc_accepted = _sum_present(materialized, "loc_added_sum")
    return {
        "interaction_count": interactions,
        "generation_count": generations,
        "acceptance_count": acceptances,
        "acceptance_rate": _ratio(acceptances, generations),
        "loc_suggested": loc_suggested,
        "loc_accepted": loc_accepted,
        "loc_acceptance_rate": _ratio(loc_accepted, loc_suggested),
        "ai_credits_used": _sum_present(materialized, "ai_credits_used"),
    }


def _user_status(row: Mapping[str, Any]) -> str:
    if _is_engaged(row):
        return "engaged"
    if _is_active(row):
        return "active"
    return "inactive"


def _is_active(row: Mapping[str, Any]) -> bool:
    return _is_engaged(row) or any(
        row.get(field) is True
        for field in (
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
    )


def _is_engaged(row: Mapping[str, Any]) -> bool:
    return any(
        (row.get(field) or 0) > 0
        for field in (
            "user_initiated_interaction_count",
            "code_generation_activity_count",
            "code_acceptance_activity_count",
            "loc_added_sum",
            "loc_deleted_sum",
        )
    )


def _is_primary(
    membership: Mapping[str, Any], mapping: Mapping[Any, Any]
) -> bool:
    scoped_key = (
        membership["scope_kind"],
        membership["scope_slug"],
        membership["user_id"],
    )
    expected = mapping.get(scoped_key, mapping.get(membership["user_id"]))
    return expected == membership["team_id"]


def _sum_present(rows: Iterable[Mapping[str, Any]], field: str) -> int | float | None:
    values = [row[field] for row in rows if row.get(field) is not None]
    return sum(values) if values else None


def _ratio(
    numerator: int | float | None, denominator: int | float | None
) -> float | None:
    if denominator in (None, 0):
        return None
    return float(numerator or 0) / float(denominator)


def _integer(value: Any) -> int:
    return int(value) if value is not None else 0


def _scope_day(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        row["scope_kind"],
        row["scope_slug"],
        _day(row["day"]).isoformat(),
    )


def _base_values(
    key: tuple[Any, ...],
    built_at: str,
    corrections: Mapping[tuple[Any, ...], int],
) -> dict[str, Any]:
    return {
        "scope_kind": key[0],
        "scope_slug": key[1],
        "day": _day(key[2]).isoformat(),
        "day_key": _day_key(_day(key[2])),
        "gold_built_at": built_at,
        "source_correction_count": corrections.get(
            (key[0], key[1], _day(key[2]).isoformat()), 0
        ),
    }


def _day(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _day_key(value: date) -> int:
    return value.year * 10000 + value.month * 100 + value.day


def _source_rank(row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(row.get(field) or "")
        for field in (
            "source_ingested_at",
            "source_ingestion_id",
            "source_path",
            "source_record_hash",
        )
    )


def _latest_ingested_at(rows: Iterable[Mapping[str, Any]]) -> str | None:
    values = [
        str(row["source_ingested_at"])
        for row in rows
        if row.get("source_ingested_at")
    ]
    return max(values) if values else None


def _sortable(value: Any) -> tuple[bool, str]:
    return value is None, str(value)
