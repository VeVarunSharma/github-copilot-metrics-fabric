# Gold analytics data dictionary

The Gold layer turns the explicit Silver contracts into stable business
tables. `copilot_metrics_fabric.gold` is pure Python and can be tested without
Spark. `copilot_metrics_fabric.gold_spark` lazily provides explicit Spark
schemas and Delta merge support for Fabric notebooks.

## Aggregation rules

- Counts and volumes are additive unless a table says otherwise.
- Every rate is `SUM(numerator) / SUM(denominator)`, never an average of row
  percentages. A zero or absent denominator produces `null`, not zero.
- Gold-derived 7- and 28-day active users are distinct users observed anywhere
  in the inclusive window.
- GitHub's source-reported weekly/monthly active-user values are snapshots.
  They are preserved on their report day and are never summed.
- Missing optional metrics remain `null`; missing is not silently converted to
  zero. Dimension rows are emitted only when that dimension was observed.
- Duplicate Silver upsert keys are treated as late corrections. The greatest
  `(source_ingested_at, source_ingestion_id, source_path, source_record_hash)`
  wins, and `source_correction_count` records replaced rows.

Activity means at least one usage flag or engagement metric is positive.
Engagement means an interaction, generation, acceptance, or accepted line
metric is positive.

## Output tables

| Table | Grain | Purpose |
| --- | --- | --- |
| `entity_adoption_daily` | scope + day | Daily observed, active, engaged, volume, acceptance, and source snapshot metrics |
| `team_adoption_daily` | scope + day + team + allocation | Team adoption with explicit additivity semantics |
| `repository_copilot_impact_daily` | scope + day + repository | Copilot-authored/reviewed PR and suggestion impact |
| `feature_usage_daily` | scope + day + feature | Feature users, activity, and acceptance |
| `language_usage_daily` | scope + day + language | Language usage aggregated across feature rows |
| `ide_usage_daily` | scope + day + IDE | IDE usage and acceptance |
| `user_adoption_daily` | scope + day + user | Daily user status and activity |
| `user_adoption_current` | scope + user | Current state rebuilt from all available Silver user history through the scope's latest reporting day |
| `adoption_rolling_daily` | scope + day + dimension + window | Derived 7/28-day distinct active users |
| `data_freshness_daily` | scope + day + report type | Data/no-data/missing/lag state, corrections, and sparsity |

All daily tables include `day_key`, `gold_built_at`, and
`source_correction_count`. The machine-readable schema is available from
`copilot_metrics_fabric.gold.contracts()`.

## Complete column contract

Types use Spark names. Except `user_adoption_current`, every table begins with
`scope_kind` string **required**, `scope_slug` string **required**, `day` date
**required**, and `day_key` long **required**, and ends with `gold_built_at`
string **required** and `source_correction_count` long **required**.

The reusable adoption set is:

| Column | Type | Meaning |
| --- | --- | --- |
| `observed_users` | long, required | Distinct user IDs present at the grain. |
| `active_users` | long, required | Distinct users engaged or with an implemented `used_*` flag. |
| `engaged_users` | long, required | Distinct users with positive interaction/generation/acceptance/added/deleted-line activity. |
| `interaction_count` | long | Sum of user-initiated interactions. |
| `generation_count` | long | Sum of code-generation activities. |
| `acceptance_count` | long | Sum of code-acceptance activities. |
| `acceptance_rate` | double | Acceptance count divided by generation count. |
| `loc_suggested` | long | Sum of lines suggested to add. |
| `loc_accepted` | long | Sum of lines added. |
| `loc_acceptance_rate` | double | Lines accepted divided by lines suggested. |
| `ai_credits_used` | double | Sum of source AI credits. |

| Table | Complete table-specific columns | Upsert key |
| --- | --- | --- |
| `entity_adoption_daily` | Adoption set; `source_reported_daily_active_users`, `source_reported_weekly_active_users`, `source_reported_monthly_active_users` long | scope, day |
| `team_adoption_daily` | `team_id` long **required**, `team_slug` string; Adoption set; `membership_count` long **required**, `allocation_method` string **required**, `is_additive` boolean **required** | scope, day, team, allocation |
| `repository_copilot_impact_daily` | `repo_id` long **required**; `repo_owner_name`, `repo_name`, `repo_visibility` string; `pull_requests_created`, `pull_requests_created_by_copilot`, `pull_requests_reviewed`, `pull_requests_reviewed_by_copilot`, `suggestions`, `applied_suggestions`, `pull_requests_merged` long; `copilot_authored_pr_rate`, `copilot_reviewed_pr_rate`, `suggestion_apply_rate`, `median_minutes_to_merge` double | scope, day, repository |
| `feature_usage_daily` | `feature` string **required**; Adoption set | scope, day, feature |
| `language_usage_daily` | `language` string **required**; Adoption set | scope, day, language |
| `ide_usage_daily` | `ide` string **required**; Adoption set | scope, day, IDE |
| `user_adoption_daily` | `user_id` long **required**, `user_login` string, `adoption_status` string **required**, `adoption_phase` string; Adoption set excluding observed/active/engaged user counts | scope, day, user |
| `user_adoption_current` | `scope_kind`, `scope_slug` string **required**; `user_id` long **required**; `user_login` string; `as_of_day` date **required**, `as_of_day_key` long **required**; `adoption_status` string **required**, `adoption_phase` string; `days_since_activity` long; `is_stale` boolean **required**; build/correction columns | scope, user |
| `adoption_rolling_daily` | `dimension_type`, `dimension_id` string **required**, `dimension_name` string; `window_days`, `distinct_active_users` long **required**; `allocation_method` string **required**, `is_additive` boolean **required** | scope, day, dimension type/ID, window |
| `data_freshness_daily` | `report_type`, `availability_status` string **required**; `has_data`, `is_no_data`, `is_complete`, `is_within_telemetry_lag` boolean **required**; `days_late` long **required**; `latest_source_ingested_at` string; `sparse_metric_count` long **required** | scope, day, report type |

### Repository rates

- `copilot_authored_pr_rate = pull_requests_created_by_copilot /
  pull_requests_created`.
- `copilot_reviewed_pr_rate = pull_requests_reviewed_by_copilot /
  pull_requests_reviewed`.
- `suggestion_apply_rate = applied_suggestions / suggestions`.

All use null for a zero/absent denominator. `median_minutes_to_merge` is a
source repository-day median; it is not additive.

### User and freshness states

- `adoption_status` is `engaged`, `active`, or `inactive` using the definitions
  above.
- For `user_adoption_current`, `as_of_day` is the latest reporting day
  available in Silver for that scope, not the end of a requested backfill.
- `days_since_activity` uses all available Silver user history and is null only
  when the user has no active day in that history.
- `is_stale` is true when the user's latest row is older than the scope's
  `as_of_day` by more than `telemetry_lag_days`.
- `availability_status` is `complete`, `no_data`, `within_lag`, or `missing`.
- `has_data` means rows were observed; `is_no_data` means GitHub explicitly
  returned no data; both `complete` and `no_data` set `is_complete=true`.
- Scopes represented only by explicit `no_data` manifests still receive
  freshness rows.
- `sparse_metric_count` counts null optional values observed for that
  scope/day/report type; it is not a count of missing rows.

## Team additivity

GitHub team membership is many-to-many. The default `all_memberships` rows
preserve that relationship and set `is_additive=false`; summing teams can
double-count users. Callers may pass `primary_team_mapping` as either
`user_id -> team_id` or `(scope_kind, scope_slug, user_id) -> team_id`.
Additional `primary_team` rows then set `is_additive=true` and are suitable for
executive rollups. The original many-to-many rows are always retained.

## Bounded replacement and backfills

Daily Gold tables replace only the requested scope/day windows. Rolling
calculations still read the preceding 27 days, so the first requested day has a
complete inclusive 28-day history (and therefore enough history for 7-day
metrics), but those lookback-only rows are never emitted or merged. The current
user table has no day grain, so the notebook separately reads the complete
`silver.user_daily` history for the requested scope and atomically replaces
that scope's current rows. A historical-only window therefore cannot roll
current state backward or remove users first observed after the backfill.

Existing Gold tables are changed with one Delta `MERGE` transaction per table.
The merge updates/inserts source rows and deletes target rows absent from the
bounded source via `whenNotMatchedBySourceDelete`; no delete-then-append gap is
used. Overlapping windows for the same scope are coalesced, predicates are
escaped, dates and scope bounds are validated, and source rows outside the
replacement predicate fail before execution.

## Freshness and completeness

`GoldBuildOptions.telemetry_lag_days` separates expected telemetry lag from
true missing data. Calendar days without an observed report are:

- `within_lag` while inside the lag allowance;
- `missing` after the allowance;
- `no_data` only when an explicit manifest status says GitHub returned no data.

Both `complete` and `no_data` are complete collection outcomes. Optional
`report_status_rows` bridge Bronze/Silver manifests into Gold and contain
`scope_kind`, `scope_slug`, `day`, `report_type`, `status`, and optionally
`ingestion_id` and `source_ingested_at`. Gold keeps only the latest status for
each snapshot. `expected_report_types` is derived from the pipeline's configured
`report_types` subset, so intentionally excluded reports are not classified as
missing. Manifest discovery uses the configured `bronze_folder`.
`sparse_metric_count` exposes null fields without inventing values.

## Fabric integration

`fabric/notebooks/build_gold.ipynb` demonstrates reading Silver tables and the
latest Bronze manifests, creating schema-bound DataFrames,
and Delta-merging corrected days. It is intentionally an integration contract,
not a pipeline, semantic model, report, or deployment definition.
