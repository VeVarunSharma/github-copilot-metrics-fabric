# Silver normalization data dictionary

The Silver layer converts retained Bronze NDJSON into deterministic,
schema-bound relational tables. The core implementation is pure Python
(`copilot_metrics_fabric.silver`) so contracts and transformations can be unit
tested without Spark. `copilot_metrics_fabric.silver_spark` lazily adds
PySpark/Delta materialization for Fabric notebooks.

## Shared conventions

Every metric table contains:

| Column | Type | Meaning |
| --- | --- | --- |
| `scope_kind` | string | `enterprise` or `organization` |
| `scope_slug` | string | Stable configured scope slug |
| `day` | date | GitHub report day |
| `day_key` | long | `YYYYMMDD` integer used for deterministic joins |
| `enterprise_id` | string | GitHub enterprise ID, when supplied |
| `organization_id` | string | GitHub organization ID, when supplied |
| `source_report_type` | string | Bronze report type that produced the row |
| `source_report_day` | date | Requested Bronze snapshot day |
| `source_ingestion_id` | string | Bronze ingestion identifier |
| `source_ingested_at` | string | Bronze ingestion timestamp |
| `source_path` | string | Retained Bronze NDJSON path |
| `source_record_hash` | string | SHA-256 of canonical source JSON |

GitHub's numeric user, team, and repository identifiers remain 64-bit integers.
Enterprise and organization IDs remain strings because GitHub may serialize
them as strings and legacy/test sources may contain non-numeric identifiers.

Each scope/report-type/report-day ingestion is a complete snapshot. Silver
selects the latest `success` or `no_data` manifest, replaces only that bounded
snapshot, and therefore removes keys omitted by a correction. Within the
selected snapshot, duplicate upsert keys retain the row with the greatest
`(source_ingested_at, source_ingestion_id, source_path, source_record_hash)`.
A warning is written to `silver_quality_issues`.

## Core daily tables

| Table | Grain / upsert key | Contents |
| --- | --- | --- |
| `entity_daily` | scope + day | Organization/enterprise daily counters and active-user metrics |
| `user_daily` | scope + day + `user_id` | Per-user counters, usage flags, login, and flattened adoption phase |
| `user_team_daily` | scope + day + `user_id` + `team_id` | Organization or enterprise team membership |
| `repository_daily` | scope + day + `repo_id` | Repository identity plus flattened pull-request totals and medians |

The full machine-readable contract is returned by
`copilot_metrics_fabric.silver.contracts()`.

## Complete column contract

Types below use Spark names: `string`, `long`, `double`, `boolean`, and
`date`. Columns marked **required** are non-null in the contract; all others
are nullable. Every normal metric table begins with the shared columns and
ends with the lineage columns.

### Shared columns

| Group | Columns |
| --- | --- |
| Base | `scope_kind` string **required**; `scope_slug` string **required**; `day` date **required**; `day_key` long **required**; `enterprise_id` string; `organization_id` string |
| Lineage | `source_report_type` string **required**; `source_report_day` date **required**; `source_ingestion_id` string; `source_ingested_at` string; `source_path` string; `source_record_hash` string **required** |
| Usage counters | `ai_credits_used` double; `user_initiated_interaction_count`, `code_generation_activity_count`, `code_acceptance_activity_count`, `loc_suggested_to_add_sum`, `loc_suggested_to_delete_sum`, `loc_added_sum`, `loc_deleted_sum`, `distinct_custom_agent_use_count`, `distinct_mcp_use_count`, `distinct_plugin_use_count`, `distinct_skill_use_count`, `distinct_slash_cmd_use_count`, `daily_active_users`, `daily_active_cli_users`, `daily_active_copilot_app_users`, `daily_active_copilot_cloud_agent_users`, `daily_active_copilot_code_review_users`, `daily_active_vscode_agent_users`, `daily_passive_copilot_code_review_users`, `weekly_active_users`, `weekly_active_copilot_cloud_agent_users`, `weekly_active_copilot_code_review_users`, `weekly_active_vscode_agent_users`, `weekly_passive_copilot_code_review_users`, `monthly_active_users`, `monthly_active_agent_users`, `monthly_active_chat_users`, `monthly_active_copilot_cloud_agent_users`, `monthly_active_copilot_code_review_users`, `monthly_active_vscode_agent_users`, `monthly_passive_copilot_code_review_users` long |
| Usage flags | `used_agent`, `used_chat`, `used_cli`, `used_copilot_app`, `used_copilot_coding_agent`, `used_copilot_cloud_agent`, `used_copilot_code_review_active`, `used_copilot_code_review_passive`, `used_vscode_agent` boolean |
| Pull-request metrics | `pr_total_reviewed`, `pr_total_created`, `pr_total_created_by_copilot`, `pr_total_reviewed_by_copilot`, `pr_total_merged`, `pr_total_suggestions`, `pr_total_applied_suggestions`, `pr_total_merged_created_by_copilot`, `pr_total_copilot_suggestions`, `pr_total_copilot_applied_suggestions`, `pr_total_merged_reviewed_by_copilot` long; `pr_median_minutes_to_merge`, `pr_median_minutes_to_merge_copilot_authored`, `pr_median_minutes_to_merge_copilot_reviewed` double |
| Breakdown metrics | `user_initiated_interaction_count`, `interaction_count`, `session_count`, `code_generation_activity_count`, `code_acceptance_activity_count`, `loc_suggested_to_add_sum`, `loc_suggested_to_delete_sum`, `loc_added_sum`, `loc_deleted_sum` long |

### Tables

Each `Columns` entry below is in addition to Base and Lineage unless stated
otherwise. The key columns are required where the contract marks them so.

| Table | Additional columns | Upsert key |
| --- | --- | --- |
| `entity_daily` | Usage counters; pull-request metrics; `report_start_day`, `report_end_day` date | scope, day |
| `user_daily` | `user_id` long **required**, `user_login` string; Usage counters; Usage flags; pull-request metrics; `adoption_phase` string, `adoption_phase_number` long, `adoption_phase_version` string | scope, day, user |
| `user_team_daily` | `user_id` long **required**, `user_login` string, `team_id` long **required**, `team_slug` string | scope, day, user, team |
| `repository_daily` | `repo_id` long **required**, `repo_owner_name`, `repo_name`, `repo_visibility` string; pull-request metrics | scope, day, repository |
| `feature_daily` | `user_id` long, `feature` string **required**; Breakdown metrics | scope, day, user, feature |
| `ide_daily` | `user_id` long, `ide` string **required**, `ide_version`, `ide_version_sampled_at`, `plugin`, `plugin_version`, `plugin_version_sampled_at` string; Breakdown metrics | scope, day, user, IDE |
| `language_feature_daily` | `user_id` long, `language`, `feature` string **required**; Breakdown metrics | scope, day, user, language, feature |
| `language_model_daily` | `user_id` long, `language`, `model` string **required**; Breakdown metrics | scope, day, user, language, model |
| `model_feature_daily` | `user_id` long, `model`, `feature` string **required**; Breakdown metrics | scope, day, user, model, feature |
| `custom_agent_daily` | `user_id` long, `custom_agent` string **required**; Breakdown metrics | scope, day, user, custom agent |
| `third_party_agent_daily` | `user_id` long, `agent_id` string **required**, `agent_name` string; Breakdown metrics | scope, day, user, agent ID |
| `mcp_daily` | `user_id` long, `mcp` string **required**; Breakdown metrics | scope, day, user, MCP |
| `plugin_daily` | `user_id` long, `plugin` string **required**; Breakdown metrics | scope, day, user, plugin |
| `skill_daily` | `user_id` long, `skill` string **required**; Breakdown metrics | scope, day, user, skill |
| `slash_command_daily` | `user_id` long, `slash_command` string **required**; Breakdown metrics | scope, day, user, command |
| `adoption_phase_daily` | `user_id` long; `phase` string **required**; `phase_number`, `total_engaged_users`, `users_in_phase_28d`, `total_pull_requests_merged` long; `avg_code_acceptance_activities`, `avg_code_generation_activities`, `avg_loc_added`, `avg_loc_deleted`, `avg_pull_requests_created`, `avg_pull_requests_median_minutes_to_merge`, `avg_pull_requests_merged`, `avg_pull_requests_minutes_to_review`, `avg_pull_requests_review_cycles`, `avg_pull_requests_reviewed`, `avg_user_initiated_interactions` double | scope, day, user, phase |
| `cli_daily` | `user_id`, `prompt_count`, `request_count`, `session_count`, `output_tokens_sum`, `prompt_tokens_sum` long; `avg_tokens_per_request` double; `last_known_cli_version`, `last_known_cli_version_sampled_at` string | scope, day, user |
| `copilot_app_daily` | `user_id`, `prompt_count`, `request_count`, `session_count`, `output_tokens_sum`, `prompt_tokens_sum` long; `avg_tokens_per_request` double | scope, day, user |
| `vscode_agent_daily` | `user_id`, `session_count`, `total_user_messages` long | scope, day, user |
| `feature_engagement_daily` | `feature` string **required**; `active_user_count`, `engaged_user_count` long | scope, day, feature |
| `repository_pr_comment_type_daily` | `repo_id` long **required**, `comment_type` string **required**, `total_copilot_suggestions`, `total_copilot_applied_suggestions` long | scope, day, repository, comment type |
| `repository_pr_review_time_daily` | `repo_id` long **required**; `authored_by`, `reviewed_by` string **required**; `total_merged` long; six `median_`/`p90_` duration columns listed below as double | scope, day, repository, author class, reviewer class |

The six review-time duration columns are
`median_minutes_ready_to_first_review`,
`p90_minutes_ready_to_first_review`,
`median_minutes_first_to_final_review`,
`p90_minutes_first_to_final_review`,
`median_minutes_final_review_to_merge`, and
`p90_minutes_final_review_to_merge`.

### Quality tables

These tables do not use the shared Base/Lineage shape.

| Table | Complete columns | Upsert key |
| --- | --- | --- |
| `silver_quarantine` | `scope_kind`, `scope_slug`, `report_type`, `target_table`, `source_record_hash`, `reason_codes_json`, `raw_record_json` string **required**; `day` date; `source_ingestion_id`, `source_path`, `nested_field` string; `nested_index` long | record hash, target, nested field/index |
| `silver_schema_drift` | `scope_kind`, `scope_slug`, `report_type`, `source_record_hash`, `field_path`, `observed_type`, `sample_value_json` string **required**; `day` date | record hash, field path |
| `silver_quality_issues` | `target_table`, `check_name`, `severity`, `message` string **required**; `upsert_key_json` string | target, key JSON, check |

`user_id` is nullable in breakdowns because entity reports and user reports can
populate the same explicit table. All counters represent source values; Silver
does not invent zero for a missing optional field.

## Repeating breakdown tables

| Source field | Silver table | Additional dimensions |
| --- | --- | --- |
| `totals_by_feature` | `feature_daily` | `user_id`, `feature` |
| `totals_by_ide` | `ide_daily` | `user_id`, `ide`; IDE/plugin versions are flattened |
| `totals_by_language_feature` | `language_feature_daily` | `user_id`, `language`, `feature` |
| `totals_by_language_model` | `language_model_daily` | `user_id`, `language`, `model` |
| `totals_by_model_feature` | `model_feature_daily` | `user_id`, `model`, `feature` |
| `totals_by_custom_agent` | `custom_agent_daily` | `user_id`, `custom_agent` |
| `totals_by_3rd_party_agent` | `third_party_agent_daily` | `user_id`, `agent_id` |
| `totals_by_mcp` | `mcp_daily` | `user_id`, `mcp` |
| `totals_by_plugin` | `plugin_daily` | `user_id`, `plugin` |
| `totals_by_skill` | `skill_daily` | `user_id`, `skill` |
| `totals_by_slash_cmd` | `slash_command_daily` | `user_id`, `slash_command` |
| `totals_by_ai_adoption_phase` | `adoption_phase_daily` | phase and adoption averages/populations |

`user_id` is null for entity-level rows and populated for per-user rows. This
keeps one explicit schema per breakdown while preserving its source grain.

## Complex scalar and repository child tables

| Source field | Silver table | Notes |
| --- | --- | --- |
| `totals_by_cli` | `cli_daily` | Counts, token usage, and last-known CLI version |
| `totals_by_copilot_app` | `copilot_app_daily` | Counts and flattened token usage |
| `totals_by_vscode_agent` | `vscode_agent_daily` | Session and user-message counts |
| `copilot_feature_engagement.totals_by_feature` | `feature_engagement_daily` | Entity engagement population by feature |
| `pull_requests.copilot_suggestions_by_comment_type` | `repository_pr_comment_type_daily` | Copilot PR suggestions by comment type |
| `pull_request_review_times` | `repository_pr_review_time_daily` | Author/reviewer combination and review-time percentiles |

## Validation, drift, and quarantine

- Required day and stable IDs are type checked.
- The row day must match the Bronze partition day when one is supplied.
- Counters, line counts, token counts, durations, and averages must be
  non-negative numbers.
- Boolean usage flags must be boolean or null.
- Acceptance activities cannot exceed generation activities.
- Copilot-attributed repository totals cannot exceed their corresponding total.
- Sparse or absent optional arrays are valid and produce no child rows.
- Invalid parent rows are written to `silver_quarantine`; invalid nested
  entries are quarantined independently so a valid parent is retained.
- Unknown fields are retained as observations in `silver_schema_drift` with
  field path, observed type, and a bounded JSON sample. They do not silently
  alter production table schemas.

## Fabric notebook use

`fabric/notebooks/build_silver.ipynb` is the deployable parameterized notebook.
It accepts Bronze manifests, selects the latest complete/no-data snapshot,
normalizes each retained NDJSON file, creates DataFrames with explicit
`StructType` contracts, and performs scope/report/day-limited Delta
replacement. Each bounded replacement is a single Delta `MERGE`: matching rows
are updated, new rows are inserted, and target rows missing from the corrected
snapshot are deleted with `whenNotMatchedBySourceDelete`. Source rows are
validated against the escaped scope/report/day predicate before the
transaction, so failures cannot leave a deleted or partially appended
snapshot. Importing or unit testing the core package does not import PySpark.

The notebook expects a schema-enabled Lakehouse and a Fabric Environment (or
wheel attachment) containing this package. It implements Silver only; Gold
aggregates, orchestration pipelines, BI assets, and deployment are outside its
scope.
