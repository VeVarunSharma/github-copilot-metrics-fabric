# Architecture, governance, and operations

## Data flow

```text
GitHub one-day report metadata
        │ short-lived signed URLs
        ▼
Bronze notebook / BronzeIngestor
        │ immutable NDJSON, manifests, attempt audit
        ▼
Files/bronze
        ▼
Silver notebook
        │ validation, quarantine, drift, deterministic Delta MERGE
        ▼
silver.* Delta tables
        ▼
Gold notebook
        │ aggregation, rolling users, freshness, team allocation
        ▼
gold.* Delta tables
        │ Direct Lake
        ▼
Semantic model → PBIR report
```

The pipeline runs Bronze, Silver, and Gold sequentially. Bronze requests
`entity`, `users`, `user-teams`, and `repositories` unless `report_types` is
overridden. A failed stage writes a sanitized audit row when possible,
re-raises the exception, and prevents downstream stages from starting.

## Storage, lineage, and audit

Under the Bronze root (`Files/bronze` by default), paths are:

```text
scope_kind=<kind>/scope=<slug>/report_type=<type>/report_day=<day>/
  ingestion_id=<id>/part-00000.ndjson
_manifests/scope_kind=<kind>/scope=<slug>/report_type=<type>/
  report_day=<day>/<ingestion-id>.json
_audit/<ingestion-id>.json
```

Content-identical reruns are deduplicated by SHA-256. Changed content is
retained as a new ingestion. Silver records ingestion ID/time, source path, and
record hash. Unknown fields go to `silver.silver_schema_drift`; invalid rows
to `silver.silver_quarantine`; duplicate-key warnings to
`silver.silver_quality_issues`.

Each notebook appends to `audit.pipeline_run_results`:
`run_id`, `stage`, `status`, `started_at`, `completed_at`, `scope_kind`,
`scope_slug`, `start_date`, `end_date`, `validation_status`, and sanitized
`details_json`.

## Metric semantics

- A report day is GitHub's UTC `day`.
- `observed_users` counts distinct user IDs present at the applicable grain.
- An **engaged** user has a positive interaction, generation, acceptance,
  added-line, or deleted-line metric.
- An **active** user is engaged or has any implemented `used_*` flag set.
- `acceptance_rate = SUM(code_acceptance_activity_count) /
  SUM(code_generation_activity_count)`.
- `loc_acceptance_rate = SUM(loc_added_sum) /
  SUM(loc_suggested_to_add_sum)`.
- A zero/absent denominator produces null. Missing is not zero.
- Derived 7/28-day users are distinct users active anywhere in the inclusive
  window. Source weekly/monthly values are separate daily snapshots and are
  never summed.
- Repository rates use summed numerators and denominators. The Power BI
  `Median Minutes to Merge` is the median of repository-day medians, not an
  event-level median.
- `source_correction_count` counts losing duplicate Silver rows for that
  scope/day in the current Gold build.

See the [Silver dictionary](silver-data-dictionary.md), [Gold dictionary](gold-data-dictionary.md),
and [Power BI measures](powerbi-assets.md).

## Team suppression and non-additivity

GitHub's user-team report omits teams below its privacy threshold (currently
fewer than five Copilot-seated users for the day). Those users can still
appear in `user_daily`, so scope totals can exceed visible-team totals. Teams
can appear/disappear as daily membership crosses the threshold. This project
does not infer suppressed teams.

Users can belong to multiple visible teams. Gold always emits
`allocation_method=all_memberships`, `is_additive=false`; summing these rows
can double-count users and activity. The pure-Python Gold API can also accept a
governed `primary_team_mapping` and emit additive `primary_team` rows. The
deployed Gold notebook does not load such a mapping, so standard deployment
produces only non-additive team rows. Starter Power BI team measures filter to
`is_additive=true` and therefore remain blank/zero until primary-team rows are
materialized.

## Privacy and RLS

Raw and Silver data contains user IDs, logins, team membership, and granular
activity. Treat it as confidential workforce data:

- document purpose, retention, and access reviews;
- restrict Lakehouse, SQL endpoint, notebook, and workspace access;
- separate operators from report consumers;
- avoid exporting user data to broadly shared workspaces;
- use required network controls;
- audit Key Vault, Fabric, and GitHub token access; and
- delete expired development extracts.

The starter report excludes user-level Gold tables, but aggregate slices may
still be sensitive. No RLS role is active by default.
`assets\powerbi\GitHubCopilotMetrics\templates\ScopeFilterTemplate.tmdl` is a
deny-all template. Production RLS requires a governed principal-to-scope
mapping, tested relationships/filters, and portal role membership. Test with
**View as**, including multi-group users. RLS does not replace workspace
permissions.

## Monitoring and runbook

Monitor:

1. Fabric pipeline/activity status and retries.
2. `audit.pipeline_run_results` failures and warnings.
3. `gold.data_freshness_daily`: `complete`, `within_lag`, or `missing`.
4. Silver quarantine, schema drift, and quality issues.
5. Power BI's `Incomplete Report Days`, `Maximum Days Late`, and
   `Sparse Metric Count`.

Alerting is not deployed by this repository. Recommended policy is to alert on
pipeline failure, any `missing` day, growing quarantine, or new drift paths.

Daily, verify each scope's scheduled run and freshness. Weekly, review quality
tables, token rotation/expiry, capacity health, and Direct Lake guardrails.
After a correction/outage, backfill the inclusive affected range and verify
Bronze manifests, Silver merges, Gold correction counts, and freshness.

## Schema evolution and upgrades

Unknown source fields do not automatically alter schemas. To adopt one:

1. Update Silver contracts/normalizers, validation, and fixtures.
2. Update Spark schemas/notebooks.
3. Update Gold semantics if applicable.
4. Update TMDL, measures, visuals, and data dictionaries.
5. Run validation, tests, lint, build, and a non-production backfill.

Gold table/column removal or rename is a breaking Power BI change. Upgrade a
fork by merging upstream, rebuilding the wheel, updating the Fabric
Environment, validating/planning/deploying, then running a bounded backfill.
Keep `.fabric-deploy-state.json` for the same target; remove it only to force
definition updates.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| GitHub `401` | Token value, expiry, and authorization. |
| GitHub `403` | Metrics policy, caller role, token permission/scope, SAML/IP policy. |
| GitHub `404` | Scope slug and whether the token can see it. |
| GitHub `204` | Valid no-data day; inspect the no-data manifest. |
| Key Vault read fails | URI/name, run identity, RBAC propagation, firewall/network. |
| `plan` cannot authenticate | `az login` or another `DefaultAzureCredential` source. |
| Duplicate item error | Make display names unique; discovery refuses ambiguity. |
| Existing Lakehouse fails | Confirm schemas are enabled and notebook binding is allowed. |
| Deployment always updates | Preserve the state file and item IDs. |
| Team cards blank | No additive `primary_team` rows exist. |
| Team missing | GitHub privacy suppression or no membership row that day. |
| Freshness is missing | Check manifests, pipeline audit, date window, and lag. |
| New fields absent | Review `silver_schema_drift`; explicit contract work is required. |
| Report schema error | Gold changed; update TMDL and regenerate/validate PBIP assets. |
