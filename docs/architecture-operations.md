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

### Governed primary-team mapping path

The pure-Python API's `primary_team_mapping` is an integration seam, not a
deployed mapping service. Before using additive executive team totals:

1. Assign a workforce-data steward and approve allocation purpose, eligible
   population, source of truth, refresh cadence, and effective-date policy.
2. Maintain the user-ID-to-primary-team-ID mapping in a restricted,
   customer-governed store, outside Git. Require one primary team per user for
   the relevant snapshot; quarantine conflicts, unknown IDs, and unapproved
   assignments. Explicitly account for unmapped users and privacy suppression.
3. Implement and review a notebook extension that loads the approved mapping
   and passes it to the Gold builder. The standard deployed notebook does not
   do this. A single mapping argument is not automatic historical membership
   versioning; define how each backfill receives the correct dated snapshot.
4. Test additive versus `all_memberships` rows, allocation flags, coverage,
   reassignment, duplicate prevention, and scope totals on an approved dataset.
   Do not force suppressed teams into visibility or fill blanks with invented
   attribution.
5. Run a bounded non-production backfill and have the steward and BI owner
   reconcile output and executive measures before enabling the live report.
   Record mapping version/approval and rollback instructions privately.

Until then, explain blank/zero executive team cards and use clearly identified
non-additive membership views only for exploration, never summed rollups.

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

## Key Vault token rotation runbook

The runtime fetches an access token from Key Vault when Bronze starts.
Automatic GitHub App token minting/renewal is **not implemented**. Installation
tokens expire after one hour; a successful initial load does not demonstrate a
durable daily schedule. An approved external renewal service would be a separate
implementation requiring security review and operational evidence.

1. Assign a secret owner and backup; record expiry, rotation lead time,
   approved GitHub scope/permissions, and alert/escalation route in a private
   inventory. Do not record token values.
2. Generate a replacement through the approved GitHub process. Validate the
   configured metrics scope using a secure session; distinguish valid `204`
   no-data from authentication failure. Never log bearer headers or signed URLs.
3. Write a new version under the configured Key Vault secret name using a
   secure secret-write flow (hidden input or approved SDK). Never use token
   values in command arguments, YAML, source, screenshots, or ticket text.
4. Confirm the actual Fabric run identity has **Key Vault Secrets User** and
   network access. Run a bounded validation with the same identity and inspect
   sanitized Bronze/pipeline audit and freshness. A running notebook may already
   hold the old token; validate with a newly started run.
5. Verify the next scheduled run, then revoke the old token when policy permits.
   If validation fails, pause affected schedules and escalate. Restore the
   previous valid secret only if still authorized and unexpired; otherwise
   issue a new token. Backfill affected dates after recovery.
6. Record rotation time, secret version reference (not value), validation run
   reference, expiry, and owner sign-off in the approved private evidence store.

Bootstrap resume does not renew tokens simply because a schedule exists. Before
handoff, rehearse expiry/failure recovery and demonstrate that the agreed
rotation interval is compatible with the chosen credential.

## Support ownership and handoff

This is a reusable starter, not a staffed managed service. All assignments
below are **unassigned** until explicitly accepted; do not infer support
ownership or an SLA from repository contributors.

| Responsibility | Current owner | Handoff requirement |
| --- | --- | --- |
| Business decision / adoption experiment | Unassigned | Sponsor, success criteria, review date. |
| GitHub policy, permissions, token issuance | Unassigned | GitHub admin and secret owner/backup; rotation rehearsal. |
| Fabric capacity, jobs, monitoring, recovery | Unassigned | Platform operator/backup, alerts, cost budget, bounded backfill rehearsal. |
| Model, visuals, refresh, access tests | Unassigned | BI owner; rendering and permission evidence. |
| Workforce-data use, mapping, RLS, retention | Unassigned | Data/security steward; access and retention approval. |
| Release/catalog distribution, upstream triage | Unassigned | Maintainer/catalog owner; verified schema before publication. |
| Customer incidents and escalation | Unassigned | Service owner, contact route, coverage hours, agreed response targets. |

Use repository issues only for sanitized reproducible defects or improvements,
not secrets, employee telemetry, or urgent service incidents. Follow
[SECURITY.md](../SECURITY.md) for security reporting. Each customer must define
its own incident route; no response time is promised here.

Before accepting handoff, complete the
[release/pilot gates](deployment-guide.md#release-and-pilot-acceptance-gates),
transfer access without transferring secrets through Git, record deployment
version and approved target inventory privately, and rehearse a failed-run
triage, token rotation, backfill, and rollback. Define the observation window,
freshness SLO, escalation thresholds, and cleanup/retention responsibility.
Rollback means stopping affected schedules, restoring reviewed definitions and
Environment/package versions, and reconciling affected data through a bounded
backfill; restoring source alone does not undo Delta data changes.

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
