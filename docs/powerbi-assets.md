# Power BI assets

The PBIP project under
`assets/powerbi/GitHubCopilotMetrics/GitHubCopilotMetrics.pbip` is a
source-controlled starter report and semantic model for the finalized
`gold` tables documented in `gold-data-dictionary.md`.

## Deployment prerequisites

- A Fabric workspace with a Lakehouse containing the Gold Delta tables.
- Power BI Desktop with PBIP, PBIR, TMDL, and Direct Lake support enabled.
- Build permission on the Lakehouse SQL analytics endpoint and permission to
  create or update semantic models and reports in the target workspace.
- A supported Fabric capacity. Direct Lake availability and guardrails vary by
  capacity SKU and current service limits.

Before opening the PBIP, replace `<WORKSPACE_NAME>` and `<LAKEHOUSE_NAME>` in
`GitHubCopilotMetrics.SemanticModel/definition/expressions.tmdl`. Keep secrets,
workspace IDs, item IDs, connection IDs, and user assignments out of source.
The deployment CLI performs these two placeholder replacements automatically
when publishing the semantic model and binds the report to the deployed model.
Manual PBIP use requires replacing them before publication.

## Direct Lake assumptions

- Gold tables are in the schema named `gold`, with names and columns matching
  `copilot_metrics_fabric.gold.contracts()`.
- Gold table and column renames are breaking changes for the semantic model.
- Fact tables use Direct Lake entity partitions. The `Date` table is a small
  calculated Import table covering 2020-2035 so it can be source controlled
  without requiring an additional Gold table. Adjust the range before
  production use. If a pure Direct Lake model is required, materialize a Gold
  date dimension and replace the calculated partition.
- Date relationships are one-to-many and single-direction from `Date` to every
  daily Gold table.
- User-level Gold tables are intentionally omitted. The report exposes
  aggregate adoption, team, feature, IDE, language, repository, rolling, and
  freshness data only.

## Measure semantics

- DAU is the sum of entity daily active users in the current date/scope
  context.
- WAU and 28-day active users use Gold's precomputed distinct rolling users;
  daily active-user counts are never summed to approximate rolling users.
- Acceptance, LOC acceptance, feature adoption, and repository rates are
  ratios of summed numerators and denominators using `DIVIDE`; stored row
  percentages are hidden and are not averaged.
- Team executive measures filter to `is_additive = TRUE()`. The warning
  measures surface the presence of `all_memberships` rows, which can
  double-count users when teams are summed.
- The deployed Gold notebook does not currently load a primary-team mapping.
  A standard deployment therefore emits only non-additive team rows, so
  executive team measures remain blank/zero until governed `primary_team`
  rows are materialized.
- Freshness measures distinguish incomplete days outside telemetry lag from
  expected lag and expose sparse metrics.

## Report pages

The starter PBIR contains data-bound KPI cards and a date trend on:

1. Executive overview
2. Team adoption
3. Developer experience
4. Repository impact
5. Data quality

The layouts are intentionally conservative starters. Validate and visually
review them in the Power BI Desktop version used by the target workspace
before publication.

## Optional RLS hook

`templates/ScopeFilterTemplate.tmdl` is intentionally outside the semantic
model definition and therefore inactive. It denies all rows until replaced.
To use it:

1. Materialize and govern an authorized-principal-to-scope mapping.
2. Add a shared scope dimension or equivalent tested filters.
3. Replace the `FALSE()` placeholder on every exposed fact table.
4. Copy the reviewed role into the model definition.
5. Configure role membership in the Power BI portal and test with **View as**.

Never enable the template as-is and never place user or group membership in
source control.

## Regeneration and validation

Regenerate deterministic assets with:

```powershell
python scripts\generate_powerbi_assets.py
```

Run repository validation with:

```powershell
python -m pytest
python -m ruff check .
```

When available, also run:

```powershell
powerbi-report-author validate assets\powerbi\GitHubCopilotMetrics\GitHubCopilotMetrics.Report
```

Then open the `.pbip` in Power BI Desktop, refresh model metadata, and inspect
every page. The report-authoring CLI and Desktop bridge are optional developer
tools and are not installed by this repository.
