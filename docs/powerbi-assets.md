# Power BI assets

The PBIP project under
`assets/powerbi/GitHubCopilotMetrics/GitHubCopilotMetrics.pbip` is a
source-controlled starter report and semantic model for the versioned
`gold` tables documented in `gold-data-dictionary.md`.

## Demo scripts

### Local illustrative fallback and evidence status

The [self-contained HTML mockup](../assets/demo/index.html) has five illustrative
pages, with an [opening slide](../assets/demo/opening.svg) and local captures:
[Executive](../assets/demo/executive.png), [Teams](../assets/demo/teams.png),
[Experience](../assets/demo/experience.png),
[Repositories](../assets/demo/repositories.png), and
[Quality](../assets/demo/quality.png). Serve it from the repository root:

```bash
python -m http.server 8000 --directory assets/demo
```

Open `http://localhost:8000`. For either timed script below, explicitly announce
**SYNTHETIC ILLUSTRATIVE MOCKUP — NOT POWER BI — NOT EXECUTION EVIDENCE**.
Values are fabricated illustrations, not actual observed telemetry. Describe
interactions as HTML mockup behavior, not proof of model measures or Direct Lake.
Use this fallback to discuss the business story only; it cannot close the
rendered-report, Fabric, pipeline, or video evidence gates.
Only page navigation works in the mockup. Narrate the filter and slice steps
below rather than claiming they were performed; those steps require Power BI.
The [capture manifest](../assets/demo/manifest.json) records source, dates,
browser, and outstanding evidence.

**Evidence status:** implementation-agent-reported local results are
**209 tests passed, 2 integration tests skipped; lint passed; build passed;
configuration/asset validation passed**. These are not CI results and do not
verify a live service. Local mockup screenshots are illustrative captures only.
Actual Power BI/Fabric rendering and credentialed end-to-end evidence remain
outstanding, as does the required walkthrough video.

Prepare using the [isolated demo path](bootstrap-guide.md#isolated-synthetic-demo-no-github-token).
Use the fixed **2026-07-09 through 2026-10-06** window unless overrides are
explicitly documented; do not describe it as current telemetry. Confirm the
report's model targets the synthetic Lakehouse and scope `synthetic-demo`,
reset filters, and review every page before the meeting. Keep the label
**SYNTHETIC DEMO — NOT CUSTOMER DATA — GOLD-ONLY** visible in the presentation
and every capture; if the report itself has no banner, add one in the capture
or presentation framing. Do not imply a banner is already implemented.

### 5-minute sponsor script

| Time | Show / say | Decision prompt |
| --- | --- | --- |
| 0:00-0:30 | State the synthetic label, fixed dates, and question: "Where would we focus enablement?" Explain Gold-only, not a live pipeline. | "Which adoption decision matters to you?" |
| 0:30-1:45 | Executive overview: point to adoption and trend; change one date filter and explain DAU versus distinct rolling users. Use observed values only. | "Which change deserves investigation?" |
| 1:45-2:45 | Developer experience: explore a feature/IDE/language slice available on the page. Acceptance is a ratio of sums, not measured productivity. | "Would training or workflow support be a useful experiment?" |
| 2:45-3:30 | Team adoption: explain overlap, privacy suppression, and additive mapping dependency. If cards are blank/zero, explain why; never imply live mapping is installed. | "Who can approve a primary-team policy?" |
| 3:30-4:15 | Data quality: show synthetic freshness and distinguish expected lag from missing data. No live SLA proof. | "What freshness and access controls would your pilot need?" |
| 4:15-5:00 | Recap one hypothesis, missing context, and next action; offer a discovery/workshop and scoped pilot. | Agree sponsor, owner, and review date—not an ROI promise. |

### 15-minute workshop script

| Time | Presenter actions / talk track | Check with the audience |
| --- | --- | --- |
| 0:00-1:30 | Declare synthetic scope, dates, and Gold-only boundary; confirm the decision and avoid employee ranking. | Sponsor agrees the question and purpose. |
| 1:30-4:00 | Executive overview: reset filters, show one date-window change, explain active versus observed users and distinct rolling counts. Read actual values; do not memorize invented KPI claims. | Ask what external context could explain a trend. |
| 4:00-6:30 | Team adoption: discuss overlapping memberships, suppression, and blank executive cards without governed mapping. Synthetic allocations, if present, are illustrative only. | Identify mapping steward and an acceptable allocation policy. |
| 6:30-9:00 | Developer experience: explore supported feature/IDE/language slices; explain numerator/denominator, null versus zero, and possible training experiment. | Select one enablement hypothesis and qualitative feedback source. |
| 9:00-11:00 | Repository impact: discuss rates and the median-of-repository-day-medians limitation, not event-level cycle-time improvement. | Identify confounders and needed delivery context. |
| 11:00-13:00 | Data quality and architecture: inspect freshness; explain live Bronze manifests, Silver drift/quarantine, and Gold audit. Show source references if no live artifacts exist; never manufacture them. | Agree evidence needed to trust a real pipeline. |
| 13:00-15:00 | Review inactive RLS, absent production mapping, token rotation, capacity/cost and unassigned support. Summarize action and pilot gates. | Name sponsor/operator/steward, next meeting, and go/no-go criteria. |

If a page is unavailable or renders incorrectly, record the failure and show
the source design only. A local validator, synthetic row count, or successful
deployment request cannot substitute for seeing the report render.

## Evidence and capture checklist

These are **required real-report/runtime captures**, not evidence supplied by
the local HTML mockup. No live credentials were available and no actual Power BI
screenshot, walkthrough video, or successful live run is claimed. Local
illustrative mockup screenshots are a separate evidence class; keep
pending/not-run entries explicit for every real-report requirement.

- [ ] Record commit/version, capture date, tool/Desktop/service versions,
  operator/reviewer, scenario, fixed or overridden data dates, target aliases,
  and pass/fail/not-run status in an approved evidence index.
- [ ] Save sanitized demo dry-run and actual apply outcomes, target-isolation
  verification, metadata ownership marker, scope `synthetic-demo`, expected
  Gold table inventory and row/date checks. Do not expose tenant/item IDs.
- [ ] Capture **Executive overview** with selected dates and readable KPIs;
  **Team adoption** including allocation caveat or genuine blank-card state;
  **Developer experience** with a meaningful supported slice;
  **Repository impact** with metric limitations; and **Data quality** with
  lag/missing interpretation. Capture all five rendered pages, not source JSON.
- [ ] Label every synthetic screenshot/video segment
  **SYNTHETIC DEMO — NOT CUSTOMER DATA — GOLD-ONLY**, including captions,
  thumbnails, and slides. Suggested names: `synthetic-demo-executive-overview.png`
  and `synthetic-demo-walkthrough.mp4`; names are examples, not existing assets.
- [ ] Record a short 2-5 minute video showing the synthetic label, fixed window,
  an actual filter interaction, page navigation, quality caveat, and next step.
  Review audio/captions and remove browser/account identifiers and notifications.
- [ ] For a separate **live** claim, require approved scope, actual sanitized
  Bronze manifests, Silver/Gold stage audits, lineage/date reconciliation,
  schedule/rotation results, and rendered live report proof. Label live and
  synthetic material separately. Synthetic Gold never closes this item.
- [ ] Have a second reviewer approve readability, metric wording, redaction,
  permission to share, storage location, and retention before external reuse.

### Reuse and anonymization

Prefer synthetic material for public demos. Reuse source/assets under the
[repository license](../LICENSE); this does not grant permission to redistribute
customer data, branding, screenshots, or third-party media. Obtain explicit
customer/security approval for each audience and channel. Remove user/logins,
organization/team/repository names, tenant/workspace/item IDs, URLs, bookmarks,
tokens, signed links, and identifying browser chrome; inspect tooltips, filters,
audio, captions, file metadata, and thumbnails too.

Renaming a customer is not anonymization: small groups, dates, rare workflows,
and combined slices may re-identify people. Aggregate/suppress or replace with
synthetic content, then seek steward review. Store raw evidence privately with
access and retention controls. Never commit live exports or unredacted captures.
Publish only reviewed synthetic or explicitly authorized sanitized derivatives;
do not link unverifiable remote screenshots as deployment proof.

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
