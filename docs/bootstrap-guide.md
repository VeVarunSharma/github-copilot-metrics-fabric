# One-command setup

This is the recommended **live-data** setup path for a fork of this repository. It supports
Windows, macOS, and Linux and creates or reuses the required Azure and
Microsoft Fabric resources.

## Isolated synthetic demo (no GitHub token)

Use this path for a sales walkthrough or workshop before requesting workforce
data access. It still requires Fabric authentication, capacity, and permission
to access the target; it is not an offline demo or a free-capacity promise.

1. Use a dedicated demo workspace and a schema-enabled Lakehouse, separate from
   every live deployment. Start empty, or reuse only a target containing
   demo-owned known Gold tables and demo metadata. Never target live tables.
2. Install the project using the [deployment guide](deployment-guide.md#fork-and-install)
   and authenticate to Azure/Fabric. Record the actual workspace/lakehouse UUIDs
   and exact Lakehouse name privately.
3. Preview the loader from the repository root, substituting the placeholders:

   ```bash
   python scripts/load_demo_gold.py --workspace-id UUID --lakehouse-id UUID --lakehouse-name NAME
   ```

   Offline planning is the default: it prints the supplied target and dates,
   without authentication, discovery, or writes. Inspect isolation separately;
   actual ownership/table checks run inside the notebook before table writes.
4. Only after confirming isolation, apply:

   ```bash
   python scripts/load_demo_gold.py --workspace-id UUID --lakehouse-id UUID --lakehouse-name NAME --confirm-isolated-demo --yes
   ```

   Both `--confirm-isolated-demo` and `--yes` are required to apply. A flag is
   not a substitute for inspecting the actual target. The loader must refuse
   unknown/live tables or missing/conflicting ownership on an existing target;
   do not defeat that refusal by deleting ownership metadata.
   Existing targets require the exact demo ownership marker, only approved
   Gold tables, and only `synthetic-demo` scope in metric rows. Temporary tables
   are refused. A partial failure without the marker requires a fresh dedicated
   Lakehouse or manual inspection; do not blindly retry against uncertain data.
   No concurrent writers are allowed during planning/application.
5. The default inclusive dates are **2026-07-09 through 2026-10-06**, not a
   moving "last 90 days." Optional `--start-date YYYY-MM-DD --end-date YYYY-MM-DD`
   overrides must be recorded with evidence. Scope is `synthetic-demo`, with a
   metadata ownership marker identifying demo-managed content.
   Date windows are limited to 366 days within the model calendar, 2020-2035.
6. Bind/publish the starter model and report using the
   [Power BI guide](powerbi-assets.md), ensuring they reference the demo
   Lakehouse rather than a live model. Select the fixed date range explicitly.
   Inspect all pages before presenting; a successful load is not visual proof.

This loader writes synthetic **Gold only**, bypassing Bronze and Silver.
Do not run live bootstrap/backfill or schedules against this demo Lakehouse.
Do not describe demo freshness rows as proof of a live ingestion SLA, or demo
team allocations as evidence that production mapping is configured.
Use the [timed scripts and capture checklist](powerbi-assets.md#demo-scripts);
if Fabric is unavailable, explain the source assets without claiming a rendered
Power BI demo. The [local illustrative HTML mockup](../assets/demo/index.html)
can support the story without Fabric; its screenshots are not execution proof.
No live end-to-end evidence is supplied by this guide.

## Live-path readiness checkpoint

Before running setup, identify a sponsor, GitHub metrics administrator, Fabric
operator, secret-rotation owner, and workforce-data approver. Record approved
scope, retention, budget, audience, and pilot exit criteria. These roles are
**unassigned by this repository**. Bootstrap creates resources; it does not
approve data use, activate RLS, or provide a governed primary-team mapping.
See [release gates](deployment-guide.md#release-and-pilot-acceptance-gates).

## Prerequisites

- Python 3.10 or later, Git, and Azure CLI 2.61 or later.
- An Azure subscription where you can manage the configured resource group
  and Key Vault and assign **Key Vault Secrets User**.
- A Microsoft Fabric capacity.
- Fabric workspace Admin when bootstrap must set workspace Spark defaults.
- GitHub permission to view Copilot metrics for the configured organization or
  enterprise.
- The GitHub enterprise policy **Copilot usage metrics** enabled.

Authenticate first:

```powershell
az login
```

## Run setup

Windows:

```powershell
.\scripts\setup.ps1
```

macOS or Linux:

```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

Common options:

```text
--plan-only / -PlanOnly       Stop after read-only planning
--yes / -Yes                 Skip confirmation in an approved workflow
--config / -Config PATH      Use another config file
--force-init / -ForceInit    Re-run the configuration wizard
```

Without the yes option, bootstrap prints every persistent cloud change and
asks for confirmation before writing anything.

## GitHub App environment variables

For a GitHub App-based POC, copy `.env.example` to the ignored `.env` file and
set:

```text
GITHUB_APP_ID=<GitHub App ID>
GITHUB_APP_INSTALLATION_ID=<enterprise installation ID>
GITHUB_APP_PRIVATE_KEY_PATH=config/<private-key-file>.pem
```

The path variable points to the PEM file; never paste the PEM contents into
`.env`. Both `.env` and matching private-key files under `config` are ignored
by Git.

The current bootstrap prompt expects an access token. Generate a short-lived
installation token from these values, then paste that token at the hidden
prompt. GitHub App installation tokens expire after one hour, so this flow is
appropriate only for a time-bounded initial POC load. The deployed Bronze
notebook reads an access token from Key Vault; it does **not** mint or renew
GitHub App installation tokens. A durable schedule needs an approved token
lifetime/rotation process, or a separately implemented and validated renewable
App runtime. Merely storing a private key in Key Vault does not implement it.
See the [token rotation runbook](architecture-operations.md#key-vault-token-rotation-runbook)
before enabling a daily schedule.

## What setup does

1. Creates `.venv` and installs the project.
2. Runs `ghcp-metrics init` if the config file doesn't exist.
3. Validates the configuration and deployment assets.
4. Performs read-only Azure and Fabric discovery.
5. Displays the ordered plan and asks for confirmation.
6. Prompts for the GitHub token using hidden input.
7. Validates the token against the configured Copilot metrics scope.
8. Creates or reuses the resource group and RBAC-enabled Key Vault.
9. Reconciles **Key Vault Secrets Officer** for the bootstrap writer and
   **Key Vault Secrets User** for the configured Fabric runtime identity, then
   writes the token directly to Key Vault without placing it in process
   arguments.
10. Creates or updates the schema-enabled Lakehouse, Fabric Environment,
    notebooks, pipeline, Direct Lake model, and report.
11. Builds and publishes the project wheel in the Fabric Environment and
    makes it the workspace Spark default.
12. Runs the configured initial backfill.
13. Reconciles one project-managed daily schedule per scope without changing
    unrelated schedules.
14. Prints links to the deployed Fabric resources.

## Direct CLI commands

```powershell
ghcp-metrics init --output config\config.yml
ghcp-metrics validate --config config\config.yml
ghcp-metrics bootstrap plan --config config\config.yml
ghcp-metrics bootstrap apply --config config\config.yml
ghcp-metrics bootstrap status --config config\config.yml
ghcp-metrics bootstrap resume --config config\config.yml
```

Add `--json` to `plan` or `status` for machine-readable output.

The wizard's schedule time zone must be an exact
[Windows time-zone ID documented by Microsoft](https://learn.microsoft.com/windows-hardware/manufacture/desktop/default-time-zones)
and accepted by Fabric, such as `UTC`, `Pacific Standard Time`, `UTC-11`, or
`UTC+12`. Validation uses a maintained in-package set on every operating
system; it does not query the Windows registry. IANA names such as
`America/Los_Angeles` and invented `* Standard Time` values are rejected.

## Safety and resume

`bootstrap plan` performs zero cloud writes. It doesn't change subscription
context, create resources, assign roles, write secrets, deploy Fabric
definitions, start a job, or create a schedule. It can poll a previously
submitted backfill and update the local job-state cache.

The ignored files `.fabric-deploy-state.json`, `.ghcp-bootstrap-state.json`,
and `.ghcp-job-state.json` contain IDs, hashes, completed phases, job and
schedule IDs, and links only. They never contain the GitHub token or
Azure/Fabric bearer tokens.

After a transient failure:

```powershell
ghcp-metrics bootstrap status --config config\config.yml
ghcp-metrics bootstrap resume --config config\config.yml
```

Completed phases are skipped. If Azure setup already completed, resume doesn't
ask for the GitHub token again.

The initial backfill is tracked separately from declarative reconciliation.
Completed ranges are identified from successful Gold records in
`audit.pipeline_run_results`; `.ghcp-job-state.json` is only a local cache.
With `wait_for_completion: false`, bootstrap records the submitted Fabric job
as pending. Later plan, apply, resume, and status commands poll that job and
mark the signature complete only after Fabric confirms success. A pending job
is never submitted twice. Failed or cancelled jobs remain visible; `resume`
does not rerun them, while a normal confirmed `apply` explicitly submits a new
attempt. Poll timeouts leave the job pending.
Daily schedules are rediscovered from their exact Fabric-returned pipeline
parameter shape and scope identity, so a fresh runner reuses them without
claiming unrelated schedules. Ambiguous matches fail without writes. Changing
the configured backfill parameters creates a new signature and intentionally
runs the new range.

## See the data

Open the report link printed by bootstrap. If the report is empty, use the
workspace link and inspect:

1. `audit.pipeline_run_results`;
2. `gold.data_freshness_daily`;
3. the latest pipeline job; and
4. the `bronze` Lakehouse Files folder.

The first run can take time while Fabric publishes the Environment, starts
Spark, downloads historical lookback data, and builds Silver and Gold tables.

## CI usage

Create configuration non-interactively:

```powershell
ghcp-metrics init --defaults --organization contoso `
  --output config\config.yml
ghcp-metrics bootstrap plan --config config\config.yml --json
```

For unattended application, inject a secret provider through the Python API.
Do not place the token in YAML or command-line arguments.

## Troubleshooting

- **Azure CLI isn't logged in:** run `az login`.
- **Subscription selection fails:** confirm subscription ID and tenant.
- **Role assignment is denied:** the caller needs role-assignment permission
  at the Key Vault scope.
- **Key Vault isn't RBAC-enabled:** use or create an RBAC-enabled vault.
- **Workspace creation fails:** supply a capacity ID or reuse a capacity-backed
  workspace.
- **Environment publish fails:** inspect the Fabric Environment publication
  details for package/runtime errors.
- **Pipeline fails:** inspect the job and `audit.pipeline_run_results`.
- **Managed schedule ambiguity:** remove the duplicate project-managed
  schedule; bootstrap never chooses arbitrarily.

## Safe removal

There is intentionally no broad delete command. Disable or delete only the
project-managed schedule, then remove the named Fabric items and Key Vault
secret after confirming no other consumers use them. Delete a workspace,
resource group, or Key Vault only when it is dedicated to this project and
contains no unrelated resources.
