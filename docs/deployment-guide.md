# Fork, configure, and deploy

> [!TIP]
> New installations should use the automated
> [one-command setup](bootstrap-guide.md). This document remains the advanced
> reference and manual recovery path.

This guide describes the deployment implemented in this repository. The CLI
creates or updates a schema-enabled Lakehouse, a published Fabric Environment,
three notebooks, one Data Pipeline, a Direct Lake semantic model, and a report.
The integrated `bootstrap plan/apply/status/resume` commands additionally
automate Azure Key Vault setup, initial execution, and the managed daily
schedule. Bootstrap does not create a Fabric capacity, Power BI role
assignments, or a primary-team mapping.

## Prerequisites and permissions

- GitHub Copilot Business or Enterprise with usage metrics enabled by policy.
- Access to the implemented one-day report endpoints under
  `/orgs/{org}/copilot/metrics/reports/` or
  `/enterprises/{enterprise}/copilot/metrics/reports/`.
- Python 3.10+, Git, and Azure CLI.
- A Fabric capacity and permission to create/update workspace items, or an
  existing workspace where you are at least a Contributor.
- Permission to use a schema-enabled Lakehouse and create/update notebooks,
  Data Pipelines, semantic models, and reports.
- An Azure Key Vault reachable by Fabric.

GitHub evaluates both the caller role and token permission. Verify current
requirements in the
[Copilot usage metrics REST documentation](https://docs.github.com/en/rest/copilot/copilot-usage).
Organization access normally requires an owner, enterprise owner, or custom
role with **View Copilot metrics**. Enterprise access normally requires an
enterprise owner, billing manager, or custom role with **View enterprise
Copilot metrics**. Prefer a fine-grained token limited to the target with the
corresponding Copilot metrics read permission. SAML authorization, IP allow
lists, token lifetime rules, and organization/enterprise token policy still
apply.

Test the exact scope before deployment:

```powershell
$env:GITHUB_TOKEN = "<token>"
$headers = @{
  Accept = "application/vnd.github+json"
  Authorization = "Bearer $env:GITHUB_TOKEN"
  "X-GitHub-Api-Version" = "2026-03-10"
}
Invoke-RestMethod -Headers $headers `
  -Uri "https://api.github.com/orgs/<org>/copilot/metrics/reports/organization-1-day?day=2026-09-30"
```

Use `enterprises/<enterprise>/.../enterprise-1-day` for enterprise mode.
`204 No Content` is a successful no-data result.

## Fork and install

```powershell
git clone https://github.com/<your-account>/<your-fork>.git
Set-Location <your-fork>
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
ghcp-metrics init --output config\config.yml
```

Keep `config\config.yml`, `.fabric-deploy-state.json`, collected data, and
local credentials uncommitted.

`init` is a local configuration wizard only. It validates organization or
enterprise scope, Azure/Fabric target names, optional backfill dates, and a
daily Fabric/Windows-time-zone schedule. Time-zone validation uses the
repository's cross-platform copy of Microsoft's documented
[Windows time-zone IDs](https://learn.microsoft.com/windows-hardware/manufacture/desktop/default-time-zones);
it does not depend on the Windows registry. Use exact IDs such as `UTC`,
`Pacific Standard Time`, `UTC-11`, or `UTC+12`, not IANA names or invented
values ending in `Standard Time`. The wizard never requests or persists
credentials.
Use `--defaults` (or `--non-interactive`) with `--organization` or
`--enterprise` in automation, `--output` to select a path, and `--force` only
when an existing file should be replaced. The Azure bootstrap settings are consumed by the integrated bootstrap CLI.

## GitHub configuration

Organization mode issues requests for every configured organization:

```yaml
github:
  mode: organization
  organizations: [contoso, fabrikam]
```

Enterprise mode issues enterprise-level requests:

```yaml
github:
  mode: enterprise
  enterprise: contoso-enterprise
  organizations: []
```

In enterprise mode `organizations` is accepted as an optional allowlist, but
the current collector exposes the enterprise as its single request scope; it
does not fan out to that list.

`collection.lookback_days` must be 1-90 and is used by local collection code.
The deployed pipeline uses its separate `trailing_days` parameter.
`collection.output_directory` is the local Bronze root, not OneLake.

## Fabric setup

Acquire capacity and create a workspace manually, or permit CLI creation:

```yaml
fabric:
  workspace_name: GitHub Copilot Metrics
  lakehouse_name: GitHubCopilotMetrics
  create_workspace: false
  capacity_id: null
```

- `create_workspace: false` requires the exact workspace name to exist.
- `create_workspace: true` allows creation. Set `capacity_id` when tenant
  policy requires assignment. The CLI does not acquire capacity.
- Duplicate workspace/item display names are rejected.
- A new Lakehouse is created with `enableSchemas: true`; an existing one must
  already support schemas.

Configure the Environment deployment:

```yaml
fabric:
  environment_name: GitHubCopilotMetrics
  create_environment: false
```

- `create_environment: false` requires the exact Environment name to exist.
- `create_environment: true` allows creation in the target workspace.
- Apply builds a reproducible project wheel, places it under
  `Libraries/CustomLibraries`, writes runtime dependencies to
  `Libraries/PublicLibraries/environment.yml`, and uses conservative Runtime
  2.0 settings in `Setting/Sparkcompute.yml`.
- Definitions are content-hashed. Changed or interrupted definitions are
  updated and published through the release API (`beta=false`) with terminal
  state polling; unchanged published definitions are skipped.
- The deployer reads workspace Spark settings and PATCHes only the
  `environment` section, preserving pool, logging, concurrency, and job
  settings. Setting the default requires workspace Admin permission.

## Credentials

### Production Key Vault

The Bronze notebook calls
`notebookutils.mssparkutils.credentials.getSecret(key_vault_uri,
github_token_secret_name)`. The bootstrap service verifies Azure CLI 2.61+, requires an existing `az
login`, explicitly selects `azure.subscription_id`, creates or reuses the
configured resource group and RBAC-enabled vault, resolves either the signed-in
user or service-principal object ID, and reconciles **Key Vault Secrets
Officer** for the secret writer. It assigns **Key Vault Secrets User** to
`azure.fabric_runtime_principal_id` when configured, or to the current
bootstrap identity otherwise.

`bootstrap plan` performs discovery only. `bootstrap apply` prompts through
hidden input, validates the token against each configured organization metrics
endpoint (or the configured enterprise endpoint), and reconciles writes.

Secret storage never uses `az ... --value <secret>`. The adapter writes the
value to a restrictive, uniquely named file in its configured secure working
directory, passes only that path with `--file`, and overwrites/removes the file
in a `finally` block. Azure CLI invocations are argument arrays with
`shell=False`; typed actions and exceptions contain no token value. Configure
the adapter's `secret_directory` to a private directory not monitored or
persisted by automation.

When Fabric uses a different execution identity, configure its Entra object ID
as `azure.fabric_runtime_principal_id` so bootstrap grants the read role to the
correct principal. Pass only
`key_vault_uri=https://<vault>.vault.azure.net/` and
`github_token_secret_name=github-copilot-metrics-token` to the pipeline. The
effective run identity depends on Fabric authentication/workspace
configuration; confirm it with a non-production secret-read test. Rotate the
secret in Key Vault and never place its value in YAML, notebook defaults,
pipeline definitions, state, or logs.

### Local development

```powershell
$env:GITHUB_TOKEN = "<development token>"
az login
```

The Python client reads `GITHUB_TOKEN` at call time. `plan` and `deploy` use
`DefaultAzureCredential`, which can alternatively use managed identity,
workload identity, or service-principal environment variables. The deployed
Bronze notebook requires Key Vault parameters; it does not use the local
environment-token provider.

## Validate, plan, and deploy

From the repository root:

```powershell
ghcp-metrics validate --config config\config.yml; if ($LASTEXITCODE -eq 0) { ghcp-metrics plan --config config\config.yml }; if ($LASTEXITCODE -eq 0) { ghcp-metrics deploy --config config\config.yml }
```

`validate` is local and unauthenticated. `plan` performs authenticated GET-only
discovery and never builds a wheel or writes state. `deploy` creates/updates
items in dependency order.
`deploy --dry-run` is equivalent to `plan`. The credential-free state file
stores item IDs, definition hashes, and the last published Environment hash;
it never stores credentials. Preserve it for skip/resume behavior.

For end-to-end setup:

```powershell
ghcp-metrics bootstrap plan --config config\config.yml
ghcp-metrics bootstrap apply --config config\config.yml
ghcp-metrics bootstrap status --config config\config.yml
ghcp-metrics bootstrap resume --config config\config.yml
```

Installed CLI wheels include the deployment assets, so execution does not
depend on the repository being the current directory. Source execution uses
`python -m build`; installed execution can assemble a deterministic wheel from
the installed package.

### Manual fallback

If REST deployment is disallowed:

1. Create a schema-enabled Lakehouse with the configured name.
2. Import all three `fabric\notebooks\*.ipynb` files and set that Lakehouse as
   each default Lakehouse.
3. Create and publish a Fabric Environment containing the built wheel, public
   dependencies, and Spark compute settings, then set it as workspace default.
4. Create/import the pipeline from
   `fabric\pipelines\copilot_metrics_orchestration.DataPipeline\pipeline-content.json`
   and replace workspace/notebook IDs.
5. Open
   `assets\powerbi\GitHubCopilotMetrics\GitHubCopilotMetrics.pbip`, replace
   workspace/lakehouse placeholders in
   `GitHubCopilotMetrics.SemanticModel\definition\expressions.tmdl`, then
   publish the model and report.

Preserve the source parameters and Bronze -> Silver -> Gold Succeeded
dependencies.

## Initial backfill and daily schedule

Version 1 configuration files can record the intended bootstrap actions:

```yaml
backfill:
  enabled: true
  start_date: 2026-09-01
  end_date: 2026-09-30
schedule:
  enabled: true
  time: "02:00"
  timezone: Pacific Standard Time
  trailing_days: 28
```

`bootstrap apply` uses these settings to run the initial backfill and reconcile
the daily schedule. The submitted backfill signature is stored in
`.ghcp-job-state.json`, so repeated apply operations don't rerun an unchanged
historical range.

Run the Fabric pipeline with:

```text
run_mode=backfill
scope_kind=organization          # or enterprise
scope_slug=<slug>
start_date=2026-09-01
end_date=2026-09-30
key_vault_uri=https://<vault>.vault.azure.net/
github_token_secret_name=github-copilot-metrics-token
```

Dates are inclusive. Backfill smaller ranges if throttled. For manual
operation, create one Fabric schedule per scope:

```text
run_mode=daily
scope_kind=<organization|enterprise>
scope_slug=<slug>
trailing_days=28
```

Daily mode processes UTC today minus `trailing_days` through yesterday,
inclusive. The trailing window captures late corrections through deterministic
Silver/Gold replacement. The low-level `deploy` command doesn't create a
schedule; `bootstrap apply` creates or reconciles the managed schedule.
