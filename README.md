# GitHub Copilot Metrics for Microsoft Fabric

An open-source, forkable starter for collecting GitHub Copilot usage metrics and
building an analytics solution in Microsoft Fabric.

> [!IMPORTANT]
> This repository provides reusable Bronze, Silver, and Gold contracts,
> deployable Fabric notebook and pipeline definitions, Power BI project
> assets, and an idempotent deployment CLI.

## Documentation

- [One-command setup](docs/bootstrap-guide.md)
- [Fork, configure, and deploy](docs/deployment-guide.md)
- [How the repository works](docs/how-it-works.md)
- [Architecture, governance, operations, and troubleshooting](docs/architecture-operations.md)
- [Fabric orchestration reference](docs/fabric-orchestration.md)
- [Silver data dictionary](docs/silver-data-dictionary.md)
- [Gold data dictionary and metric semantics](docs/gold-data-dictionary.md)
- [Power BI assets, measures, and RLS](docs/powerbi-assets.md)
- [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

## Architecture

The implemented solution separates responsibilities so each layer can evolve
without embedding credentials or environment-specific identifiers in source:

![Repository architecture](docs/diagrams/github-copilot-metrics-architecture.svg)

The diagram is also available as
[PNG](docs/diagrams/github-copilot-metrics-architecture.png) and
[editable Mermaid source](docs/diagrams/github-copilot-metrics-architecture.mmd).
See [How the repository works](docs/how-it-works.md) for the detailed
deployment, security, data-flow, and operational model.

```text
GitHub Copilot metrics APIs
            |
     Python collector
            |
  versioned raw metric files
            |
 Fabric ingestion and transformation
            |
 Lakehouse / semantic model / Power BI
```

Repository areas:

| Path | Responsibility |
| --- | --- |
| `src/copilot_metrics_fabric/` | Python package, CLI, and configuration |
| `config/` | Safe, non-secret configuration examples |
| `fabric/notebooks/` | Parameterized Fabric notebook definitions |
| `fabric/pipelines/` | Bronze-to-Silver-to-Gold pipeline definition |
| `assets/powerbi/` | TMDL semantic model and PBIR report assets |
| `tests/` | Focused Python tests |
| `docs/` | Deployment, architecture, operations, and data dictionaries |

Authentication is deliberately outside the YAML contract. The default
`EnvironmentTokenProvider` reads `GITHUB_TOKEN` at request time. Applications
can inject another callable backed by managed identity or a secret store.
Tokens and complete signed report URLs must never be logged or persisted.

## One-command quickstart

Prerequisites: Python 3.10+, Git, Azure CLI, a Fabric capacity, and permission
to manage the selected Azure subscription and Fabric workspace. Run `az login`
first.

Windows:

```powershell
.\scripts\setup.ps1
```

macOS or Linux:

```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

The setup command creates a virtual environment, installs the project, starts
the non-secret configuration wizard, validates prerequisites, shows a
write-free plan, asks for confirmation, securely prompts for the GitHub token,
creates or reuses Azure and Fabric resources, publishes the Fabric
Environment, deploys the solution, runs the initial backfill, and reconciles
the daily schedule.

The GitHub token is entered through hidden input and written directly to Azure
Key Vault. It is never written to YAML, command arguments, bootstrap state, or
logs.

Follow-up commands:

```powershell
ghcp-metrics bootstrap status --config config\config.yml
ghcp-metrics bootstrap resume --config config\config.yml
ghcp-metrics bootstrap plan --config config\config.yml --json
```

See the [one-command setup guide](docs/bootstrap-guide.md) for permissions,
CI usage, troubleshooting, and safe removal. The
[deployment guide](docs/deployment-guide.md) remains the advanced/manual path.

## Configuration

The root configuration has seven sections:

- `schema_version`: contract version; currently `1`.
- `github`: either `organization` or `enterprise` mode.
- `collection`: portable collection-window and output preferences.
- `azure`: Azure subscription, resource group, location, and Key Vault
  create-or-reuse preferences (identifiers only).
- `fabric`: exact deployment target names and credential-free resume state.
- `backfill`: optional inclusive initial backfill dates.
- `schedule`: intended daily time, Fabric/Windows time-zone ID, and trailing
  window.

Unknown keys are rejected to catch misspellings and prevent accidental secret
fields from being accepted. Existing version 1 files remain valid because all
new sections and fields have explicit defaults. See
[`config/config.example.yml`](config/config.example.yml) for the complete
contract. Schedule time zones are validated against the maintained Microsoft
list of Windows time-zone IDs accepted by Fabric; examples include `UTC`,
`Pacific Standard Time`, `UTC-11`, and `UTC+12`. IANA names and arbitrary
values ending in `Standard Time` are rejected.

## GitHub report client

`GitHubCopilotClient` resolves the configured organization or enterprise scope
for entity, user, user-team, and repository one-day reports. It validates
GitHub's metadata response and signed HTTPS URLs, applies bounded retries, and
streams NDJSON to a caller-provided binary destination while calculating a
SHA-256 content hash. Usage-report requests send the supported
`X-GitHub-Api-Version: 2026-03-10` header.

```python
from datetime import date

from copilot_metrics_fabric.config import load_config
from copilot_metrics_fabric.credentials import EnvironmentTokenProvider
from copilot_metrics_fabric.github_client import GitHubCopilotClient, ReportType

config = load_config("config/config.yml")
client = GitHubCopilotClient(config.github, EnvironmentTokenProvider())
metadata = client.get_daily_report(
    client.scopes[0], ReportType.USERS, date(2025, 10, 13)
)
```

The client returns `None` for a `204 No Content` response.

## Bronze ingestion

`BronzeIngestor` retrieves metadata and immediately downloads every expiring
NDJSON link. It writes immutable files under scope, `report_type`,
`report_day`, and `ingestion_id` partitions, plus credential-free manifests
and per-attempt audit records. Exact content reruns are deduplicated by SHA-256;
changed late-arriving telemetry is retained as a new ingestion. Pipeline runs
may reuse already-complete manifests for lookback-only dates before the Gold
replacement window, while still refreshing dates whose Gold outputs are being
replaced.

```python
from datetime import date

from copilot_metrics_fabric.bronze import (
    BronzeIngestor,
    LocalBronzeStorage,
    report_days,
)

storage = LocalBronzeStorage(config.collection.output_directory)
ingestor = BronzeIngestor(client, storage)

# Default late-arrival window, ending yesterday:
days = report_days(trailing_days=config.collection.lookback_days)
results = ingestor.ingest(days)

# Or an explicit, inclusive historical backfill:
backfill = report_days(
    start_day=date(2025, 9, 1),
    end_day=date(2025, 9, 30),
)
```

A `204 No Content` response creates a successful no-data manifest. A failure
while downloading a multi-file report removes staged files and writes only a
sanitized failure audit; other requests in the same batch continue, then
`BronzeBatchError` reports the failures and completed results.

`BronzeStorage` is the notebook-friendly storage protocol.
`LocalBronzeStorage` works with local paths and mounted OneLake paths, while
`MemoryBronzeStorage` supports tests and small interactive experiments. Custom
Fabric adapters can implement the same six-method protocol without changing
ingestion logic. Manifests and audits never include credentials, exception
messages, or complete signed download URLs.

## Silver normalization

`normalize_ndjson` and `normalize_records` convert entity, user, user-team, and
repository reports into explicit daily tables. Repeating feature, IDE,
language, model, agent, CLI, Copilot app, MCP, plugin, skill, slash-command,
adoption, and pull-request structures are expanded without requiring Spark.
Invalid records are quarantined, unknown fields are logged as schema drift, and
duplicate upsert keys resolve deterministically.

```python
from copilot_metrics_fabric.silver import SourceContext, normalize_ndjson

batch = normalize_ndjson(
    bronze_content,
    SourceContext(
        scope_kind="organization",
        scope_slug="example-org",
        report_type="users",
        report_day="2025-10-13",
        ingestion_id="01J...",
    ),
)
user_rows = batch.rows["user_daily"]
```

Fabric notebooks can use `silver_spark.create_dataframes` for explicit
`StructType` DataFrames and `silver_spark.merge_dataframes` for atomic Delta
snapshot replacement. Bounded replacements use one `MERGE` transaction that
upserts the new snapshot and removes corrected-away rows; they never
delete-then-append.
See [`docs/silver-data-dictionary.md`](docs/silver-data-dictionary.md) and
[`fabric/notebooks/build_silver.ipynb`](fabric/notebooks/build_silver.ipynb).

## Gold analytics

`build_gold` creates testable daily entity, team, repository, feature,
language, IDE, user-state, rolling adoption, and freshness outputs from Silver
rows. Rates use ratios of summed values, rolling users are distinct across the
window, and source-reported rolling values remain unsummed snapshots.

```python
from copilot_metrics_fabric.gold import GoldBuildOptions, build_gold

gold = build_gold(
    silver_rows,
    options=GoldBuildOptions(as_of_day="2025-10-15", telemetry_lag_days=2),
    report_status_rows=manifest_status_rows,
    primary_team_mapping={101: 42},
)
entity_daily = gold.rows["entity_adoption_daily"]
```

Many-to-many team rows are marked non-additive. An optional primary-team
mapping emits separate additive rows for executive rollups. Late corrections
replace older Silver keys deterministically. `user_adoption_current` is always
rebuilt from all available Silver user history through each scope's latest
reporting day, even when the requested daily output is a historical backfill.
See
[`docs/gold-data-dictionary.md`](docs/gold-data-dictionary.md) and
[`fabric/notebooks/build_gold.ipynb`](fabric/notebooks/build_gold.ipynb).

## CLI

```powershell
ghcp-metrics --version
ghcp-metrics validate-config --config config\config.yml
ghcp-metrics validate --config config\config.yml
ghcp-metrics plan --config config\config.yml
ghcp-metrics deploy --config config\config.yml --dry-run
ghcp-metrics deploy --config config\config.yml
```

`validate` performs local structural and cross-asset checks before any Fabric
request. `plan` and `deploy --dry-run` dynamically discover targets and perform
no writes. `deploy` uses Azure Identity and Fabric REST APIs to create or
update the configured schema-enabled lakehouse, bound notebooks, pipeline,
semantic model, and report. It retries transient failures, polls long-running
operations, and persists credential-free resume state. See
[`docs/fabric-orchestration.md`](docs/fabric-orchestration.md).

## Fabric orchestration

The parameterized Fabric pipeline supports daily trailing-window runs and
explicit date-range backfills. It runs Bronze, Silver, and Gold sequentially,
retries transient activity failures, and writes sanitized validation results to
an audit Delta table. Customer identifiers, Fabric item IDs, and secret values
remain deployment/runtime parameters. See
[`docs/fabric-orchestration.md`](docs/fabric-orchestration.md) for lakehouse,
environment, binding, scheduling, and Key Vault expectations.

The CLI does not create a capacity, Key Vault, Fabric Environment, pipeline
schedule, alert, RLS assignment, or primary-team mapping.

## Development

```powershell
python -m pytest --cov=copilot_metrics_fabric --cov-report=term-missing
python -m ruff check .
python -m build
```

Credential-gated integration tests are skipped unless explicitly enabled. They
never run as part of the default test command:

```powershell
$env:GHCP_RUN_INTEGRATION = "1"
$env:GITHUB_TOKEN = "<fine-grained token>"
$env:GHCP_GITHUB_ORGANIZATION = "<organization slug>"
$env:GHCP_FABRIC_WORKSPACE_NAME = "<workspace name>"
python -m pytest -m integration --run-integration
```

The GitHub token must read the named organization. Azure Identity
(`DefaultAzureCredential`) must be configured and able to list Fabric
workspaces; the named workspace is only read, never modified. Omit
`GHCP_RUN_INTEGRATION` or `--run-integration` to skip all external calls safely.

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before contributing.

## License

Licensed under the [MIT License](LICENSE).
