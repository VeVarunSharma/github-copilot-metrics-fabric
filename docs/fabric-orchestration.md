# Fabric orchestration assets

The repository contains deployable definitions for a schema-enabled Fabric
lakehouse:

- `fabric/notebooks/ingest_bronze.ipynb`
- `fabric/notebooks/build_silver.ipynb`
- `fabric/notebooks/build_gold.ipynb`
- `fabric/pipelines/copilot_metrics_orchestration.DataPipeline/`

## Deployment expectations

Create the lakehouse with schemas enabled
(`creationPayload.enableSchemas: true`) and bind all three notebooks to it
during deployment. The notebooks create and use `silver`, `gold`, and `audit`
schemas; the raw Bronze payloads remain immutable files under
`Files/bronze`. A non-schema-enabled lakehouse is unsupported because the
existing Spark adapters use schema-qualified Delta tables.

Attach a Fabric Environment containing this project wheel and its runtime
dependencies to each notebook. The deployment CLI resolves the pipeline's
workspace and notebook parameters from item display names. No workspace,
lakehouse, tenant, customer, or item IDs are committed here.

The CLI does not create or attach the Environment. Build the wheel with
`python -m build`, upload it to an Environment, and attach it to all three
notebooks after deployment.

## Deployment CLI

Authenticate with any credential supported by Azure Identity
(`az login`, managed identity, workload identity, or service principal), then
run:

```powershell
ghcp-metrics validate --config config\config.yml
ghcp-metrics plan --config config\config.yml
ghcp-metrics deploy --config config\config.yml --dry-run
ghcp-metrics deploy --config config\config.yml
```

`validate` checks notebook nbformat requirements, code-cell output fields,
pipeline references, TMDL/PBISM content, and PBIR-to-model references without
authenticating or calling Fabric. `plan` and `deploy --dry-run` authenticate
and discover the workspace and every item by exact display name and type, but
send only `GET` requests. A missing workspace is shown as a planned creation
only when `fabric.create_workspace` is enabled.

`deploy` creates or updates, in dependency order:

1. The configured workspace, when it is missing and creation is enabled.
2. One schema-enabled lakehouse (`creationPayload.enableSchemas: true`).
3. Bronze, Silver, and Gold notebooks with a default lakehouse binding.
4. The data pipeline with resolved workspace and notebook IDs.
5. The Direct Lake semantic model with workspace/lakehouse placeholders
   resolved.
6. The PBIR report with a live reference to the deployed semantic model.

The CLI polls Fabric long-running operations, honors `Retry-After`, retries
transient HTTP/network failures, and returns request IDs with sanitized error
messages. Every Fabric request, including retries and LRO polls, carries
`x-ms-fabric-skill: e2e-medallion-architecture`.

Successful definition updates are recorded in the configured state file
(default `.fabric-deploy-state.json`). A retry discovers the live items again
and skips definitions whose item ID and content hash already match the saved
state. The state file contains resource IDs and hashes only, never credentials.
Delete it to force definition updates. Keep it out of source control.

Workspace and item display names must be unique within their discovery scope.
The CLI fails explicitly on duplicate names rather than choosing an arbitrary
target. Workspace creation may require `fabric.capacity_id`, depending on
tenant policy.

## Runtime parameters

`run_mode=daily` replaces the trailing Gold window ending yesterday. Set
`trailing_days` to the desired correction window. `run_mode=backfill` requires
inclusive ISO `start_date` and `end_date` values for the Gold replacement
window. The pipeline derives a separate Bronze/Silver ingestion start by
subtracting `calculation_lookback_days`; Gold continues to receive and replace
only the requested window. Supply `scope_kind`, `scope_slug`, and `entity_type`
per customer/run.

| Parameter | Default | Purpose |
| --- | --- | --- |
| `run_mode` | `daily` | `daily` or `backfill` |
| `scope_kind` | `organization` | `organization` or `enterprise` |
| `scope_slug` | empty | Required GitHub slug; one scope per run |
| `entity_type` | `copilot_usage` | Audit classification passed through all stages |
| `report_types` | all four | Comma-separated Bronze reports |
| `trailing_days` | `28` | Daily inclusive Gold replacement window ending yesterday |
| `calculation_lookback_days` | `27` | Additional Bronze/Silver history required before the Gold start |
| `start_date`, `end_date` | empty | Required inclusive Gold replacement dates for backfill |
| `earliest_date` | empty | Optional ISO floor for Bronze/Silver ingestion when GitHub history starts later |
| `lakehouse_files_root` | `/lakehouse/default/Files` | Default Lakehouse Files root |
| `bronze_folder` | `bronze` | Bronze folder below the files root |
| `silver_schema`, `gold_schema`, `audit_schema` | `silver`, `gold`, `audit` | Schema names |
| `telemetry_lag_days` | `2` | Freshness grace period |
| `key_vault_uri`, `github_token_secret_name` | empty | Required Bronze secret lookup inputs |

Store the GitHub token in Azure Key Vault. Pass only `key_vault_uri` and
`github_token_secret_name`; grant the effective Fabric run identity secret
access. The pipeline marks Bronze inputs and outputs secure, and notebooks
never persist the token or signed download URLs. See
[the production credential setup](deployment-guide.md#production-key-vault).

## Execution and audit behavior

The pipeline runs Bronze, Silver, then Gold with `Succeeded` dependencies and
bounded retries. Bronze streams HTTPS report content into lakehouse Files
before its first Spark read. Every notebook appends a success or failure row to
`audit.pipeline_run_results`, including its run ID, effective scope/date
window, validation status, and sanitized JSON details. Exceptions are
re-raised after failure audit writes so Fabric marks the activity and pipeline
failed.

`FabricJobBootstrap` uses the current Core Job Scheduler APIs to plan or apply
an initial `run_mode=backfill` invocation and one managed `run_mode=daily`
schedule per configured scope. Planning performs discovery-only GET requests.
Apply passes typed pipeline parameters, can either poll each backfill to a
terminal status or return its job instance ID, and reconciles only schedules
marked `github-copilot-metrics-fabric/bootstrap-jobs/v1`. Other schedules are
left unchanged. Multiple managed schedules for one scope are treated as an
ambiguity and require operator cleanup.

All job, retry, polling, and schedule calls include the Fabric telemetry
header. Polling honors `Retry-After`; failed and cancelled job details are
sanitized before they are raised. Capacity acquisition, Key Vault
provisioning, Environment attachment, alerts, and primary-team mapping remain
outside this implementation.

Silver and Gold bounded outputs are replaced atomically with Delta `MERGE`,
including deletion of rows removed by corrections. Bronze and Silver receive
the requested Gold start minus 27 days (or `earliest_date`, when that floor is
later), so an initial January 28 backfill starts ingestion on January 1. Gold
still receives January 28 as its replacement start, reads the preceding
Silver history for rolling calculations, and emits only the requested daily
window. Bronze reuses an existing successful or no-data manifest for
lookback-only dates before the Gold replacement start; requested Gold dates
are refreshed so late corrections remain discoverable. Its manifest scan uses
`bronze_folder`, and freshness expects only the configured `report_types`.
`user_adoption_current` is recomputed from the complete
Silver user history through the latest reporting day for the scope. Historical
backfills therefore preserve true current state.
