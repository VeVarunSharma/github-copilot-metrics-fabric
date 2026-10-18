# Contributing

Thank you for improving this project.

## Development setup

1. Fork and clone the repository.
2. Create a virtual environment.
3. Run `python -m pip install -e ".[dev]"`.
4. Create a focused branch and make your changes.
5. Run `ghcp-metrics validate --config config\config.example.yml`,
   `python -m pytest`, `python -m ruff check .`, and `python -m build`.
6. Open a pull request describing the behavior and validation performed.

Keep responsibilities separated: Python collection code belongs under `src/`,
Fabric implementation under `fabric/`, and BI artifacts under
`assets/powerbi/`. Never commit access tokens, secrets, customer data, generated
metric exports, or customer-specific identifiers.

Documentation changes must keep commands and paths executable from the
repository root. Contract changes require the corresponding Silver or Gold
dictionary and affected TMDL/report assets to be updated. Notebook or pipeline
changes require the deployment and operations guides to be reviewed.

## Readiness and evidence standards

- Separate **implemented assets**, **locally validated behavior**, **synthetic
  demo**, and **live verified deployment** in every change description. Record
  commands actually run and skipped/blocked checks; never manufacture evidence.
- Documentation-only changes require local link, command, and consistency
  review, not code tests unless the documentation has dedicated tests. For
  code changes, use the smallest applicable existing validation/tests/lint/build
  commands; do not add tooling solely to validate prose.
- Synthetic loader output is Gold-only with scope `synthetic-demo` and demo
  ownership metadata. It bypasses Bronze/Silver and cannot prove end-to-end
  ingestion, live freshness, or production governance.
- Follow the [demo scripts and capture checklist](docs/powerbi-assets.md#demo-scripts).
  Screenshots/video must be actually captured, reviewed, synthetic-labeled or
  explicitly authorized, and sanitized. Do not reinstate unverified remote
  screenshots. See [reuse/anonymization](docs/powerbi-assets.md#reuse-and-anonymization).
- Keep absent capabilities visible: standard notebook primary-team mapping is
  absent, RLS is inactive, and renewable GitHub App runtime is not implemented.
  The current catalog URL returns GitHub 404, so catalog schema conformance is
  unverified; do not infer it from a locally valid metadata file.
- Use the [release gates](docs/deployment-guide.md#release-and-pilot-acceptance-gates)
  and [handoff responsibilities](docs/architecture-operations.md#support-ownership-and-handoff).
  Assign real reviewers/owners before claiming demo, pilot, or operational
  readiness. Repository contributors are not automatically a support team.

Integration tests require explicit opt-in and real credentials; never include
their output or collected data in a pull request. Use synthetic fixtures.

By participating, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
