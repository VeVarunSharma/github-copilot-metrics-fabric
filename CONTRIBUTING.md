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

Integration tests require explicit opt-in and real credentials; never include
their output or collected data in a pull request. Use synthetic fixtures.

By participating, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
