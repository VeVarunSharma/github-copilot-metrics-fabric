# Contributor safety and validation

- Keep collection code in `src/`, Fabric assets in `fabric/`, and BI assets in
  `assets/powerbi/`. Coordinate contract changes across data dictionaries,
  notebooks, Gold measures, TMDL, and PBIR.
- Use `ghcp-metrics validate --config config/config.example.yml`,
  `python -m pytest`, `python -m ruff check .`, and `python -m build`.
  Integration tests require explicit opt-in and approved credentials.
- Never commit secrets, customer telemetry, deployment identifiers, local state,
  notebook outputs, signed report URLs, or unreviewed customer screenshots.
- Use synthetic fixtures for tests. Keep demo execution isolated from live
  telemetry. Do not remove confirmation or ownership checks.
- Missing is not zero; rolling users are distinct, team memberships overlap,
  and ratios use summed numerators and denominators.
- Never describe synthetic Gold loading as verified ingestion or infer causal
  productivity, ROI, or individual performance from usage metrics.
- Preserve honest readiness gates: live deployment, RLS, token renewal,
  independent handoff, and catalog-schema validation require actual evidence.
