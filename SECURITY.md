# Security Policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use the repository's
GitHub **Security** tab to submit a private vulnerability report. If private
reporting is not enabled on a fork, contact that fork's maintainers privately.

Include a concise description, reproduction steps, affected versions, and the
potential impact. Maintainers should acknowledge reports promptly and avoid
disclosing details until a fix is available.

Do not attach real tokens, signed report URLs, customer exports, workspace
IDs, user logins, or team membership to a public issue or pull request.
Provide only sanitized evidence through the private report and rotate any
credential that may have been exposed.

## Secrets and data

Configuration files in this project are intentionally non-secret. Credentials
must come from runtime environment variables, managed identity, or an approved
secret store. Do not commit GitHub tokens, Microsoft Entra credentials, tenant
or workspace IDs, customer identifiers, or collected usage data.

`.fabric-deploy-state.json` is credential-free but contains Fabric resource
IDs and must remain uncommitted. Bronze, Silver, and Gold data is potentially
sensitive workforce telemetry and must follow the operator's retention,
access-control, and incident-response policies.
