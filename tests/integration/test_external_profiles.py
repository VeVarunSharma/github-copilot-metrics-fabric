import os
from datetime import date, timedelta

import pytest

from copilot_metrics_fabric.config import GitHubConfig
from copilot_metrics_fabric.deployment import FabricClient
from copilot_metrics_fabric.github_client import (
    GitHubCopilotClient,
    ReportType,
)

pytestmark = pytest.mark.integration


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        pytest.skip(f"{name} is required for the integration profile")
    return value


def test_github_credential_can_read_copilot_metrics_endpoint():
    token = _required("GITHUB_TOKEN")
    organization = _required("GHCP_GITHUB_ORGANIZATION")
    client = GitHubCopilotClient(
        GitHubConfig("organization", (organization,)),
        lambda: token,
    )
    report_day = date.today() - timedelta(days=2)

    result = client.get_daily_report(
        client.scopes[0],
        ReportType.USERS,
        report_day,
    )

    assert result is None or result.report_day == report_day


def test_fabric_credential_can_discover_named_workspace():
    workspace_name = _required("GHCP_FABRIC_WORKSPACE_NAME")
    payload = FabricClient().request("GET", "workspaces", expected=(200,))

    assert isinstance(payload, dict)
    assert any(
        item.get("displayName") == workspace_name
        for item in payload.get("value", [])
        if isinstance(item, dict)
    )
