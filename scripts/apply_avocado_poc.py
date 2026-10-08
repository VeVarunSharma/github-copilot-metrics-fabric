"""Apply the staged Avocado POC deployment using GITHUB_TOKEN."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

from copilot_metrics_fabric.azure_bootstrap import AzureBootstrapService, AzureCli
from copilot_metrics_fabric.bootstrap_jobs import FabricJobBootstrap
from copilot_metrics_fabric.config import load_config
from copilot_metrics_fabric.deployment import (
    FabricClient,
    FabricDeployer,
    resolve_asset_root,
)


def main() -> int:
    config_path = Path("config/avocado-poc.yml")
    config = load_config(config_path)
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        raise RuntimeError("GITHUB_TOKEN was not supplied")

    root = resolve_asset_root(config_path)
    fabric_client = FabricClient()

    AzureBootstrapService(
        config,
        cli=AzureCli(secret_directory=root),
    ).apply(lambda: token)

    FabricDeployer(
        config,
        root=root,
        client=fabric_client,
        include_bi=False,
    ).deploy()

    poc_config = replace(
        config,
        schedule=replace(config.schedule, enabled=False),
    )
    FabricJobBootstrap(
        poc_config,
        root=root,
        client=fabric_client,
    ).apply()

    FabricDeployer(
        config,
        root=root,
        client=fabric_client,
        include_bi=True,
    ).deploy()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
