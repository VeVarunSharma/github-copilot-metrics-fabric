import os

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run credential-gated tests that contact GitHub and Fabric",
    )


def pytest_collection_modifyitems(config, items):
    enabled = (
        config.getoption("--run-integration")
        and os.environ.get("GHCP_RUN_INTEGRATION") == "1"
    )
    if enabled:
        return
    skip = pytest.mark.skip(
        reason=(
            "integration tests require --run-integration and "
            "GHCP_RUN_INTEGRATION=1"
        )
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
