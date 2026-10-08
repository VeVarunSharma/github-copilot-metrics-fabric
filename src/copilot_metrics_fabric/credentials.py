"""Credential providers that keep secrets outside configuration files."""

from __future__ import annotations

import os
from collections.abc import Mapping

from copilot_metrics_fabric.github_client import CredentialError


class EnvironmentTokenProvider:
    """Read a GitHub token at call time without retaining or displaying it."""

    def __init__(
        self,
        environment: Mapping[str, str] | None = None,
        variable: str = "GITHUB_TOKEN",
    ) -> None:
        self._environment = environment if environment is not None else os.environ
        self._variable = variable

    def __call__(self) -> str:
        token = self._environment.get(self._variable, "")
        if not token.strip():
            raise CredentialError(
                f"required credential environment variable {self._variable} is not set"
            )
        return token
