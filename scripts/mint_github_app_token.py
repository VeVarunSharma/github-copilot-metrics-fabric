"""Mint a short-lived GitHub App installation token from environment values."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import jwt
import requests


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> int:
    try:
        app_id = required("GITHUB_APP_ID")
        installation_id = required("GITHUB_APP_INSTALLATION_ID")
        private_key = Path(
            required("GITHUB_APP_PRIVATE_KEY_PATH")
        ).read_text(encoding="utf-8")
        now = int(time.time())
        app_jwt = jwt.encode(
            {
                "iat": now - 60,
                "exp": now + 540,
                "iss": app_id,
            },
            private_key,
            algorithm="RS256",
        )
        response = requests.post(
            (
                "https://api.github.com/app/installations/"
                f"{installation_id}/access_tokens"
            ),
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {app_jwt}",
                "X-GitHub-Api-Version": "2026-03-10",
                "User-Agent": "github-copilot-metrics-fabric",
            },
            timeout=30,
        )
        response.raise_for_status()
        token = response.json().get("token")
        if not isinstance(token, str) or not token:
            raise RuntimeError("GitHub returned no installation token")
    except Exception as error:
        print(
            f"GitHub App authentication failed: {type(error).__name__}",
            file=sys.stderr,
        )
        return 2
    print(token, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
