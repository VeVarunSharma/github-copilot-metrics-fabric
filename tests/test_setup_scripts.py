from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_powershell_setup_wraps_complete_bootstrap() -> None:
    content = (ROOT / "scripts/setup.ps1").read_text(encoding="utf-8")

    assert "python -m venv .venv" in content
    assert 'pip install -e ".[dev]"' in content
    assert "bootstrap plan --config" in content
    assert '"bootstrap", "apply"' in content
    assert "GITHUB_TOKEN" not in content


def test_posix_setup_wraps_complete_bootstrap() -> None:
    content = (ROOT / "scripts/setup.sh").read_text(encoding="utf-8")

    assert content.startswith("#!/usr/bin/env sh\n")
    assert "set -eu" in content
    assert "python3 -m venv .venv" in content
    assert 'pip install -e ".[dev]"' in content
    assert "bootstrap plan" in content
    assert "bootstrap apply" in content
    assert "GITHUB_TOKEN" not in content
