from copilot_metrics_fabric import cli
from copilot_metrics_fabric.cli import main
from copilot_metrics_fabric.config import load_config


def test_validate_config_command(capsys) -> None:
    result = main(
        ["validate-config", "--config", "config/config.example.yml"]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert "Configuration is valid" in captured.out
    assert captured.err == ""


def test_validate_config_command_reports_errors(tmp_path, capsys) -> None:
    path = tmp_path / "config.yml"
    path.write_text("schema_version: 2\n", encoding="utf-8")

    result = main(["validate-config", "--config", str(path)])

    captured = capsys.readouterr()
    assert result == 2
    assert "Configuration error" in captured.err


def test_validate_deployment_assets(capsys) -> None:
    result = main(["validate", "--config", "config/config.example.yml"])

    captured = capsys.readouterr()
    assert result == 0
    assert "deployment assets are valid" in captured.out


def test_deploy_dry_run_uses_read_only_plan(monkeypatch, capsys) -> None:
    calls = []

    class FakeDeployer:
        def __init__(self, config, *, root, client, **kwargs):
            calls.append(("init", config, root, client, kwargs))

        def plan(self):
            calls.append(("plan",))
            return []

        def deploy(self):
            raise AssertionError("dry-run must not deploy")

    monkeypatch.setattr(cli, "FabricClient", lambda: object())
    monkeypatch.setattr(cli, "FabricDeployer", FakeDeployer)

    result = main(
        [
            "deploy",
            "--config",
            "config/config.example.yml",
            "--dry-run",
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert [call[0] for call in calls] == ["init", "plan"]
    assert "Deployment plan (no writes)" in captured.out


def test_init_defaults_organization_flow(tmp_path, capsys) -> None:
    output = tmp_path / "organization.yml"

    result = main(
        [
            "init",
            "--defaults",
            "--organization",
            "contoso",
            "--output",
            str(output),
        ]
    )

    assert result == 0
    assert load_config(output).github.organizations == ("contoso",)
    assert "Wrote non-secret organization" in capsys.readouterr().out
    content = output.read_text(encoding="utf-8").lower()
    assert "github_token_secret_name" in content
    assert "token:" not in content
    assert "\npassword:" not in content


def test_init_defaults_enterprise_flow(tmp_path) -> None:
    output = tmp_path / "enterprise.yml"

    result = main(
        [
            "init",
            "--defaults",
            "--enterprise",
            "contoso-enterprise",
            "--output",
            str(output),
        ]
    )

    config = load_config(output)
    assert result == 0
    assert config.github.mode == "enterprise"
    assert config.github.enterprise == "contoso-enterprise"


def test_init_interactive_organization_flow(
    tmp_path, monkeypatch
) -> None:
    output = tmp_path / "interactive.yml"
    answers = iter(
        [
            "organization",
            "contoso,fabrikam",
            "00000000-0000-0000-0000-000000000000",
            "metrics-rg",
            "",
            "yes",
            "metrics-kv-123",
            "yes",
            "",
            "GitHub Copilot Metrics",
            "yes",
            "",
            "Metrics Environment",
            "yes",
            "",
            "",
            "",
            "",
            "",
        ]
    )
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    result = main(["init", "--output", str(output)])

    config = load_config(output)
    assert result == 0
    assert config.github.organizations == ("contoso", "fabrikam")
    assert config.azure.resource_group_name == "metrics-rg"
    assert config.azure.key_vault_name == "metrics-kv-123"
    assert config.fabric.create_workspace is True
    assert config.schedule.enabled is True


def test_init_protects_existing_file_and_force_overwrites(
    tmp_path, capsys
) -> None:
    output = tmp_path / "config.yml"
    output.write_text("keep me", encoding="utf-8")

    result = main(["init", "--defaults", "--output", str(output)])

    assert result == 2
    assert output.read_text(encoding="utf-8") == "keep me"
    assert "use --force" in capsys.readouterr().err

    result = main(
        ["init", "--defaults", "--force", "--output", str(output)]
    )
    assert result == 0
    assert load_config(output).schema_version == 1


def test_init_rejects_invalid_scope_slug_without_writing(tmp_path) -> None:
    output = tmp_path / "config.yml"

    result = main(
        [
            "init",
            "--defaults",
            "--organization",
            "not a slug",
            "--output",
            str(output),
        ]
    )

    assert result == 2
    assert not output.exists()
