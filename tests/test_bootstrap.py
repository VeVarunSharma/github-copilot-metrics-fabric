from __future__ import annotations

from pathlib import Path

import pytest

from copilot_metrics_fabric import bootstrap as bootstrap_module
from copilot_metrics_fabric import cli
from copilot_metrics_fabric.azure_bootstrap import (
    ActionKind,
    BootstrapAction,
    ExecutionPrincipal,
)
from copilot_metrics_fabric.azure_bootstrap import (
    BootstrapPlan as AzurePlan,
)
from copilot_metrics_fabric.bootstrap import (
    BOOTSTRAP_STATE_FILE,
    BootstrapOrchestrator,
)
from copilot_metrics_fabric.bootstrap_jobs import (
    JobApplyResult,
    JobPlanAction,
    JobScope,
)
from copilot_metrics_fabric.config import load_config
from copilot_metrics_fabric.deployment import PlanAction

ROOT = Path(__file__).parents[1]
CONFIG = ROOT / "config/config.example.yml"


class FakeAzureService:
    applied = 0
    planned = 0
    tokens: list[str] = []

    def __init__(self, config, *, cli):
        pass

    def plan(self, token_provider=None):
        self.__class__.planned += 1
        assert token_provider is None
        return AzurePlan(
            (
                BootstrapAction(
                    ActionKind.CREATE, "KeyVault", "vault", write=True
                ),
            ),
            ExecutionPrincipal("principal", "User"),
            None,
        )

    def apply(self, token_provider):
        self.__class__.applied += 1
        self.__class__.tokens.append(token_provider())
        return self.plan()


class FakeDeployer:
    planned = 0
    deployed = 0

    def __init__(self, config, *, root, client, **kwargs):
        pass

    def plan(self):
        self.__class__.planned += 1
        return [PlanAction("create", "Lakehouse", "Lakehouse")]

    def deploy(self):
        self.__class__.deployed += 1
        return [PlanAction("create", "Lakehouse", "Lakehouse")]


class FakeJobs:
    planned = 0
    applied = 0
    fail_once = False
    rerun_failed_values: list[bool] = []

    def __init__(
        self,
        config,
        *,
        root,
        client,
        state_root=None,
        completion_store=None,
    ):
        pass

    def plan(self):
        self.__class__.planned += 1
        return [
            JobPlanAction(
                "create", "DailySchedule", JobScope("organization", "example")
            )
        ]

    def apply(self, *, rerun_failed=True):
        self.__class__.applied += 1
        self.__class__.rerun_failed_values.append(rerun_failed)
        if self.__class__.fail_once:
            self.__class__.fail_once = False
            raise RuntimeError("transient job failure")
        return JobApplyResult(tuple(self.plan()), {"organization:example": "job"})

    def status(self):
        return {"organization:example": {"status": "completed"}}


@pytest.fixture(autouse=True)
def reset_fakes(monkeypatch):
    FakeAzureService.applied = 0
    FakeAzureService.planned = 0
    FakeAzureService.tokens = []
    FakeDeployer.planned = 0
    FakeDeployer.deployed = 0
    FakeJobs.planned = 0
    FakeJobs.applied = 0
    FakeJobs.fail_once = False
    FakeJobs.rerun_failed_values = []
    monkeypatch.setattr(
        bootstrap_module, "AzureBootstrapService", FakeAzureService
    )
    monkeypatch.setattr(bootstrap_module, "FabricDeployer", FakeDeployer)
    monkeypatch.setattr(bootstrap_module, "FabricJobBootstrap", FakeJobs)


def orchestrator(tmp_path: Path) -> BootstrapOrchestrator:
    return BootstrapOrchestrator(
        load_config(CONFIG),
        root=ROOT,
        config_path=tmp_path / "config.yml",
        fabric_client=object(),
        azure_cli=object(),
        token_provider=lambda: "sensitive-token",
        state_root=tmp_path,
    )


def test_plan_is_read_only_and_json_serializable(tmp_path: Path) -> None:
    plan = orchestrator(tmp_path).plan()

    assert FakeAzureService.applied == 0
    assert FakeDeployer.deployed == 0
    assert FakeJobs.applied == 0
    assert plan.to_dict()["azure"][0]["resource_type"] == "KeyVault"


def test_apply_records_phases_without_secret(tmp_path: Path) -> None:
    runner = orchestrator(tmp_path)

    result = runner.apply()

    state = (tmp_path / BOOTSTRAP_STATE_FILE).read_text(encoding="utf-8")
    assert "sensitive-token" not in state
    assert '"azure"' in state and '"fabric"' in state and '"jobs"' in state
    assert result.jobs == {"organization:example": "job"}


def test_resume_skips_completed_phases_after_partial_failure(
    tmp_path: Path,
) -> None:
    FakeJobs.fail_once = True
    runner = orchestrator(tmp_path)

    with pytest.raises(RuntimeError, match="transient"):
        runner.apply()

    assert FakeAzureService.applied == 1
    assert FakeDeployer.deployed == 1
    runner.token_provider = lambda: pytest.fail("resume must not request token")
    runner.apply(resume=True)

    assert FakeAzureService.applied == 1
    assert FakeDeployer.deployed == 1
    assert FakeJobs.applied == 2
    assert FakeJobs.rerun_failed_values == [True, False]


def test_normal_apply_reconciles_all_completed_phases_again(
    tmp_path: Path,
) -> None:
    runner = orchestrator(tmp_path)

    runner.apply()
    runner.apply()

    assert FakeAzureService.applied == 2
    assert FakeDeployer.deployed == 2
    assert FakeJobs.applied == 2


def test_cli_apply_uses_hidden_token_prompt(monkeypatch, capsys) -> None:
    seen: dict[str, object] = {}

    class FakeOrchestrator:
        def __init__(self, config, *, root, config_path, **_kwargs):
            self.token_provider = None

        def plan(self):
            return bootstrap_module.BootstrapPlan((), (), ())

        def apply(self, *, resume=False):
            seen["token"] = self.token_provider()
            return bootstrap_module.BootstrapResult(
                bootstrap_module.BootstrapPlan((), (), ()), {}, {}
            )

    monkeypatch.setattr(cli, "BootstrapOrchestrator", FakeOrchestrator)
    monkeypatch.setattr(cli.getpass, "getpass", lambda _prompt: "hidden-token")

    result = cli.main(
        [
            "bootstrap",
            "apply",
            "--yes",
            "--config",
            str(CONFIG),
        ]
    )

    assert result == 0
    assert seen["token"] == "hidden-token"
    assert "hidden-token" not in capsys.readouterr().out
