import base64
import importlib.util
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

SPEC = importlib.util.spec_from_file_location(
    "load_demo_gold", Path(__file__).parents[1] / "scripts/load_demo_gold.py"
)
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)
ARGS = [
    "--workspace-id",
    "00000000-0000-0000-0000-000000000001",
    "--lakehouse-id",
    "00000000-0000-0000-0000-000000000002",
    "--lakehouse-name",
    "SyntheticDemo",
]


def spark_with(tables, owner=None, foreign_rows=0):
    spark = Mock()

    def sql(query):
        if query == "SHOW SCHEMAS":
            rows = [(name,) for name in {schema for schema, _ in tables}]
        else:
            schema = query.removeprefix("SHOW TABLES IN `").removesuffix("`")
            rows = [
                SimpleNamespace(
                    asDict=lambda name=name: {"tableName": name, "isTemporary": False}
                )
                for db, name in tables
                if db == schema
            ]
        return SimpleNamespace(collect=lambda: rows)

    spark.sql.side_effect = sql
    spark.table.return_value.collect.return_value = [
        SimpleNamespace(asDict=lambda: {"demo_owner": owner})
    ]
    filtered = spark.table.return_value.where.return_value.limit.return_value
    filtered.count.return_value = foreign_rows
    return spark


def test_offline_plan_has_no_client(monkeypatch, capsys):
    client = Mock()
    monkeypatch.setattr(demo, "FabricClient", client)
    assert demo.main(ARGS) == 0
    client.assert_not_called()
    assert "Plan only" in capsys.readouterr().out


@pytest.mark.parametrize(
    "extra",
    [
        ["--yes"],
        ["--end-date", "2019-01-01"],
        ["--start-date", "2026-10-07"],
        ["--start-date", "2020-01-01"],
        ["--workspace-id", "../../other"],
    ],
)
def test_invalid_or_unconfirmed_apply_never_creates_client(monkeypatch, extra):
    client = Mock()
    monkeypatch.setattr(demo, "FabricClient", client)
    with pytest.raises(SystemExit):
        demo.main(ARGS + extra)
    client.assert_not_called()


def test_generated_notebook_is_valid_and_reproducible():
    code = demo.notebook_code(date(2026, 1, 1), date(2026, 1, 31))
    compile(code, "<demo-notebook>", "exec")
    assert code == demo.notebook_code(date(2026, 1, 1), date(2026, 1, 31))
    assert "2026-01-31" in code
    assert "synthetic-demo" in code
    assert code.index("validate_demo_target(spark, set(frames)") < code.index(
        'frame.write.format("delta")'
    )


@pytest.mark.parametrize(
    "tables,owner,foreign_rows",
    [
        ([("silver", "user_daily")], None, 0),
        ([("gold", "entity_adoption_daily")], None, 0),
        ([("gold", "demo_metadata")], "another-loader", 0),
        (
            [("gold", "demo_metadata"), ("gold", "entity_adoption_daily")],
            "github-copilot-metrics-fabric/synthetic-demo/v1",
            1,
        ),
    ],
)
def test_existing_non_demo_tables_are_refused(tables, owner, foreign_rows):
    with pytest.raises(RuntimeError):
        demo.validate_demo_target(
            spark_with(tables, owner, foreign_rows),
            {"entity_adoption_daily", "demo_metadata"},
        )


def test_empty_lakehouse_is_allowed():
    demo.validate_demo_target(spark_with([]), {"demo_metadata"})


def test_owned_demo_rerun_is_allowed():
    demo.validate_demo_target(
        spark_with(
            [("gold", "demo_metadata"), ("gold", "entity_adoption_daily")],
            "github-copilot-metrics-fabric/synthetic-demo/v1",
        ),
        {"entity_adoption_daily", "demo_metadata"},
    )


@pytest.mark.parametrize("status", ["InProgress", "NotStarted", "Unknown"])
def test_active_jobs_block_notebook_update(monkeypatch, status):
    client = Mock()
    client.request.side_effect = [
        {"displayName": "SyntheticDemo"},
        {"value": [{"displayName": demo.NOTEBOOK_NAME, "id": "notebook"}]},
        {"value": [{"status": status}]},
    ]
    monkeypatch.setattr(demo, "FabricClient", Mock(return_value=client))
    with pytest.raises(RuntimeError, match="job state"):
        demo.main(ARGS + ["--yes", "--confirm-isolated-demo"])
    assert all(call.args[0] == "GET" for call in client.request.call_args_list)
    client.request_response.assert_not_called()


def test_confirmed_apply_binds_explicit_target_and_labels_data(monkeypatch):
    client = Mock()
    client.request.side_effect = [
        {"displayName": "SyntheticDemo"},
        {"value": []},
        {"id": "notebook"},
        {},
    ]
    client.request_response.return_value.headers = {"Location": "job-location"}
    client.wait_for_job.return_value = {"status": "Completed"}
    monkeypatch.setattr(demo, "FabricClient", Mock(return_value=client))
    assert demo.main(ARGS + ["--yes", "--confirm-isolated-demo"]) == 0
    definition = client.request.call_args_list[-1].kwargs["body"]["definition"]
    notebook = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
    binding = notebook["metadata"]["dependencies"]["lakehouse"]
    assert binding["default_lakehouse"] == ARGS[3]
    assert "Synthetic demonstration" in "".join(notebook["cells"][0]["source"])
    assert notebook["cells"][1]["outputs"] == []


def test_wrong_target_name_prevents_writes(monkeypatch):
    client = Mock()
    client.request.return_value = {"displayName": "LiveTelemetry"}
    monkeypatch.setattr(demo, "FabricClient", Mock(return_value=client))
    with pytest.raises(RuntimeError, match="does not match"):
        demo.main(ARGS + ["--yes", "--confirm-isolated-demo"])
    assert client.request.call_count == 1


def test_paginated_notebooks_fail_closed():
    client = Mock()
    client.request.return_value = {"value": [], "continuationToken": "next"}
    with pytest.raises(RuntimeError, match="paginated"):
        demo.list_items(client, ARGS[1], "Notebook")
