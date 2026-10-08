import json
import re
from datetime import date
from pathlib import Path

from copilot_metrics_fabric.orchestration import (
    GOLD_END_EXPRESSION,
    GOLD_START_EXPRESSION,
    INGESTION_START_EXPRESSION,
    resolve_pipeline_bounds,
)

ROOT = Path(__file__).parents[1]
NOTEBOOKS = {
    "Bronze ingestion": ROOT / "fabric/notebooks/ingest_bronze.ipynb",
    "Silver normalization": ROOT / "fabric/notebooks/build_silver.ipynb",
    "Gold materialization": ROOT / "fabric/notebooks/build_gold.ipynb",
}
PIPELINE = (
    ROOT
    / "fabric/pipelines/copilot_metrics_orchestration.DataPipeline"
    / "pipeline-content.json"
)
COMMON_PARAMETERS = {
    "run_id",
    "run_mode",
    "scope_kind",
    "scope_slug",
    "entity_type",
    "start_date",
    "end_date",
    "lakehouse_files_root",
    "audit_schema",
}
GOLD_CONFIG_PARAMETERS = {"bronze_folder", "report_types"}


def _notebook(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _parameter_names(notebook):
    parameter_cells = [
        cell
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
        and "parameters" in cell.get("metadata", {}).get("tags", [])
    ]
    assert len(parameter_cells) == 1
    assignments = "".join(parameter_cells[0]["source"])
    return set(re.findall(r"^([a-z][a-z0-9_]*)\s*=", assignments, re.MULTILINE))


def test_notebooks_are_valid_parameterized_ipynb_definitions():
    for path in NOTEBOOKS.values():
        notebook = _notebook(path)
        assert notebook["nbformat"] == 4
        assert notebook["metadata"]["fabric"]["lakehouse_schemas_required"] is True
        assert _parameter_names(notebook) >= COMMON_PARAMETERS
        for index, cell in enumerate(notebook["cells"]):
            if cell["cell_type"] == "code":
                assert cell["outputs"] == []
                assert cell["execution_count"] is None
                compile("".join(cell["source"]), f"{path.name}:cell-{index}", "exec")
    gold_notebook = _notebook(NOTEBOOKS["Gold materialization"])
    assert _parameter_names(gold_notebook) >= GOLD_CONFIG_PARAMETERS


def test_notebooks_use_contracts_and_propagate_failures():
    expected_imports = {
        "Bronze ingestion": (
            "BronzeIngestor",
            "GitHubCopilotClient",
            "raise",
        ),
        "Silver normalization": (
            "normalize_ndjson",
            "create_dataframes",
            "merge_dataframes",
            "raise",
        ),
        "Gold materialization": (
            "GoldBuildOptions",
            "build_dataframes",
            "merge_dataframes",
            "raise",
        ),
    }
    for activity, path in NOTEBOOKS.items():
        source = "\n".join(
            "".join(cell.get("source", [])) for cell in _notebook(path)["cells"]
        )
        assert "pipeline_run_results" in source
        assert "write_audit(\"failed\"" in source
        for required in expected_imports[activity]:
            assert required in source


def test_bronze_lands_http_payloads_before_spark_reads():
    source = "\n".join(
        "".join(cell.get("source", []))
        for cell in _notebook(NOTEBOOKS["Bronze ingestion"])["cells"]
    )
    assert source.index("ingestor.ingest(") < source.index("spark.read.text(")
    assert "LocalBronzeStorage(bronze_root)" in source
    assert "getSecret(key_vault_uri, github_token_secret_name)" in source


def test_silver_and_gold_notebooks_wire_snapshot_replacement_and_no_data():
    bronze = "\n".join(
        "".join(cell.get("source", []))
        for cell in _notebook(NOTEBOOKS["Bronze ingestion"])["cells"]
    )
    silver = "\n".join(
        "".join(cell.get("source", []))
        for cell in _notebook(NOTEBOOKS["Silver normalization"])["cells"]
    )
    gold = "\n".join(
        "".join(cell.get("source", []))
        for cell in _notebook(NOTEBOOKS["Gold materialization"])["cells"]
    )

    assert "reuse_before=reuse_before" in bronze
    assert bronze.count('result.status != "reused"') == 2
    assert "select_latest_manifests(candidates)" in silver
    assert 'combined.record_snapshot(context, status="no_data")' in silver
    assert "snapshots=combined.snapshots" in silver
    assert "report_status_rows=report_status_rows" in gold
    assert "current_user_frame=current_user_frame" in gold
    assert 'if name == "user_daily"' in gold
    assert "timedelta(days=27)" in gold
    assert 'Path(lakehouse_files_root) / bronze_folder' in gold
    assert "normalize_report_types(report_types)" in gold
    assert 'manifest.get("report_type") in expected_report_types' in gold
    assert '"complete" if manifest["status"] == "success" else "no_data"' in gold
    assert "replacement_windows=windows" in gold


def test_pipeline_references_notebooks_in_strict_medallion_order():
    pipeline = json.loads(PIPELINE.read_text(encoding="utf-8"))["properties"]
    activities = pipeline["activities"]
    assert [activity["name"] for activity in activities] == list(NOTEBOOKS)
    assert activities[0]["dependsOn"] == []
    assert activities[1]["dependsOn"] == [
        {"activity": "Bronze ingestion", "dependencyConditions": ["Succeeded"]}
    ]
    assert activities[2]["dependsOn"] == [
        {"activity": "Silver normalization", "dependencyConditions": ["Succeeded"]}
    ]

    id_parameters = {
        "workspace_id",
        "bronze_notebook_id",
        "silver_notebook_id",
        "gold_notebook_id",
    }
    for name in id_parameters:
        assert pipeline["parameters"][name]["defaultValue"] == ""
    for activity in activities:
        assert activity["type"] == "TridentNotebook"
        assert activity["policy"]["retry"] >= 2
        assert activity["typeProperties"]["workspaceId"]["type"] == "Expression"
        assert activity["typeProperties"]["notebookId"]["type"] == "Expression"
        notebook_parameters = _parameter_names(
            _notebook(NOTEBOOKS[activity["name"]])
        )
        assert set(activity["typeProperties"]["parameters"]) <= notebook_parameters
    gold_parameters = activities[2]["typeProperties"]["parameters"]
    assert gold_parameters["bronze_folder"]["value"]["value"] == (
        "@pipeline().parameters.bronze_folder"
    )
    assert gold_parameters["report_types"]["value"]["value"] == (
        "@pipeline().parameters.report_types"
    )


def test_pipeline_supports_daily_windows_and_explicit_backfills_without_secrets():
    raw = PIPELINE.read_text(encoding="utf-8")
    pipeline = json.loads(raw)["properties"]
    assert pipeline["parameters"]["run_mode"]["defaultValue"] == "daily"
    assert pipeline["parameters"]["trailing_days"]["defaultValue"] == 28
    assert pipeline["parameters"]["calculation_lookback_days"]["defaultValue"] == 27
    assert pipeline["parameters"]["start_date"]["defaultValue"] == ""
    assert pipeline["parameters"]["end_date"]["defaultValue"] == ""
    assert pipeline["parameters"]["earliest_date"]["defaultValue"] == ""
    assert pipeline["parameters"]["scope_slug"]["defaultValue"] == ""
    assert pipeline["parameters"]["key_vault_uri"]["defaultValue"] == ""
    assert pipeline["parameters"]["github_token_secret_name"]["defaultValue"] == ""
    assert "@if(equals(pipeline().parameters.run_mode, 'backfill')" in raw
    assert not re.search(
        r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
        r"[89ab][0-9a-f]{3}-[0-9a-f]{12}\b",
        raw,
        re.IGNORECASE,
    )


def test_pipeline_separates_ingestion_and_gold_replacement_bounds():
    pipeline = json.loads(PIPELINE.read_text(encoding="utf-8"))["properties"]
    activities = {item["name"]: item for item in pipeline["activities"]}
    bronze = activities["Bronze ingestion"]["typeProperties"]["parameters"]
    silver = activities["Silver normalization"]["typeProperties"]["parameters"]
    gold = activities["Gold materialization"]["typeProperties"]["parameters"]

    assert bronze["start_date"]["value"]["value"] == INGESTION_START_EXPRESSION
    assert silver["start_date"]["value"]["value"] == INGESTION_START_EXPRESSION
    assert bronze["end_date"]["value"]["value"] == GOLD_END_EXPRESSION
    assert silver["end_date"]["value"]["value"] == GOLD_END_EXPRESSION
    assert bronze["replacement_start_date"]["value"]["value"] == (
        GOLD_START_EXPRESSION
    )
    assert gold["start_date"]["value"]["value"] == GOLD_START_EXPRESSION
    assert gold["end_date"]["value"]["value"] == GOLD_END_EXPRESSION


def test_january_backfill_contract_supplies_history_only_to_bronze_and_silver():
    bounds = resolve_pipeline_bounds(
        run_mode="backfill",
        today=date(2026, 2, 15),
        start_date=date(2026, 1, 28),
        end_date=date(2026, 2, 10),
    )

    assert bounds.ingestion_start == date(2026, 1, 1)
    assert bounds.ingestion_end == date(2026, 2, 10)
    assert bounds.replacement_start == date(2026, 1, 28)
    assert bounds.replacement_end == date(2026, 2, 10)


def test_daily_contract_keeps_trailing_gold_window_and_earliest_date_floor():
    daily = resolve_pipeline_bounds(
        run_mode="daily",
        today=date(2026, 2, 1),
        trailing_days=28,
    )
    floored = resolve_pipeline_bounds(
        run_mode="backfill",
        today=date(2026, 2, 1),
        start_date=date(2026, 1, 20),
        end_date=date(2026, 1, 31),
        earliest_date=date(2026, 1, 1),
    )

    assert daily.replacement_start == date(2026, 1, 4)
    assert daily.ingestion_start == date(2025, 12, 8)
    assert daily.replacement_end == date(2026, 1, 31)
    assert floored.ingestion_start == date(2026, 1, 1)
    assert floored.replacement_start == date(2026, 1, 20)
