import json
import re
import subprocess
import sys
from pathlib import Path

from copilot_metrics_fabric.gold import contracts

ROOT = Path(__file__).parents[1]
ASSETS = ROOT / "assets" / "powerbi" / "GitHubCopilotMetrics"
MODEL = ASSETS / "GitHubCopilotMetrics.SemanticModel" / "definition"
REPORT = ASSETS / "GitHubCopilotMetrics.Report"


def read(path):
    return path.read_text(encoding="utf-8")


def test_generator_is_deterministic():
    before = {
        path.relative_to(ASSETS): path.read_bytes()
        for path in ASSETS.rglob("*")
        if path.is_file()
    }
    subprocess.run(
        [sys.executable, "scripts/generate_powerbi_assets.py"],
        cwd=ROOT,
        check=True,
    )
    after = {
        path.relative_to(ASSETS): path.read_bytes()
        for path in ASSETS.rglob("*")
        if path.is_file()
    }
    assert after == before


def test_direct_lake_tables_match_finalized_gold_contracts():
    manifest = json.loads(read(ASSETS / "asset-manifest.json"))
    expected_tables = [
        name
        for name in contracts()
        if name not in {"user_adoption_daily", "user_adoption_current"}
    ]
    assert manifest["storageMode"] == "DirectLake"
    assert manifest["goldSchema"] == "gold"
    assert manifest["sourceTables"] == expected_tables
    assert manifest["userDetailExposed"] is False

    table_text = "\n".join(read(path) for path in (MODEL / "tables").glob("*.tmdl"))
    for table_name in expected_tables:
        contract = contracts()[table_name]
        assert f"entityName: {table_name}" in table_text
        for column in contract["columns"]:
            assert f"sourceColumn: {column['name']}" in table_text
    assert table_text.count("mode: directLake") == len(contracts()) - 2


def test_date_table_relationships_and_direct_lake_connection_exist():
    date = read(MODEL / "tables" / "Date.tmdl")
    assert "table Date" in date
    assert "CALENDAR ( _start, _end )" in date
    relationships = read(MODEL / "relationships.tmdl")
    assert relationships.count("toColumn: Date.Date") == 8
    connection = read(MODEL / "expressions.tmdl")
    assert "Sql.Database" in connection
    assert "<SQL_ENDPOINT>" in connection
    assert "<SQL_DATABASE>" in connection


def test_required_measures_exist_and_rates_are_ratio_of_sums():
    model_text = "\n".join(read(path) for path in MODEL.rglob("*.tmdl"))
    required = {
        "Daily Active Users",
        "Weekly Active Users",
        "28-Day Active Users",
        "Acceptance Rate",
        "LOC Acceptance Rate",
        "Feature Adoption Rate",
        "Team Adoption Rate",
        "Team Acceptance Rate",
        "Team Additivity Warning",
        "Copilot Authored PR Rate",
        "Copilot Reviewed PR Rate",
        "Repository Suggestion Apply Rate",
        "Freshness Status",
        "Incomplete Report Days",
    }
    defined = set(re.findall(r"^\s*measure '([^']+)' =", model_text, re.MULTILINE))
    assert required <= defined
    assert "DIVIDE ( [Acceptances], [Generations] )" in model_text
    assert "DIVIDE ( [Lines Accepted], [Lines Suggested] )" in model_text
    assert "AVERAGE ( 'Entity Adoption Daily'[acceptance_rate] )" not in model_text
    assert "'Team Adoption Daily'[is_additive] = TRUE()" in model_text
    assert "'Team Adoption Daily'[is_additive] = FALSE()" in model_text


def test_report_pages_and_measure_references_are_valid():
    pages = json.loads(read(REPORT / "definition" / "pages" / "pages.json"))
    expected = {
        "ExecutiveOverview",
        "TeamAdoption",
        "DeveloperExperience",
        "RepositoryImpact",
        "DataQuality",
    }
    assert set(pages["pageOrder"]) == expected
    defined_measures = set()
    for path in MODEL.rglob("*.tmdl"):
        defined_measures.update(
            re.findall(r"^\s*measure '([^']+)' =", read(path), re.MULTILINE)
        )

    for page_id in expected:
        page_dir = REPORT / "definition" / "pages" / page_id
        page = json.loads(read(page_dir / "page.json"))
        assert page["displayName"]
        visuals = [
            json.loads(read(path))
            for path in (page_dir / "visuals").glob("*/visual.json")
        ]
        assert any(v["visual"]["visualType"] == "textbox" for v in visuals)
        assert any(v["visual"]["visualType"] == "cardVisual" for v in visuals)
        assert any(v["visual"]["visualType"] == "lineChart" for v in visuals)
        for visual in visuals:
            query = visual["visual"].get("query", {})
            for state in query.get("queryState", {}).values():
                for projection in state.get("projections", []):
                    field = projection["field"]
                    if "Measure" in field:
                        assert field["Measure"]["Property"] in defined_measures


def test_all_visual_table_and_measure_references_resolve_to_model_assets():
    table_names = set()
    defined_measures = set()
    for path in MODEL.rglob("*.tmdl"):
        text = read(path)
        table_names.update(
            re.findall(r"^table '?([^'\n]+)'?$", text, re.MULTILINE)
        )
        defined_measures.update(
            re.findall(r"^\s*measure '([^']+)' =", text, re.MULTILINE)
        )

    for path in (REPORT / "definition" / "pages").glob(
        "*/visuals/*/visual.json"
    ):
        visual = json.loads(read(path))
        query_state = visual["visual"].get("query", {}).get("queryState", {})
        for state in query_state.values():
            for projection in state.get("projections", []):
                field = projection.get("field", {})
                for kind in ("Column", "Measure"):
                    reference = field.get(kind)
                    if not reference:
                        continue
                    entity = reference["Expression"]["SourceRef"]["Entity"]
                    assert entity in table_names
                    if kind == "Measure":
                        assert reference["Property"] in defined_measures


def test_pbip_binding_and_rls_template_are_safe():
    binding = json.loads(read(REPORT / "definition.pbir"))
    assert binding["datasetReference"]["byPath"]["path"] == (
        "../GitHubCopilotMetrics.SemanticModel"
    )
    template_path = ASSETS / "templates" / "ScopeFilterTemplate.tmdl"
    assert template_path.exists()
    assert not (MODEL / "roles").exists()
    template = read(template_path)
    assert "FALSE()" in template
    assert "@" not in template
