"""Generate the source-controlled Power BI project.

The generated model intentionally uses placeholders for the OneLake workspace
and lakehouse names. It does not deploy or bind anything.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

ROOT = Path("assets/powerbi/GitHubCopilotMetrics")
MODEL = ROOT / "GitHubCopilotMetrics.SemanticModel"
REPORT = ROOT / "GitHubCopilotMetrics.Report"
DEFINITION = MODEL / "definition"
TABLES = DEFINITION / "tables"
PAGES = REPORT / "definition" / "pages"
UUID_NAMESPACE = uuid.UUID("fc6bbb75-fd6d-4ef1-a061-4151f72b804f")


def stable_uuid(value: str) -> uuid.UUID:
    return uuid.uuid5(UUID_NAMESPACE, value)

BASE_COLUMNS = {
    "scope_kind": "string",
    "scope_slug": "string",
    "day": "dateTime",
    "day_key": "int64",
    "gold_built_at": "string",
    "source_correction_count": "int64",
}
ADOPTION_COLUMNS = {
    "observed_users": "int64",
    "active_users": "int64",
    "engaged_users": "int64",
    "interaction_count": "int64",
    "generation_count": "int64",
    "acceptance_count": "int64",
    "acceptance_rate": "double",
    "loc_suggested": "int64",
    "loc_accepted": "int64",
    "loc_acceptance_rate": "double",
    "ai_credits_used": "double",
}
TABLE_CONTRACTS = {
    "Entity Adoption Daily": (
        "entity_adoption_daily",
        {
            **BASE_COLUMNS,
            **ADOPTION_COLUMNS,
            "source_reported_daily_active_users": "int64",
            "source_reported_weekly_active_users": "int64",
            "source_reported_monthly_active_users": "int64",
        },
    ),
    "Team Adoption Daily": (
        "team_adoption_daily",
        {
            **BASE_COLUMNS,
            "team_id": "int64",
            "team_slug": "string",
            **ADOPTION_COLUMNS,
            "membership_count": "int64",
            "allocation_method": "string",
            "is_additive": "boolean",
        },
    ),
    "Repository Impact Daily": (
        "repository_copilot_impact_daily",
        {
            **BASE_COLUMNS,
            "repo_id": "int64",
            "repo_owner_name": "string",
            "repo_name": "string",
            "repo_visibility": "string",
            "pull_requests_created": "int64",
            "pull_requests_created_by_copilot": "int64",
            "copilot_authored_pr_rate": "double",
            "pull_requests_reviewed": "int64",
            "pull_requests_reviewed_by_copilot": "int64",
            "copilot_reviewed_pr_rate": "double",
            "suggestions": "int64",
            "applied_suggestions": "int64",
            "suggestion_apply_rate": "double",
            "pull_requests_merged": "int64",
            "median_minutes_to_merge": "double",
        },
    ),
    "Feature Usage Daily": (
        "feature_usage_daily",
        {**BASE_COLUMNS, "feature": "string", **ADOPTION_COLUMNS},
    ),
    "Language Usage Daily": (
        "language_usage_daily",
        {**BASE_COLUMNS, "language": "string", **ADOPTION_COLUMNS},
    ),
    "IDE Usage Daily": (
        "ide_usage_daily",
        {**BASE_COLUMNS, "ide": "string", **ADOPTION_COLUMNS},
    ),
    "Adoption Rolling Daily": (
        "adoption_rolling_daily",
        {
            **BASE_COLUMNS,
            "dimension_type": "string",
            "dimension_id": "string",
            "dimension_name": "string",
            "window_days": "int64",
            "distinct_active_users": "int64",
            "allocation_method": "string",
            "is_additive": "boolean",
        },
    ),
    "Data Freshness Daily": (
        "data_freshness_daily",
        {
            **BASE_COLUMNS,
            "report_type": "string",
            "availability_status": "string",
            "has_data": "boolean",
            "is_no_data": "boolean",
            "is_complete": "boolean",
            "is_within_telemetry_lag": "boolean",
            "days_late": "int64",
            "latest_source_ingested_at": "string",
            "sparse_metric_count": "int64",
        },
    ),
}

MEASURES = {
    "Entity Adoption Daily": [
        ("Daily Active Users", "SUM ( 'Entity Adoption Daily'[active_users] )", "#,0"),
        ("Observed Users", "SUM ( 'Entity Adoption Daily'[observed_users] )", "#,0"),
        ("Engaged Users", "SUM ( 'Entity Adoption Daily'[engaged_users] )", "#,0"),
        ("Engagement Rate", "DIVIDE ( [Engaged Users], [Observed Users] )", "0.0%"),
        ("Generations", "SUM ( 'Entity Adoption Daily'[generation_count] )", "#,0"),
        ("Acceptances", "SUM ( 'Entity Adoption Daily'[acceptance_count] )", "#,0"),
        ("Acceptance Rate", "DIVIDE ( [Acceptances], [Generations] )", "0.0%"),
        ("Lines Suggested", "SUM ( 'Entity Adoption Daily'[loc_suggested] )", "#,0"),
        ("Lines Accepted", "SUM ( 'Entity Adoption Daily'[loc_accepted] )", "#,0"),
        (
            "LOC Acceptance Rate",
            "DIVIDE ( [Lines Accepted], [Lines Suggested] )",
            "0.0%",
        ),
        (
            "Interactions per Active User",
            "DIVIDE ( SUM ( 'Entity Adoption Daily'[interaction_count] ), "
            "[Daily Active Users] )",
            "0.00",
        ),
        (
            "AI Credits Used",
            "SUM ( 'Entity Adoption Daily'[ai_credits_used] )",
            "#,0.00",
        ),
        ("Latest Metric Date", "MAX ( 'Entity Adoption Daily'[day] )", "yyyy-mm-dd"),
    ],
    "Adoption Rolling Daily": [
        (
            "Weekly Active Users",
            "CALCULATE ( SUM ( 'Adoption Rolling Daily'[distinct_active_users] ), "
            "KEEPFILTERS ( 'Adoption Rolling Daily'[dimension_type] = \"entity\" ), "
            "KEEPFILTERS ( 'Adoption Rolling Daily'[window_days] = 7 ) )",
            "#,0",
        ),
        (
            "28-Day Active Users",
            "CALCULATE ( SUM ( 'Adoption Rolling Daily'[distinct_active_users] ), "
            "KEEPFILTERS ( 'Adoption Rolling Daily'[dimension_type] = \"entity\" ), "
            "KEEPFILTERS ( 'Adoption Rolling Daily'[window_days] = 28 ) )",
            "#,0",
        ),
    ],
    "Team Adoption Daily": [
        (
            "Team Active Users",
            "CALCULATE ( SUM ( 'Team Adoption Daily'[active_users] ), "
            "KEEPFILTERS ( 'Team Adoption Daily'[is_additive] = TRUE() ) )",
            "#,0",
        ),
        (
            "Team Observed Users",
            "CALCULATE ( SUM ( 'Team Adoption Daily'[observed_users] ), "
            "KEEPFILTERS ( 'Team Adoption Daily'[is_additive] = TRUE() ) )",
            "#,0",
        ),
        (
            "Team Adoption Rate",
            "DIVIDE ( [Team Active Users], [Team Observed Users] )",
            "0.0%",
        ),
        (
            "Team Acceptance Rate",
            "VAR _accepted = CALCULATE ( "
            "SUM ( 'Team Adoption Daily'[acceptance_count] ), "
            "KEEPFILTERS ( 'Team Adoption Daily'[is_additive] = TRUE() ) ) "
            "VAR _generated = CALCULATE ( "
            "SUM ( 'Team Adoption Daily'[generation_count] ), "
            "KEEPFILTERS ( 'Team Adoption Daily'[is_additive] = TRUE() ) ) "
            "RETURN DIVIDE ( _accepted, _generated )",
            "0.0%",
        ),
        (
            "Non-Additive Team Rows",
            "CALCULATE ( COUNTROWS ( 'Team Adoption Daily' ), "
            "KEEPFILTERS ( 'Team Adoption Daily'[is_additive] = FALSE() ) )",
            "#,0",
        ),
        (
            "Team Additivity Warning",
            'IF ( [Non-Additive Team Rows] > 0, "Warning: all-memberships rows '
            'can double-count users; executive totals use primary-team rows only.", '
            '"Additive team allocation is available." )',
            "",
        ),
    ],
    "Feature Usage Daily": [
        ("Feature Active Users", "SUM ( 'Feature Usage Daily'[active_users] )", "#,0"),
        (
            "Feature Observed Users",
            "SUM ( 'Feature Usage Daily'[observed_users] )",
            "#,0",
        ),
        (
            "Feature Adoption Rate",
            "DIVIDE ( [Feature Active Users], [Feature Observed Users] )",
            "0.0%",
        ),
        (
            "Feature Acceptance Rate",
            "DIVIDE ( SUM ( 'Feature Usage Daily'[acceptance_count] ), "
            "SUM ( 'Feature Usage Daily'[generation_count] ) )",
            "0.0%",
        ),
    ],
    "Repository Impact Daily": [
        (
            "Pull Requests Created",
            "SUM ( 'Repository Impact Daily'[pull_requests_created] )",
            "#,0",
        ),
        (
            "Copilot Authored Pull Requests",
            "SUM ( 'Repository Impact Daily'[pull_requests_created_by_copilot] )",
            "#,0",
        ),
        (
            "Copilot Authored PR Rate",
            "DIVIDE ( [Copilot Authored Pull Requests], [Pull Requests Created] )",
            "0.0%",
        ),
        (
            "Pull Requests Reviewed",
            "SUM ( 'Repository Impact Daily'[pull_requests_reviewed] )",
            "#,0",
        ),
        (
            "Copilot Reviewed Pull Requests",
            "SUM ( 'Repository Impact Daily'[pull_requests_reviewed_by_copilot] )",
            "#,0",
        ),
        (
            "Copilot Reviewed PR Rate",
            "DIVIDE ( [Copilot Reviewed Pull Requests], [Pull Requests Reviewed] )",
            "0.0%",
        ),
        (
            "Repository Suggestion Apply Rate",
            "DIVIDE ( SUM ( 'Repository Impact Daily'[applied_suggestions] ), "
            "SUM ( 'Repository Impact Daily'[suggestions] ) )",
            "0.0%",
        ),
        (
            "Median Minutes to Merge",
            "MEDIAN ( 'Repository Impact Daily'[median_minutes_to_merge] )",
            "#,0.0",
        ),
    ],
    "Data Freshness Daily": [
        (
            "Incomplete Report Days",
            "CALCULATE ( COUNTROWS ( 'Data Freshness Daily' ), "
            "KEEPFILTERS ( 'Data Freshness Daily'[is_complete] = FALSE() ), "
            "KEEPFILTERS ( "
            "'Data Freshness Daily'[is_within_telemetry_lag] = FALSE() ) )",
            "#,0",
        ),
        ("Maximum Days Late", "MAX ( 'Data Freshness Daily'[days_late] )", "#,0"),
        (
            "Sparse Metric Count",
            "SUM ( 'Data Freshness Daily'[sparse_metric_count] )",
            "#,0",
        ),
        (
            "Latest Source Ingested At",
            "MAX ( 'Data Freshness Daily'[latest_source_ingested_at] )",
            "",
        ),
        (
            "Freshness Status",
            'IF ( [Incomplete Report Days] > 0, "Attention required", '
            'IF ( [Maximum Days Late] > 0, "Within telemetry lag", "Current" ) )',
            "",
        ),
    ],
}

PAGE_SPECS = [
    (
        "ExecutiveOverview",
        "Executive overview",
        [
            ("Daily Active Users", "Entity Adoption Daily"),
            ("Weekly Active Users", "Adoption Rolling Daily"),
            ("28-Day Active Users", "Adoption Rolling Daily"),
            ("Acceptance Rate", "Entity Adoption Daily"),
            ("LOC Acceptance Rate", "Entity Adoption Daily"),
            ("Freshness Status", "Data Freshness Daily"),
        ],
    ),
    (
        "TeamAdoption",
        "Team adoption",
        [
            ("Team Active Users", "Team Adoption Daily"),
            ("Team Adoption Rate", "Team Adoption Daily"),
            ("Team Acceptance Rate", "Team Adoption Daily"),
            ("Team Additivity Warning", "Team Adoption Daily"),
        ],
    ),
    (
        "DeveloperExperience",
        "Developer experience",
        [
            ("Interactions per Active User", "Entity Adoption Daily"),
            ("AI Credits Used", "Entity Adoption Daily"),
            ("Feature Adoption Rate", "Feature Usage Daily"),
            ("Feature Acceptance Rate", "Feature Usage Daily"),
        ],
    ),
    (
        "RepositoryImpact",
        "Repository impact",
        [
            ("Pull Requests Created", "Repository Impact Daily"),
            ("Copilot Authored PR Rate", "Repository Impact Daily"),
            ("Copilot Reviewed PR Rate", "Repository Impact Daily"),
            ("Repository Suggestion Apply Rate", "Repository Impact Daily"),
            ("Median Minutes to Merge", "Repository Impact Daily"),
        ],
    ),
    (
        "DataQuality",
        "Data quality",
        [
            ("Freshness Status", "Data Freshness Daily"),
            ("Incomplete Report Days", "Data Freshness Daily"),
            ("Maximum Days Late", "Data Freshness Daily"),
            ("Sparse Metric Count", "Data Freshness Daily"),
            ("Non-Additive Team Rows", "Team Adoption Daily"),
        ],
    ),
]


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def write_project_files() -> None:
    write_json(
        ROOT / "GitHubCopilotMetrics.pbip",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/"
            "pbipProperties/1.0.0/schema.json",
            "version": "1.0",
            "artifacts": [{"report": {"path": "GitHubCopilotMetrics.Report"}}],
            "settings": {"enableAutoRecovery": True},
        },
    )
    write_json(
        MODEL / "definition.pbism",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/"
            "semanticModel/definitionProperties/1.0.0/schema.json",
            "version": "4.2",
            "settings": {"qnaEnabled": True},
        },
    )
    write_json(
        REPORT / "definition.pbir",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/"
            "report/definitionProperties/2.0.0/schema.json",
            "version": "4.0",
            "datasetReference": {
                "byPath": {"path": "../GitHubCopilotMetrics.SemanticModel"}
            },
        },
    )


def write_model() -> None:
    DEFINITION.mkdir(parents=True, exist_ok=True)
    (DEFINITION / "database.tmdl").write_text(
        "database GitHubCopilotMetrics\n\tcompatibilityLevel: 1702\n",
        encoding="utf-8",
    )
    (DEFINITION / "model.tmdl").write_text(
        """model Model
\tculture: en-US
\tdefaultPowerBIDataSourceVersion: powerBI_V3
\tsourceQueryCulture: en-US
\tdataAccessOptions
\t\tlegacyRedirects
\t\treturnErrorValuesAsNull
""",
        encoding="utf-8",
    )
    (DEFINITION / "expressions.tmdl").write_text(
        """        expression DirectLakeConnection = ```
        let
            Source = Sql.Database("<SQL_ENDPOINT>", "<SQL_DATABASE>")
        in
            Source
```
\tlineageTag: 3c13361c-0efe-4da2-a602-fdeebec3c0a1
""",
        encoding="utf-8",
    )
    hidden = {
        "day_key",
        "gold_built_at",
        "source_correction_count",
        "acceptance_rate",
        "loc_acceptance_rate",
        "copilot_authored_pr_rate",
        "copilot_reviewed_pr_rate",
        "suggestion_apply_rate",
    }
    TABLES.mkdir(parents=True, exist_ok=True)
    for display_name, (entity_name, columns) in TABLE_CONTRACTS.items():
        lines = [
            f"table '{display_name}'",
            f"\tlineageTag: {stable_uuid(f'table:{display_name}')}",
        ]
        for column, data_type in columns.items():
            lines.extend(
                [
                    f"\n\tcolumn {column}",
                    f"\t\tdataType: {data_type}",
                    f"\t\tsourceColumn: {column}",
                ]
            )
            if column in hidden:
                lines.append("\t\tisHidden")
            if column == "day":
                lines.append("\t\tformatString: yyyy-mm-dd")
            folder = (
                "Technical"
                if column in hidden
                else "Dimensions"
                if data_type in {"string", "boolean"} or column.endswith("_id")
                else "Metrics"
            )
            lines.append(f"\t\tdisplayFolder: {folder}")
        for name, expression, format_string in MEASURES.get(display_name, []):
            lines.extend(
                [
                    f"\n\tmeasure '{name}' = {expression}",
                    f"\t\tlineageTag: {stable_uuid(f'measure:{display_name}:{name}')}",
                    "\t\tdisplayFolder: Measures",
                ]
            )
            if format_string:
                lines.append(f"\t\tformatString: {format_string}")
        lines.extend(
            [
                f"\n\tpartition {entity_name} = entity",
                "\t\tmode: directLake",
                "\t\tsource",
                f"\t\t\tentityName: {entity_name}",
                "\t\t\tschemaName: gold",
                "\t\t\texpressionSource: DirectLakeConnection",
                "",
            ]
        )
        filename = display_name.replace(" ", "") + ".tmdl"
        (TABLES / filename).write_text("\n".join(lines), encoding="utf-8")

    (TABLES / "Date.tmdl").write_text(
        """table Date
\tlineageTag: 4ffbf09c-c658-4c3e-aea1-eae326bd86ec
\tdataCategory: Time

\tcolumn Date
\t\tdataType: dateTime
\t\tformatString: yyyy-mm-dd
\t\tisKey

\tcolumn Year
\t\tdataType: int64

\tcolumn 'Month Number'
\t\tdataType: int64
\t\tisHidden

\tcolumn Month
\t\tdataType: string
\t\tsortByColumn: 'Month Number'

\tcolumn 'Year Month'
\t\tdataType: string
\t\tsortByColumn: 'Year Month Number'

\tcolumn 'Year Month Number'
\t\tdataType: int64
\t\tisHidden

\tpartition Date = calculated
\t\tmode: import
\t\tsource = ```
VAR _start = DATE ( 2020, 1, 1 )
VAR _end = DATE ( 2035, 12, 31 )
RETURN
    ADDCOLUMNS (
        CALENDAR ( _start, _end ),
        "Year", YEAR ( [Date] ),
        "Month Number", MONTH ( [Date] ),
        "Month", FORMAT ( [Date], "mmm" ),
        "Year Month", FORMAT ( [Date], "yyyy-mmm" ),
        "Year Month Number", YEAR ( [Date] ) * 100 + MONTH ( [Date] )
    )
```
""",
        encoding="utf-8",
    )
    relationships = []
    for display_name in TABLE_CONTRACTS:
        relationships.extend(
            [
                f"relationship {stable_uuid(f'relationship:{display_name}:Date')}",
                f"\tfromColumn: '{display_name}'.day",
                "\ttoColumn: Date.Date",
                "\tcrossFilteringBehavior: oneDirection",
                "",
            ]
        )
    (DEFINITION / "relationships.tmdl").write_text(
        "\n".join(relationships), encoding="utf-8"
    )


def measure_field(table: str, measure: str) -> dict:
    return {
        "Measure": {
            "Expression": {"SourceRef": {"Entity": table}},
            "Property": measure,
        }
    }


def title_visual(page_id: str, title: str) -> dict:
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/"
        "report/definition/visualContainer/2.5.0/schema.json",
        "name": f"{page_id}_title",
        "position": {
            "x": 48,
            "y": 32,
            "z": 0,
            "height": 80,
            "width": 1200,
            "tabOrder": 0,
        },
        "visual": {
            "visualType": "textbox",
            "objects": {
                "general": [
                    {
                        "properties": {
                            "paragraphs": [
                                {
                                    "textRuns": [
                                        {
                                            "value": title,
                                            "textStyle": {
                                                "fontFamily": "Segoe UI Semibold",
                                                "fontSize": "24pt",
                                                "color": "#172B4D",
                                            },
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            },
        },
    }


def card_visual(
    page_id: str,
    index: int,
    table: str,
    measure: str,
) -> dict:
    column = index % 3
    row = index // 3
    query_ref = f"{table}.{measure}"
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/"
        "report/definition/visualContainer/2.5.0/schema.json",
        "name": f"{page_id}_kpi_{index + 1}",
        "position": {
            "x": 48 + column * 604,
            "y": 144 + row * 260,
            "z": index + 1,
            "height": 220,
            "width": 556,
            "tabOrder": index + 1,
        },
        "visual": {
            "visualType": "cardVisual",
            "query": {
                "queryState": {
                    "Data": {
                        "projections": [
                            {
                                "field": measure_field(table, measure),
                                "queryRef": query_ref,
                                "nativeQueryRef": measure,
                            }
                        ]
                    }
                }
            },
        },
    }


def trend_visual(page_id: str, table: str, measure: str) -> dict:
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/"
        "report/definition/visualContainer/2.5.0/schema.json",
        "name": f"{page_id}_trend",
        "position": {
            "x": 48,
            "y": 680,
            "z": 20,
            "height": 340,
            "width": 1764,
            "tabOrder": 20,
        },
        "visual": {
            "visualType": "lineChart",
            "query": {
                "queryState": {
                    "Category": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {"Entity": "Date"}
                                        },
                                        "Property": "Date",
                                    }
                                },
                                "queryRef": "Date.Date",
                                "nativeQueryRef": "Date",
                            }
                        ]
                    },
                    "Y": {
                        "projections": [
                            {
                                "field": measure_field(table, measure),
                                "queryRef": f"{table}.{measure}",
                                "nativeQueryRef": measure,
                            }
                        ]
                    },
                }
            },
        },
    }


def write_report() -> None:
    report_definition = REPORT / "definition"
    write_json(
        report_definition / "version.json",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/"
            "report/definition/versionMetadata/1.0.0/schema.json",
            "version": "2.0.0",
        },
    )
    write_json(
        report_definition / "report.json",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/"
            "report/definition/report/3.0.0/schema.json",
            "themeCollection": {
                "baseTheme": {
                    "name": "CY24SU10",
                    "reportVersionAtImport": "5.60",
                    "type": "SharedResources",
                }
            },
            "layoutOptimization": "None",
        },
    )
    page_order = []
    for page_id, title, kpis in PAGE_SPECS:
        page_order.append(page_id)
        page_dir = PAGES / page_id
        write_json(
            page_dir / "page.json",
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/"
                "item/report/definition/page/2.0.0/schema.json",
                "name": page_id,
                "displayName": title,
                "displayOption": "FitToPage",
                "height": 1080,
                "width": 1920,
                "filterConfig": {"filters": []},
                "objects": {
                    "background": [
                        {
                            "properties": {
                                "color": {"solid": {"color": "#F4F7FB"}},
                                "transparency": 0,
                            }
                        }
                    ]
                },
            },
        )
        visuals = [title_visual(page_id, title)]
        visuals.extend(
            card_visual(page_id, index, table, measure)
            for index, (measure, table) in enumerate(kpis)
        )
        first_measure, first_table = kpis[0]
        visuals.append(trend_visual(page_id, first_table, first_measure))
        for visual in visuals:
            write_json(
                page_dir / "visuals" / visual["name"] / "visual.json", visual
            )
    write_json(
        PAGES / "pages.json",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/"
            "report/definition/pagesMetadata/1.0.0/schema.json",
            "pageOrder": page_order,
            "activePageName": "ExecutiveOverview",
        },
    )


def write_supporting_files() -> None:
    template = ROOT / "templates" / "ScopeFilterTemplate.tmdl"
    template.parent.mkdir(parents=True, exist_ok=True)
    template.write_text(
        """// OPTIONAL TEMPLATE ONLY - intentionally outside definition/.
// Replace FALSE() with a reviewed authorized-scope predicate on every exposed
// fact before enabling the role. Configure role membership in Power BI.
role 'Scope Filter Template'
\tmodelPermission: read
\ttablePermission 'Entity Adoption Daily' = ```
FALSE()
```
""",
        encoding="utf-8",
    )
    write_json(
        ROOT / "asset-manifest.json",
        {
            "semanticModel": "GitHubCopilotMetrics",
            "storageMode": "DirectLake",
            "goldSchema": "gold",
            "sourceTables": [item[0] for item in TABLE_CONTRACTS.values()],
            "reportPages": [
                {
                    "id": page_id,
                    "displayName": title,
                    "measures": [measure for measure, _ in kpis],
                }
                for page_id, title, kpis in PAGE_SPECS
            ],
            "userDetailExposed": False,
            "rlsTemplate": "templates/ScopeFilterTemplate.tmdl",
        },
    )


def main() -> None:
    write_project_files()
    write_model()
    write_report()
    write_supporting_files()


if __name__ == "__main__":
    main()
