from pathlib import Path

import pandas as pd
import pytest

from ownership_graph.export.neo4j_runtime import (
    QUERY_DEFINITIONS,
    load_runtime_bundle,
    resolve_connection_args,
)


def test_resolve_connection_args_from_env(monkeypatch):
    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "secret")
    monkeypatch.setenv("NEO4J_DATABASE", "neo4j")

    resolved = resolve_connection_args(None, None, None, None)
    assert resolved == ("bolt://localhost:7687", "neo4j", "secret", "neo4j")


def test_resolve_connection_args_requires_missing_values(monkeypatch):
    monkeypatch.delenv("NEO4J_URI", raising=False)
    monkeypatch.delenv("NEO4J_USERNAME", raising=False)
    monkeypatch.delenv("NEO4J_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="Missing required Neo4j connection settings"):
        resolve_connection_args(None, None, None, None)


def test_load_runtime_bundle_reads_analysis_outputs(tmp_path: Path, demo_tables):
    staged_dir = tmp_path / "staged"
    analysis_dir = tmp_path / "analysis"
    staged_dir.mkdir()
    analysis_dir.mkdir()

    demo_tables.entities.to_csv(staged_dir / "entities.csv", index=False)
    demo_tables.assets.to_csv(staged_dir / "assets.csv", index=False)
    demo_tables.entity_ownership.to_csv(staged_dir / "entity_ownership.csv", index=False)
    demo_tables.asset_ownership.to_csv(staged_dir / "asset_ownership.csv", index=False)
    demo_tables.asset_emissions.to_csv(staged_dir / "asset_emissions.csv", index=False)

    responsibility = pd.DataFrame(
        [
            {
                "root_entity_id": "ENT_001",
                "entity_id": "ENT_011",
                "asset_id": "AST_100",
                "hierarchy_level": 2,
                "entity_path": "ENT_001|ENT_010|ENT_011",
                "entity_ownership_share": 0.4,
                "asset_ownership_share": 0.6,
                "compound_ownership_share": 0.24,
                "asset_emissions": 1000.0,
                "attributed_emissions": 240.0,
                "emission_profile_id": "EP_100",
                "reporting_year": 2025,
            }
        ]
    )
    responsibility.to_csv(analysis_dir / "responsibility_attribution.csv", index=False)
    responsibility.rename(columns={"entity_id": "holder_entity_id"})[
        [
            "root_entity_id",
            "emission_profile_id",
            "asset_id",
            "holder_entity_id",
            "entity_path",
            "entity_ownership_share",
            "asset_ownership_share",
            "compound_ownership_share",
            "attributed_emissions",
            "reporting_year",
        ]
    ].to_csv(analysis_dir / "attributed_emission_relationships.csv", index=False)

    _, responsibility_df, attributed_df = load_runtime_bundle(tmp_path)
    assert len(responsibility_df) == 1
    assert len(attributed_df) == 1
    assert attributed_df.iloc[0]["holder_entity_id"] == "ENT_011"


def test_query_definitions_cover_expected_runtime_queries():
    assert set(QUERY_DEFINITIONS) == {"asset-trace", "root-chain", "top-entities"}
