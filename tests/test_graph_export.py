from pathlib import Path

import pandas as pd

from ownership_graph.analysis.attribution import build_responsibility_table
from ownership_graph.analysis.hierarchy import trace_entity_hierarchy
from ownership_graph.export.neo4j import write_neo4j_exports
from ownership_graph.graph.build import build_graph, normalized_edges_df, normalized_nodes_df


def test_build_graph_and_normalized_exports(demo_tables):
    graph = build_graph(demo_tables)
    nodes = normalized_nodes_df(demo_tables)
    edges = normalized_edges_df(demo_tables)

    assert graph.number_of_nodes() == 14
    assert graph.number_of_edges() == 13
    assert len(nodes) == 14
    assert len(edges) == 13

    ast100_edge = edges[
        (edges["source_id"] == "entity:ENT_011") & (edges["target_id"] == "asset:AST_100")
    ].iloc[0]
    assert ast100_edge["weight"] == 0.6


def test_write_neo4j_exports(tmp_path: Path, demo_tables):
    descendants = trace_entity_hierarchy(demo_tables.entity_ownership, "ENT_001")
    responsibility = build_responsibility_table(
        "ENT_001", descendants, demo_tables.asset_ownership, demo_tables.asset_emissions
    )
    write_neo4j_exports(demo_tables, tmp_path, responsibility)

    expected_files = {
        "entities.csv",
        "assets.csv",
        "emission_profiles.csv",
        "owns_entity.csv",
        "owns_asset.csv",
        "has_emissions.csv",
        "attributed_emissions.csv",
        "load.cypher",
        "queries.cypher",
    }
    assert expected_files == {path.name for path in tmp_path.iterdir()}

    owns_asset = pd.read_csv(tmp_path / "owns_asset.csv")
    ast100_row = owns_asset[
        (owns_asset[":START_ID(Entity-ID)"] == "ENT_011")
        & (owns_asset[":END_ID(Asset-ID)"] == "AST_100")
    ]
    assert len(ast100_row) == 1
    assert ast100_row.iloc[0]["weight:float"] == 0.6

    attributed = pd.read_csv(tmp_path / "attributed_emissions.csv")
    assert len(attributed) == 3
    assert set(attributed[":TYPE"]) == {"ATTRIBUTED_EMISSIONS"}
