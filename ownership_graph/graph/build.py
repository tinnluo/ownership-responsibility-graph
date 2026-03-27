"""Build a NetworkX graph and normalized graph exports."""

from __future__ import annotations

from pathlib import Path

import networkx as nx
import pandas as pd

from ownership_graph.analysis.attribution import (
    aggregate_direct_asset_ownership,
    find_root_entities,
)
from ownership_graph.models.schemas import DemoTables


def build_graph(tables: DemoTables) -> nx.DiGraph:
    """Build the three-layer ownership graph."""

    graph = nx.DiGraph()

    for row in tables.entities.to_dict(orient="records"):
        graph.add_node(
            f"entity:{row['entity_id']}",
            node_type="Entity",
            entity_id=row["entity_id"],
            display_name=row["entity_name"],
            country_iso3=row["country_iso3"],
            entity_kind=row["entity_kind"],
        )

    for row in tables.assets.to_dict(orient="records"):
        graph.add_node(
            f"asset:{row['asset_id']}",
            node_type="Asset",
            asset_id=row["asset_id"],
            display_name=row["asset_name"],
            sector=row["sector"],
            sub_sector=row["sub_sector"],
            technology=row["technology"],
            country_iso3=row["country_iso3"],
        )

    for row in tables.asset_emissions.to_dict(orient="records"):
        graph.add_node(
            f"emission:{row['emission_profile_id']}",
            node_type="EmissionProfile",
            emission_profile_id=row["emission_profile_id"],
            asset_id=row["asset_id"],
            reporting_year=int(row["reporting_year"]),
            total_tco2e=float(row["total_tco2e"]),
        )

    current_entity_ownership = tables.entity_ownership[tables.entity_ownership["is_current"]].copy()
    for row in current_entity_ownership.to_dict(orient="records"):
        graph.add_edge(
            f"entity:{row['parent_entity_id']}",
            f"entity:{row['child_entity_id']}",
            edge_type="OWNS_ENTITY",
            weight=float(row["ownership_share"]),
        )

    aggregated_assets = aggregate_direct_asset_ownership(tables.asset_ownership)
    for row in aggregated_assets.to_dict(orient="records"):
        graph.add_edge(
            f"entity:{row['entity_id']}",
            f"asset:{row['asset_id']}",
            edge_type="OWNS_ASSET",
            weight=float(row["asset_ownership_share"]),
        )

    for row in tables.asset_emissions.to_dict(orient="records"):
        graph.add_edge(
            f"asset:{row['asset_id']}",
            f"emission:{row['emission_profile_id']}",
            edge_type="HAS_EMISSIONS",
            weight=1.0,
        )

    return graph


def normalized_nodes_df(tables: DemoTables) -> pd.DataFrame:
    """Return a flat node export."""

    entity_nodes = tables.entities.assign(
        node_id=lambda df: "entity:" + df["entity_id"],
        node_type="Entity",
        label=lambda df: df["entity_name"],
        display_name=lambda df: df["entity_name"],
        sector="",
        sub_sector="",
        technology="",
        reporting_year="",
        total_tco2e="",
    )[
        [
            "node_id",
            "node_type",
            "label",
            "display_name",
            "country_iso3",
            "sector",
            "sub_sector",
            "technology",
            "reporting_year",
            "total_tco2e",
        ]
    ]

    asset_nodes = tables.assets.assign(
        node_id=lambda df: "asset:" + df["asset_id"],
        node_type="Asset",
        label=lambda df: df["asset_name"],
        display_name=lambda df: df["asset_name"],
        reporting_year="",
        total_tco2e="",
    )[
        [
            "node_id",
            "node_type",
            "label",
            "display_name",
            "country_iso3",
            "sector",
            "sub_sector",
            "technology",
            "reporting_year",
            "total_tco2e",
        ]
    ]

    emission_nodes = tables.asset_emissions.assign(
        node_id=lambda df: "emission:" + df["emission_profile_id"],
        node_type="EmissionProfile",
        label=lambda df: df["emission_profile_id"],
        display_name=lambda df: df["emission_profile_id"],
        country_iso3="",
        sector="",
        sub_sector="",
        technology="",
        reporting_year=lambda df: df["reporting_year"],
        total_tco2e=lambda df: df["total_tco2e"],
    )[
        [
            "node_id",
            "node_type",
            "label",
            "display_name",
            "country_iso3",
            "sector",
            "sub_sector",
            "technology",
            "reporting_year",
            "total_tco2e",
        ]
    ]

    return pd.concat([entity_nodes, asset_nodes, emission_nodes], ignore_index=True)


def normalized_edges_df(tables: DemoTables) -> pd.DataFrame:
    """Return a flat edge export."""

    current_entity_ownership = tables.entity_ownership[tables.entity_ownership["is_current"]].copy()
    entity_edges = current_entity_ownership.assign(
        source_id=lambda df: "entity:" + df["parent_entity_id"],
        target_id=lambda df: "entity:" + df["child_entity_id"],
        edge_type="OWNS_ENTITY",
        weight=lambda df: df["ownership_share"],
    )[["source_id", "target_id", "edge_type", "weight"]]

    aggregated_assets = aggregate_direct_asset_ownership(tables.asset_ownership)
    asset_edges = aggregated_assets.assign(
        source_id=lambda df: "entity:" + df["entity_id"],
        target_id=lambda df: "asset:" + df["asset_id"],
        edge_type="OWNS_ASSET",
        weight=lambda df: df["asset_ownership_share"],
    )[["source_id", "target_id", "edge_type", "weight"]]

    emission_edges = tables.asset_emissions.assign(
        source_id=lambda df: "asset:" + df["asset_id"],
        target_id=lambda df: "emission:" + df["emission_profile_id"],
        edge_type="HAS_EMISSIONS",
        weight=1.0,
    )[["source_id", "target_id", "edge_type", "weight"]]

    return pd.concat([entity_edges, asset_edges, emission_edges], ignore_index=True)


def write_build_outputs(tables: DemoTables, output_dir: str | Path) -> dict[str, str]:
    """Write staged and normalized graph artifacts."""

    output_path = Path(output_dir)
    normalized_dir = output_path / "normalized"
    staged_dir = output_path / "staged"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    staged_dir.mkdir(parents=True, exist_ok=True)

    nodes_df = normalized_nodes_df(tables)
    edges_df = normalized_edges_df(tables)
    nodes_df.to_csv(normalized_dir / "nodes.csv", index=False)
    edges_df.to_csv(normalized_dir / "edges.csv", index=False)

    tables.entities.to_csv(staged_dir / "entities.csv", index=False)
    tables.assets.to_csv(staged_dir / "assets.csv", index=False)
    tables.entity_ownership.to_csv(staged_dir / "entity_ownership.csv", index=False)
    tables.asset_ownership.to_csv(staged_dir / "asset_ownership.csv", index=False)
    tables.asset_emissions.to_csv(staged_dir / "asset_emissions.csv", index=False)

    graph = build_graph(tables)
    roots = find_root_entities(tables.entities["entity_id"], tables.entity_ownership)
    summary = {
        "node_count": graph.number_of_nodes(),
        "edge_count": graph.number_of_edges(),
        "root_entities": roots,
        "artifacts": {
            "nodes": str((normalized_dir / "nodes.csv").relative_to(output_path.parent)),
            "edges": str((normalized_dir / "edges.csv").relative_to(output_path.parent)),
            "staged_dir": str(staged_dir.relative_to(output_path.parent)),
        },
    }
    (output_path / "graph_summary.json").write_text(
        pd.Series(summary).to_json(indent=2), encoding="utf-8"
    )
    return summary
