"""Ownership aggregation and emissions attribution."""

from __future__ import annotations

from typing import Iterable

import networkx as nx
import pandas as pd

from ownership_graph.analysis.hierarchy import trace_entity_hierarchy


def aggregate_direct_asset_ownership(asset_ownership_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate duplicate owner-asset rows after filtering to direct owners."""

    direct_df = asset_ownership_df[
        asset_ownership_df["owner_type"].astype(str).str.lower() == "direct_owner"
    ].copy()
    grouped = (
        direct_df.groupby(["entity_id", "asset_id"], as_index=False)
        .agg(asset_ownership_share=("ownership_share", "sum"))
        .sort_values(["entity_id", "asset_id"])
        .reset_index(drop=True)
    )
    invalid_mask = (grouped["asset_ownership_share"] < 0) | (grouped["asset_ownership_share"] > 1)
    if invalid_mask.any():
        raise ValueError("Aggregated asset ownership share must stay within [0, 1]")
    return grouped


def build_responsibility_table(
    root_entity_id: str,
    descendants_df: pd.DataFrame,
    asset_ownership_df: pd.DataFrame,
    asset_emissions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Join hierarchy, asset ownership, and emissions into one attribution table."""

    aggregated_assets = aggregate_direct_asset_ownership(asset_ownership_df)

    descendant_assets = descendants_df.merge(aggregated_assets, on="entity_id", how="inner")
    root_assets = aggregated_assets[aggregated_assets["entity_id"] == root_entity_id].copy()
    root_assets["root_entity_id"] = root_entity_id
    root_assets["hierarchy_level"] = 0
    root_assets["entity_path"] = root_entity_id
    root_assets["entity_ownership_share"] = 1.0

    combined = pd.concat(
        [
            descendant_assets[
                [
                    "root_entity_id",
                    "entity_id",
                    "hierarchy_level",
                    "entity_path",
                    "entity_ownership_share",
                    "asset_id",
                    "asset_ownership_share",
                ]
            ],
            root_assets[
                [
                    "root_entity_id",
                    "entity_id",
                    "hierarchy_level",
                    "entity_path",
                    "entity_ownership_share",
                    "asset_id",
                    "asset_ownership_share",
                ]
            ],
        ],
        ignore_index=True,
    )

    final = combined.merge(
        asset_emissions_df[["asset_id", "emission_profile_id", "reporting_year", "total_tco2e"]],
        on="asset_id",
        how="inner",
    )
    final["compound_ownership_share"] = final.apply(_compound_share, axis=1)
    final["asset_emissions"] = final["total_tco2e"]
    final["attributed_emissions"] = final["compound_ownership_share"] * final["asset_emissions"]

    return (
        final[
            [
                "root_entity_id",
                "entity_id",
                "asset_id",
                "hierarchy_level",
                "entity_path",
                "entity_ownership_share",
                "asset_ownership_share",
                "compound_ownership_share",
                "asset_emissions",
                "attributed_emissions",
                "emission_profile_id",
                "reporting_year",
            ]
        ]
        .sort_values(["hierarchy_level", "asset_id", "entity_id"])
        .reset_index(drop=True)
    )


def build_path_examples(responsibility_df: pd.DataFrame, asset_id: str) -> pd.DataFrame:
    """Return trace rows for one selected asset."""

    filtered = responsibility_df[responsibility_df["asset_id"] == asset_id].copy()
    if filtered.empty:
        return pd.DataFrame(
            columns=[
                "root_entity_id",
                "asset_id",
                "entity_path",
                "entity_ownership_share",
                "asset_ownership_share",
                "compound_ownership_share",
                "attributed_emissions",
            ]
        )
    return (
        filtered[
            [
                "root_entity_id",
                "asset_id",
                "entity_path",
                "entity_ownership_share",
                "asset_ownership_share",
                "compound_ownership_share",
                "attributed_emissions",
            ]
        ]
        .sort_values(["compound_ownership_share", "root_entity_id"], ascending=[False, True])
        .reset_index(drop=True)
    )


def build_attributed_emission_relationships(responsibility_df: pd.DataFrame) -> pd.DataFrame:
    """Materialize root-entity to emission-profile attribution rows for Neo4j."""

    if responsibility_df.empty:
        return pd.DataFrame(
            columns=[
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
        )

    return (
        responsibility_df.rename(columns={"entity_id": "holder_entity_id"})[
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
        ]
        .sort_values(["root_entity_id", "asset_id", "holder_entity_id"])
        .reset_index(drop=True)
    )


def build_all_attributed_emission_relationships(
    entities_df: pd.DataFrame,
    entity_ownership_df: pd.DataFrame,
    asset_ownership_df: pd.DataFrame,
    asset_emissions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build attribution relationships for every root entity in the graph."""

    roots = find_root_entities(entities_df["entity_id"], entity_ownership_df)
    frames = []
    for root_entity_id in roots:
        descendants_df = trace_entity_hierarchy(entity_ownership_df, root_entity_id)
        responsibility_df = build_responsibility_table(
            root_entity_id,
            descendants_df,
            asset_ownership_df,
            asset_emissions_df,
        )
        frames.append(build_attributed_emission_relationships(responsibility_df))

    if not frames:
        return build_attributed_emission_relationships(pd.DataFrame())
    return pd.concat(frames, ignore_index=True).sort_values(
        ["root_entity_id", "asset_id", "holder_entity_id"]
    ).reset_index(drop=True)


def rank_root_entities(
    entities_df: pd.DataFrame,
    entity_ownership_df: pd.DataFrame,
    asset_ownership_df: pd.DataFrame,
    asset_emissions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Rank root entities by attributed emissions."""

    roots = find_root_entities(entities_df["entity_id"], entity_ownership_df)
    rows: list[dict[str, object]] = []
    entity_name_map = dict(zip(entities_df["entity_id"], entities_df["entity_name"], strict=True))
    for root_entity_id in roots:
        descendants_df = trace_entity_hierarchy(entity_ownership_df, root_entity_id)
        responsibility_df = build_responsibility_table(
            root_entity_id, descendants_df, asset_ownership_df, asset_emissions_df
        )
        rows.append(
            {
                "root_entity_id": root_entity_id,
                "root_entity_name": entity_name_map[root_entity_id],
                "total_attributed_emissions": responsibility_df["attributed_emissions"].sum(),
                "asset_count": responsibility_df["asset_id"].nunique(),
                "descendant_count": len(descendants_df),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["total_attributed_emissions", "root_entity_id"], ascending=[False, True]
    ).reset_index(drop=True)


def find_root_entities(entity_ids: Iterable[str], entity_ownership_df: pd.DataFrame) -> list[str]:
    """Return entities without incoming current ownership edges."""

    current_df = entity_ownership_df[entity_ownership_df["is_current"]].copy()
    parents = set(current_df["parent_entity_id"])
    children = set(current_df["child_entity_id"])
    return sorted(set(entity_ids) & (parents - children))


def compute_entity_centrality(entity_ownership_df: pd.DataFrame) -> pd.DataFrame:
    """Return lightweight weighted centrality metrics on the entity-only subgraph."""

    graph = nx.DiGraph()
    current_df = entity_ownership_df[entity_ownership_df["is_current"]].copy()
    for row in current_df.to_dict(orient="records"):
        weight = float(row["ownership_share"])
        graph.add_edge(
            row["parent_entity_id"],
            row["child_entity_id"],
            weight=weight,
            distance=1 / weight,
        )

    if graph.number_of_nodes() == 0:
        return pd.DataFrame(
            columns=[
                "entity_id",
                "weighted_out_degree",
                "weighted_in_degree",
                "betweenness_centrality",
            ]
        )

    betweenness = nx.betweenness_centrality(graph, weight="distance", normalized=True)
    weighted_out_degree = dict(graph.out_degree(weight="weight"))
    weighted_in_degree = dict(graph.in_degree(weight="weight"))

    rows = []
    for node in sorted(graph.nodes()):
        rows.append(
            {
                "entity_id": node,
                "weighted_out_degree": weighted_out_degree.get(node, 0.0),
                "weighted_in_degree": weighted_in_degree.get(node, 0.0),
                "betweenness_centrality": betweenness.get(node, 0.0),
            }
        )
    return pd.DataFrame(rows).sort_values("entity_id").reset_index(drop=True)


def _compound_share(row: pd.Series) -> float:
    if int(row["hierarchy_level"]) == 0:
        return float(row["asset_ownership_share"])
    return float(row["entity_ownership_share"]) * float(row["asset_ownership_share"])
