"""Write Neo4j bulk import artifacts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ownership_graph.analysis.attribution import aggregate_direct_asset_ownership
from ownership_graph.models.schemas import DemoTables


def write_neo4j_exports(tables: DemoTables, output_dir: str | Path) -> None:
    """Write label-specific node and relationship CSV files plus helper Cypher."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    entity_nodes = tables.entities.rename(
        columns={
            "entity_id": "entity_id:ID(Entity-ID)",
            "entity_name": "name",
        }
    ).assign(**{":LABEL": "Entity"})
    asset_nodes = tables.assets.rename(
        columns={
            "asset_id": "asset_id:ID(Asset-ID)",
            "asset_name": "name",
        }
    ).assign(**{":LABEL": "Asset"})
    emission_nodes = tables.asset_emissions.rename(
        columns={
            "emission_profile_id": "emission_profile_id:ID(Emission-ID)",
            "asset_id": "asset_id",
            "reporting_year": "reporting_year:int",
            "scope_1_tco2e": "scope_1_tco2e:float",
            "scope_2_tco2e": "scope_2_tco2e:float",
            "total_tco2e": "total_tco2e:float",
        }
    ).assign(**{":LABEL": "EmissionProfile"})

    current_ownership = tables.entity_ownership[tables.entity_ownership["is_current"]].copy()
    owns_entity = current_ownership.rename(
        columns={
            "parent_entity_id": ":START_ID(Entity-ID)",
            "child_entity_id": ":END_ID(Entity-ID)",
            "ownership_share": "weight:float",
        }
    ).assign(**{":TYPE": "OWNS_ENTITY"})
    aggregated_assets = aggregate_direct_asset_ownership(tables.asset_ownership)
    owns_asset = aggregated_assets.rename(
        columns={
            "entity_id": ":START_ID(Entity-ID)",
            "asset_id": ":END_ID(Asset-ID)",
            "asset_ownership_share": "weight:float",
        }
    ).assign(**{":TYPE": "OWNS_ASSET"})
    has_emissions = tables.asset_emissions.assign(**{":TYPE": "HAS_EMISSIONS"}).rename(
        columns={
            "asset_id": ":START_ID(Asset-ID)",
            "emission_profile_id": ":END_ID(Emission-ID)",
        }
    )
    has_emissions["weight:float"] = 1.0

    entity_nodes.to_csv(output_path / "entities.csv", index=False)
    asset_nodes.to_csv(output_path / "assets.csv", index=False)
    emission_nodes.to_csv(output_path / "emission_profiles.csv", index=False)
    owns_entity.to_csv(output_path / "owns_entity.csv", index=False)
    owns_asset.to_csv(output_path / "owns_asset.csv", index=False)
    has_emissions[
        [":START_ID(Asset-ID)", ":END_ID(Emission-ID)", "weight:float", ":TYPE"]
    ].to_csv(output_path / "has_emissions.csv", index=False)

    (output_path / "load.cypher").write_text(_load_cypher(), encoding="utf-8")
    (output_path / "queries.cypher").write_text(_queries_cypher(), encoding="utf-8")


def load_tables_from_staged_dir(staged_dir: str | Path) -> DemoTables:
    """Load staged CSVs produced by the build step."""

    staged_path = Path(staged_dir)
    return DemoTables(
        entities=pd.read_csv(staged_path / "entities.csv"),
        assets=pd.read_csv(staged_path / "assets.csv"),
        entity_ownership=pd.read_csv(staged_path / "entity_ownership.csv"),
        asset_ownership=pd.read_csv(staged_path / "asset_ownership.csv"),
        asset_emissions=pd.read_csv(staged_path / "asset_emissions.csv"),
    )


def _load_cypher() -> str:
    return """LOAD CSV WITH HEADERS FROM 'file:///entities.csv' AS row
MERGE (e:Entity {entity_id: row.`entity_id:ID(Entity-ID)`})
SET e.name = row.name,
    e.country_iso3 = row.country_iso3,
    e.entity_kind = row.entity_kind;

LOAD CSV WITH HEADERS FROM 'file:///assets.csv' AS row
MERGE (a:Asset {asset_id: row.`asset_id:ID(Asset-ID)`})
SET a.name = row.name,
    a.sector = row.sector,
    a.sub_sector = row.sub_sector,
    a.technology = row.technology,
    a.country_iso3 = row.country_iso3;

LOAD CSV WITH HEADERS FROM 'file:///emission_profiles.csv' AS row
MERGE (ep:EmissionProfile {emission_profile_id: row.`emission_profile_id:ID(Emission-ID)`})
SET ep.asset_id = row.asset_id,
    ep.reporting_year = toInteger(row.`reporting_year:int`),
    ep.scope_1_tco2e = toFloat(row.`scope_1_tco2e:float`),
    ep.scope_2_tco2e = toFloat(row.`scope_2_tco2e:float`),
    ep.total_tco2e = toFloat(row.`total_tco2e:float`);

LOAD CSV WITH HEADERS FROM 'file:///owns_entity.csv' AS row
MATCH (parent:Entity {entity_id: row.`:START_ID(Entity-ID)`})
MATCH (child:Entity {entity_id: row.`:END_ID(Entity-ID)`})
MERGE (parent)-[r:OWNS_ENTITY]->(child)
SET r.weight = toFloat(row.`weight:float`);

LOAD CSV WITH HEADERS FROM 'file:///owns_asset.csv' AS row
MATCH (entity:Entity {entity_id: row.`:START_ID(Entity-ID)`})
MATCH (asset:Asset {asset_id: row.`:END_ID(Asset-ID)`})
MERGE (entity)-[r:OWNS_ASSET]->(asset)
SET r.weight = toFloat(row.`weight:float`);

LOAD CSV WITH HEADERS FROM 'file:///has_emissions.csv' AS row
MATCH (asset:Asset {asset_id: row.`:START_ID(Asset-ID)`})
MATCH (ep:EmissionProfile {emission_profile_id: row.`:END_ID(Emission-ID)`})
MERGE (asset)-[r:HAS_EMISSIONS]->(ep)
SET r.weight = toFloat(row.`weight:float`);
"""


def _queries_cypher() -> str:
    return """// Trace responsibility for a selected asset.
MATCH path = (root:Entity)-[:OWNS_ENTITY*0..10]->(holder:Entity)
             -[oa:OWNS_ASSET]->(asset:Asset {asset_id: 'AST_100'})
             -[:HAS_EMISSIONS]->(ep:EmissionProfile)
WITH root, holder, asset, ep, oa,
     relationships(path)[0..size(relationships(path)) - 1] AS entity_rels,
     [node IN nodes(path) |
      coalesce(node.entity_id, node.asset_id, node.emission_profile_id)] AS path_nodes
WITH root, holder, asset, ep, oa, path_nodes,
     reduce(weight = 1.0, rel IN entity_rels | weight * rel.weight) AS entity_path_share
RETURN root.entity_id AS root_entity,
       holder.entity_id AS holder_entity,
       path_nodes,
       entity_path_share,
       oa.weight AS asset_share,
       entity_path_share * oa.weight AS compound_share,
       entity_path_share * oa.weight * ep.total_tco2e AS attributed_emissions
ORDER BY attributed_emissions DESC;

// Show the weighted ownership chain from a selected root entity.
MATCH path = (root:Entity {entity_id: 'ENT_001'})-[:OWNS_ENTITY*0..10]->(holder:Entity)
             -[oa:OWNS_ASSET]->(asset:Asset)
WITH path, oa,
     relationships(path)[0..size(relationships(path)) - 1] AS entity_rels,
     [node IN nodes(path) | coalesce(node.entity_id, node.asset_id)] AS chain
RETURN chain,
       reduce(weight = 1.0, rel IN entity_rels | weight * rel.weight) AS entity_path_share,
       oa.weight AS asset_share,
       asset.asset_id AS asset_id
ORDER BY asset_id;

// Rank entities by attributed emissions.
MATCH path = (root:Entity)-[:OWNS_ENTITY*0..10]->(holder:Entity)
             -[oa:OWNS_ASSET]->(asset:Asset)
             -[:HAS_EMISSIONS]->(ep:EmissionProfile)
WITH root, oa, ep,
     relationships(path)[0..size(relationships(path)) - 1] AS entity_rels
WITH root,
     reduce(weight = 1.0, rel IN entity_rels | weight * rel.weight)
     * oa.weight
     * ep.total_tco2e AS attributed_emissions
RETURN root.entity_id AS root_entity,
       sum(attributed_emissions) AS total_attributed_emissions
ORDER BY total_attributed_emissions DESC;
"""
