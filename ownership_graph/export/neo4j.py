"""Write Neo4j bulk import artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from ownership_graph.analysis.attribution import (
    aggregate_direct_asset_ownership,
    build_attributed_emission_relationships,
)
from ownership_graph.models.schemas import DemoTables


def write_neo4j_exports(
    tables: DemoTables,
    output_dir: str | Path,
    responsibility_df: pd.DataFrame | None = None,
    attributed_df: pd.DataFrame | None = None,
) -> None:
    """Write label-specific node and relationship CSV files plus helper Cypher."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    attributed_emissions = attributed_df
    if attributed_emissions is None:
        attributed_emissions = build_attributed_emission_relationships(
            responsibility_df if responsibility_df is not None else pd.DataFrame()
        )

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
    attributed_emissions.rename(
        columns={
            "root_entity_id": ":START_ID(Entity-ID)",
            "emission_profile_id": ":END_ID(Emission-ID)",
        }
    ).assign(**{":TYPE": "ATTRIBUTED_EMISSIONS"}).to_csv(
        output_path / "attributed_emissions.csv",
        index=False,
    )

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


def load_analysis_artifacts(output_dir: str | Path) -> Mapping[str, pd.DataFrame]:
    """Load analysis CSVs required for Neo4j export/runtime operations."""

    analysis_path = Path(output_dir) / "analysis"
    responsibility_path = analysis_path / "responsibility_attribution.csv"
    attributed_path = analysis_path / "attributed_emission_relationships.csv"
    if not responsibility_path.exists():
        raise FileNotFoundError(
            "Missing required analysis artifact: "
            f"{responsibility_path}. Run ownership-graph-analyze first."
        )

    artifacts = {
        "responsibility_attribution": pd.read_csv(responsibility_path),
    }
    if attributed_path.exists():
        artifacts["attributed_emission_relationships"] = pd.read_csv(attributed_path)
    return artifacts


def _load_cypher() -> str:
    return """CREATE CONSTRAINT entity_id_unique IF NOT EXISTS
FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE;
CREATE CONSTRAINT asset_id_unique IF NOT EXISTS
FOR (a:Asset) REQUIRE a.asset_id IS UNIQUE;
CREATE CONSTRAINT emission_profile_id_unique IF NOT EXISTS
FOR (ep:EmissionProfile) REQUIRE ep.emission_profile_id IS UNIQUE;
CREATE INDEX entity_kind_index IF NOT EXISTS FOR (e:Entity) ON (e.entity_kind);
CREATE INDEX asset_sector_index IF NOT EXISTS FOR (a:Asset) ON (a.sector);
CREATE INDEX emission_reporting_year_index IF NOT EXISTS
FOR (ep:EmissionProfile) ON (ep.reporting_year);

LOAD CSV WITH HEADERS FROM 'file:///entities.csv' AS row
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

LOAD CSV WITH HEADERS FROM 'file:///attributed_emissions.csv' AS row
MATCH (root:Entity {entity_id: row.`:START_ID(Entity-ID)`})
MATCH (ep:EmissionProfile {emission_profile_id: row.`:END_ID(Emission-ID)`})
MERGE (root)-[r:ATTRIBUTED_EMISSIONS]->(ep)
SET r.asset_id = row.asset_id,
    r.holder_entity_id = row.holder_entity_id,
    r.entity_path = row.entity_path,
    r.entity_ownership_share = toFloat(row.entity_ownership_share),
    r.asset_ownership_share = toFloat(row.asset_ownership_share),
    r.compound_ownership_share = toFloat(row.compound_ownership_share),
    r.attributed_emissions = toFloat(row.attributed_emissions),
    r.reporting_year = toInteger(row.reporting_year);
"""


def _queries_cypher() -> str:
    return """// Trace responsibility for a selected asset.
MATCH (root:Entity)-[r:ATTRIBUTED_EMISSIONS]->(ep:EmissionProfile)
      <-[:HAS_EMISSIONS]-(asset:Asset {asset_id: 'AST_100'})
RETURN root.entity_id AS root_entity,
       r.holder_entity_id AS holder_entity,
       split(r.entity_path, '|') AS path_nodes,
       r.entity_ownership_share AS entity_path_share,
       r.asset_ownership_share AS asset_share,
       r.compound_ownership_share AS compound_share,
       r.attributed_emissions AS attributed_emissions
ORDER BY attributed_emissions DESC;

// Show the weighted ownership chain from a selected root entity.
MATCH (root:Entity {entity_id: 'ENT_001'})-[r:ATTRIBUTED_EMISSIONS]
      ->(ep:EmissionProfile)<-[:HAS_EMISSIONS]-(asset:Asset)
RETURN split(r.entity_path, '|') AS chain,
       r.entity_ownership_share AS entity_path_share,
       r.asset_ownership_share AS asset_share,
       r.compound_ownership_share AS compound_share,
       asset.asset_id AS asset_id
ORDER BY asset_id;

// Rank entities by attributed emissions.
MATCH (root:Entity)-[r:ATTRIBUTED_EMISSIONS]->(:EmissionProfile)
RETURN root.entity_id AS root_entity,
       sum(r.attributed_emissions) AS total_attributed_emissions
ORDER BY total_attributed_emissions DESC;
"""
