"""Neo4j driver-backed load and query helpers."""

from __future__ import annotations

import os
from itertools import islice
from pathlib import Path
from typing import Iterable

import pandas as pd

from ownership_graph.analysis.attribution import build_attributed_emission_relationships
from ownership_graph.export.neo4j import load_analysis_artifacts, load_tables_from_staged_dir
from ownership_graph.models.schemas import DemoTables

DEFAULT_BATCH_SIZE = 1000
DEFAULT_DATABASE = "neo4j"


QUERY_DEFINITIONS = {
    "asset-trace": {
        "filename": "asset_trace.csv",
        "required_params": ("asset_id",),
        "cypher": """
MATCH (root:Entity)-[r:ATTRIBUTED_EMISSIONS]->(ep:EmissionProfile)
      <-[:HAS_EMISSIONS]-(asset:Asset {asset_id: $asset_id})
RETURN root.entity_id AS root_entity_id,
       r.holder_entity_id AS holder_entity_id,
       r.entity_path AS entity_path,
       r.entity_ownership_share AS entity_ownership_share,
       r.asset_ownership_share AS asset_ownership_share,
       r.compound_ownership_share AS compound_ownership_share,
       r.attributed_emissions AS attributed_emissions,
       asset.asset_id AS asset_id,
       ep.emission_profile_id AS emission_profile_id
ORDER BY attributed_emissions DESC, root_entity_id ASC
""",
    },
    "root-chain": {
        "filename": "root_chain.csv",
        "required_params": ("root_entity_id",),
        "cypher": """
MATCH (root:Entity {entity_id: $root_entity_id})-[r:ATTRIBUTED_EMISSIONS]
      ->(ep:EmissionProfile)<-[:HAS_EMISSIONS]-(asset:Asset)
RETURN root.entity_id AS root_entity_id,
       r.holder_entity_id AS holder_entity_id,
       r.entity_path AS entity_path,
       r.entity_ownership_share AS entity_ownership_share,
       r.asset_ownership_share AS asset_ownership_share,
       r.compound_ownership_share AS compound_ownership_share,
       r.attributed_emissions AS attributed_emissions,
       asset.asset_id AS asset_id,
       ep.emission_profile_id AS emission_profile_id
ORDER BY asset_id ASC, holder_entity_id ASC
""",
    },
    "top-entities": {
        "filename": "top_entities.csv",
        "required_params": (),
        "cypher": """
MATCH (root:Entity)-[r:ATTRIBUTED_EMISSIONS]->(:EmissionProfile)
RETURN root.entity_id AS root_entity_id,
       sum(r.attributed_emissions) AS total_attributed_emissions
ORDER BY total_attributed_emissions DESC, root_entity_id ASC
""",
    },
}


def load_runtime_bundle(input_dir: str | Path) -> tuple[DemoTables, pd.DataFrame, pd.DataFrame]:
    """Load staged and analysis outputs required for Neo4j runtime operations."""

    input_path = Path(input_dir)
    tables = load_tables_from_staged_dir(input_path / "staged")
    artifacts = load_analysis_artifacts(input_path)
    responsibility_df = artifacts["responsibility_attribution"]
    attributed_df = artifacts.get("attributed_emission_relationships")
    if attributed_df is None:
        attributed_df = build_attributed_emission_relationships(responsibility_df)
    return tables, responsibility_df, attributed_df


def resolve_connection_args(
    uri: str | None,
    username: str | None,
    password: str | None,
    database: str | None,
) -> tuple[str, str, str, str]:
    """Resolve Neo4j connection fields from CLI args or environment."""

    resolved_uri = uri or os.getenv("NEO4J_URI")
    resolved_username = username or os.getenv("NEO4J_USERNAME")
    resolved_password = password or os.getenv("NEO4J_PASSWORD")
    resolved_database = database or os.getenv("NEO4J_DATABASE") or DEFAULT_DATABASE

    missing = [
        name
        for name, value in [
            ("NEO4J_URI", resolved_uri),
            ("NEO4J_USERNAME", resolved_username),
            ("NEO4J_PASSWORD", resolved_password),
        ]
        if not value
    ]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Missing required Neo4j connection settings: {joined}")

    return resolved_uri, resolved_username, resolved_password, resolved_database


def load_into_neo4j(
    tables: DemoTables,
    attributed_df: pd.DataFrame,
    *,
    uri: str,
    username: str,
    password: str,
    database: str,
    wipe: bool = False,
) -> None:
    """Create schema and load the demo graph into a running Neo4j instance."""

    graph_database = _get_graph_database()
    driver = graph_database.driver(uri, auth=(username, password))
    try:
        driver.verify_connectivity()
        with driver.session(database=database) as session:
            if wipe:
                session.run("MATCH (n) DETACH DELETE n").consume()

            for statement in _schema_statements():
                session.run(statement).consume()

            _write_rows(session, _entity_rows(tables), _entity_query())
            _write_rows(session, _asset_rows(tables), _asset_query())
            _write_rows(session, _emission_rows(tables), _emission_query())
            _write_rows(session, _owns_entity_rows(tables), _owns_entity_query())
            _write_rows(session, _owns_asset_rows(tables), _owns_asset_query())
            _write_rows(session, _has_emissions_rows(tables), _has_emissions_query())
            _write_rows(
                session,
                attributed_df.to_dict(orient="records"),
                _attributed_emissions_query(),
            )
    finally:
        driver.close()


def run_query_set(
    query_set: str,
    *,
    uri: str,
    username: str,
    password: str,
    database: str,
    asset_id: str | None = None,
    root_entity_id: str | None = None,
) -> pd.DataFrame:
    """Execute one canned Neo4j query set and return the rows as a DataFrame."""

    if query_set not in QUERY_DEFINITIONS:
        raise ValueError(f"Unsupported query set: {query_set}")

    definition = QUERY_DEFINITIONS[query_set]
    params = {"asset_id": asset_id, "root_entity_id": root_entity_id}
    missing = [name for name in definition["required_params"] if not params[name]]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Missing required parameters for {query_set}: {joined}")

    graph_database = _get_graph_database()
    driver = graph_database.driver(uri, auth=(username, password))
    try:
        driver.verify_connectivity()
        with driver.session(database=database) as session:
            records = session.run(
                definition["cypher"],
                asset_id=asset_id,
                root_entity_id=root_entity_id,
            ).data()
        return pd.DataFrame(records)
    finally:
        driver.close()


def write_query_results(df: pd.DataFrame, query_set: str, output_dir: str | Path) -> Path:
    """Persist one query-set result table to disk."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / QUERY_DEFINITIONS[query_set]["filename"]
    df.to_csv(file_path, index=False)
    return file_path


def _schema_statements() -> list[str]:
    return [
        (
            "CREATE CONSTRAINT entity_id_unique IF NOT EXISTS "
            "FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE"
        ),
        (
            "CREATE CONSTRAINT asset_id_unique IF NOT EXISTS "
            "FOR (a:Asset) REQUIRE a.asset_id IS UNIQUE"
        ),
        (
            "CREATE CONSTRAINT emission_profile_id_unique IF NOT EXISTS "
            "FOR (ep:EmissionProfile) REQUIRE ep.emission_profile_id IS UNIQUE"
        ),
        "CREATE INDEX entity_kind_index IF NOT EXISTS FOR (e:Entity) ON (e.entity_kind)",
        "CREATE INDEX asset_sector_index IF NOT EXISTS FOR (a:Asset) ON (a.sector)",
        (
            "CREATE INDEX emission_reporting_year_index IF NOT EXISTS "
            "FOR (ep:EmissionProfile) ON (ep.reporting_year)"
        ),
    ]


def _write_rows(session, rows: list[dict[str, object]], query: str) -> None:
    for batch in _batched(rows, DEFAULT_BATCH_SIZE):
        session.run(query, rows=batch).consume()


def _batched(rows: list[dict[str, object]], size: int) -> Iterable[list[dict[str, object]]]:
    iterator = iter(rows)
    while batch := list(islice(iterator, size)):
        yield batch


def _entity_rows(tables: DemoTables) -> list[dict[str, object]]:
    return tables.entities.to_dict(orient="records")


def _asset_rows(tables: DemoTables) -> list[dict[str, object]]:
    return tables.assets.to_dict(orient="records")


def _emission_rows(tables: DemoTables) -> list[dict[str, object]]:
    return tables.asset_emissions.to_dict(orient="records")


def _owns_entity_rows(tables: DemoTables) -> list[dict[str, object]]:
    return tables.entity_ownership[tables.entity_ownership["is_current"]].to_dict(orient="records")


def _owns_asset_rows(tables: DemoTables) -> list[dict[str, object]]:
    from ownership_graph.analysis.attribution import aggregate_direct_asset_ownership

    return aggregate_direct_asset_ownership(tables.asset_ownership).to_dict(orient="records")


def _has_emissions_rows(tables: DemoTables) -> list[dict[str, object]]:
    return tables.asset_emissions[["asset_id", "emission_profile_id"]].to_dict(orient="records")


def _entity_query() -> str:
    return """
UNWIND $rows AS row
MERGE (e:Entity {entity_id: row.entity_id})
SET e.name = row.entity_name,
    e.country_iso3 = row.country_iso3,
    e.entity_kind = row.entity_kind
"""


def _asset_query() -> str:
    return """
UNWIND $rows AS row
MERGE (a:Asset {asset_id: row.asset_id})
SET a.name = row.asset_name,
    a.sector = row.sector,
    a.sub_sector = row.sub_sector,
    a.technology = row.technology,
    a.country_iso3 = row.country_iso3
"""


def _emission_query() -> str:
    return """
UNWIND $rows AS row
MERGE (ep:EmissionProfile {emission_profile_id: row.emission_profile_id})
SET ep.asset_id = row.asset_id,
    ep.reporting_year = toInteger(row.reporting_year),
    ep.scope_1_tco2e = toFloat(row.scope_1_tco2e),
    ep.scope_2_tco2e = toFloat(row.scope_2_tco2e),
    ep.total_tco2e = toFloat(row.total_tco2e)
"""


def _owns_entity_query() -> str:
    return """
UNWIND $rows AS row
MATCH (parent:Entity {entity_id: row.parent_entity_id})
MATCH (child:Entity {entity_id: row.child_entity_id})
MERGE (parent)-[r:OWNS_ENTITY]->(child)
SET r.weight = toFloat(row.ownership_share)
"""


def _owns_asset_query() -> str:
    return """
UNWIND $rows AS row
MATCH (entity:Entity {entity_id: row.entity_id})
MATCH (asset:Asset {asset_id: row.asset_id})
MERGE (entity)-[r:OWNS_ASSET]->(asset)
SET r.weight = toFloat(row.asset_ownership_share)
"""


def _has_emissions_query() -> str:
    return """
UNWIND $rows AS row
MATCH (asset:Asset {asset_id: row.asset_id})
MATCH (ep:EmissionProfile {emission_profile_id: row.emission_profile_id})
MERGE (asset)-[r:HAS_EMISSIONS]->(ep)
SET r.weight = 1.0
"""


def _attributed_emissions_query() -> str:
    return """
UNWIND $rows AS row
MATCH (root:Entity {entity_id: row.root_entity_id})
MATCH (ep:EmissionProfile {emission_profile_id: row.emission_profile_id})
MERGE (root)-[r:ATTRIBUTED_EMISSIONS]->(ep)
SET r.asset_id = row.asset_id,
    r.holder_entity_id = row.holder_entity_id,
    r.entity_path = row.entity_path,
    r.entity_ownership_share = toFloat(row.entity_ownership_share),
    r.asset_ownership_share = toFloat(row.asset_ownership_share),
    r.compound_ownership_share = toFloat(row.compound_ownership_share),
    r.attributed_emissions = toFloat(row.attributed_emissions),
    r.reporting_year = toInteger(row.reporting_year)
"""


def _get_graph_database():
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise RuntimeError(
            "Neo4j runtime features require the 'neo4j' package. "
            'Install the project dependencies with `pip install -e ".[dev]"` '
            "or `uv sync --extra dev`."
        ) from exc
    return GraphDatabase
