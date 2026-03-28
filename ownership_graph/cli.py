"""Command line entrypoints for the demo pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from ownership_graph.analysis.attribution import (
    build_all_attributed_emission_relationships,
    build_path_examples,
    build_responsibility_table,
    compute_entity_centrality,
    rank_root_entities,
)
from ownership_graph.analysis.hierarchy import trace_entity_hierarchy
from ownership_graph.export.neo4j import (
    load_analysis_artifacts,
    load_tables_from_staged_dir,
    write_neo4j_exports,
)
from ownership_graph.export.neo4j_runtime import (
    QUERY_DEFINITIONS,
    load_into_neo4j,
    load_runtime_bundle,
    resolve_connection_args,
    run_query_set,
    write_query_results,
)
from ownership_graph.graph.build import write_build_outputs
from ownership_graph.ingest.loaders import load_demo_tables


def build_main() -> None:
    parser = argparse.ArgumentParser(description="Build normalized graph artifacts.")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    tables = load_demo_tables(args.data_dir)
    write_build_outputs(tables, args.output_dir)


def analyze_main() -> None:
    parser = argparse.ArgumentParser(description="Run attribution analysis.")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--root-entity-id", required=True)
    parser.add_argument("--asset-id", required=True)
    args = parser.parse_args()

    tables = load_demo_tables(args.data_dir)
    output_dir = Path(args.output_dir) / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)

    descendants_df = trace_entity_hierarchy(tables.entity_ownership, args.root_entity_id)
    responsibility_df = build_responsibility_table(
        args.root_entity_id, descendants_df, tables.asset_ownership, tables.asset_emissions
    )
    path_examples_df = build_path_examples(responsibility_df, args.asset_id)
    attributed_relationships_df = build_all_attributed_emission_relationships(
        tables.entities,
        tables.entity_ownership,
        tables.asset_ownership,
        tables.asset_emissions,
    )
    top_roots_df = rank_root_entities(
        tables.entities, tables.entity_ownership, tables.asset_ownership, tables.asset_emissions
    )
    centrality_df = compute_entity_centrality(tables.entity_ownership)

    descendants_df.to_csv(output_dir / "descendants.csv", index=False)
    responsibility_df.to_csv(output_dir / "responsibility_attribution.csv", index=False)
    path_examples_df.to_csv(output_dir / "path_examples.csv", index=False)
    attributed_relationships_df.to_csv(
        output_dir / "attributed_emission_relationships.csv",
        index=False,
    )
    top_roots_df.to_csv(output_dir / "top_responsible_entities.csv", index=False)
    centrality_df.to_csv(output_dir / "entity_centrality.csv", index=False)


def export_main() -> None:
    parser = argparse.ArgumentParser(description="Write Neo4j import artifacts.")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    tables = load_tables_from_staged_dir(input_dir / "staged")
    artifacts = load_analysis_artifacts(input_dir)
    responsibility_df = artifacts["responsibility_attribution"]
    attributed_df = artifacts.get("attributed_emission_relationships")
    write_neo4j_exports(tables, args.output_dir, responsibility_df, attributed_df)


def load_neo4j_main() -> None:
    parser = argparse.ArgumentParser(description="Load the demo graph into Neo4j.")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--uri")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--database")
    parser.add_argument("--wipe", action="store_true")
    args = parser.parse_args()

    uri, username, password, database = resolve_connection_args(
        args.uri,
        args.username,
        args.password,
        args.database,
    )
    tables, _, attributed_df = load_runtime_bundle(args.input_dir)
    load_into_neo4j(
        tables,
        attributed_df,
        uri=uri,
        username=username,
        password=password,
        database=database,
        wipe=args.wipe,
    )


def query_neo4j_main() -> None:
    parser = argparse.ArgumentParser(description="Run canned Neo4j query sets.")
    parser.add_argument("--query-set", choices=sorted(QUERY_DEFINITIONS), required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--asset-id")
    parser.add_argument("--root-entity-id")
    parser.add_argument("--uri")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--database")
    args = parser.parse_args()

    uri, username, password, database = resolve_connection_args(
        args.uri,
        args.username,
        args.password,
        args.database,
    )
    df = run_query_set(
        args.query_set,
        uri=uri,
        username=username,
        password=password,
        database=database,
        asset_id=args.asset_id,
        root_entity_id=args.root_entity_id,
    )
    write_query_results(df, args.query_set, args.output_dir)
