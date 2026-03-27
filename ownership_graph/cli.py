"""Command line entrypoints for the demo pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from ownership_graph.analysis.attribution import (
    build_path_examples,
    build_responsibility_table,
    compute_entity_centrality,
    rank_root_entities,
)
from ownership_graph.analysis.hierarchy import trace_entity_hierarchy
from ownership_graph.export.neo4j import load_tables_from_staged_dir, write_neo4j_exports
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
    top_roots_df = rank_root_entities(
        tables.entities, tables.entity_ownership, tables.asset_ownership, tables.asset_emissions
    )
    centrality_df = compute_entity_centrality(tables.entity_ownership)

    descendants_df.to_csv(output_dir / "descendants.csv", index=False)
    responsibility_df.to_csv(output_dir / "responsibility_attribution.csv", index=False)
    path_examples_df.to_csv(output_dir / "path_examples.csv", index=False)
    top_roots_df.to_csv(output_dir / "top_responsible_entities.csv", index=False)
    centrality_df.to_csv(output_dir / "entity_centrality.csv", index=False)


def export_main() -> None:
    parser = argparse.ArgumentParser(description="Write Neo4j import artifacts.")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    tables = load_tables_from_staged_dir(Path(args.input_dir) / "staged")
    write_neo4j_exports(tables, args.output_dir)
