# Architecture

## Overview

This repo implements a narrow ownership-attribution pipeline:

```text
CSV fixtures
  -> validation and normalization
  -> recursive entity traversal
  -> direct asset ownership aggregation
  -> emissions attribution
  -> NetworkX graph construction
  -> Neo4j-ready export
```

## Data Flow

1. `load_demo_tables()` validates master tables, share ranges, uniqueness, and cross-table ids.
2. `trace_entity_hierarchy()` traverses current ownership edges from a selected root entity.
3. `aggregate_direct_asset_ownership()` collapses duplicate owner-asset rows.
4. `build_responsibility_table()` joins hierarchy rows to direct asset ownership and asset emissions.
5. `build_graph()` materializes the three-layer graph in NetworkX.
6. `build_attributed_emission_relationships()` materializes root-to-emission-profile attribution rows from the responsibility table.
7. `write_neo4j_exports()` writes CSV and Cypher artifacts for graph-database loading.
8. `ownership-graph-load-neo4j` can load the staged graph and attribution rows directly into a running Neo4j instance via the Neo4j Python driver.

## Ownership Arithmetic

For a selected root entity:

- `entity_ownership_share` = product of entity ownership weights along the path from root to holder
- `asset_ownership_share` = aggregated direct ownership share on the asset
- `compound_ownership_share`:
  `asset_ownership_share` when the root owns the asset directly
  `entity_ownership_share * asset_ownership_share` when a descendant owns the asset
- `attributed_emissions = compound_ownership_share * total_tco2e`

Example:

- `ENT_001 -> ENT_010 = 0.8`
- `ENT_010 -> ENT_011 = 0.5`
- `ENT_011 -> AST_100 = 0.35 + 0.25 = 0.6`
- `entity_ownership_share = 0.8 * 0.5 = 0.4`
- `compound_ownership_share = 0.4 * 0.6 = 0.24`
- `attributed_emissions = 0.24 * 1000 = 240`

## Graph Model

- Entity nodes represent parent and subsidiary entities.
- Asset nodes represent emitting assets.
- EmissionProfile nodes represent the emissions record used for attribution.

Edges:

- `OWNS_ENTITY.weight` = entity ownership share
- `OWNS_ASSET.weight` = direct asset ownership share
- `HAS_EMISSIONS.weight` = `1.0`
- `ATTRIBUTED_EMISSIONS.attributed_emissions` = precomputed root-level responsibility on an emission profile

## Deliberate Constraints

- The checked-in sample is tree-shaped.
- Cycles are detected and recursion stops before revisiting a node.
- Multiple active ownership paths from the same root to the same descendant are rejected in v1 because that would require a more advanced ownership-resolution model.
- `total_tco2e` is the attribution basis in v1 even though scope columns are preserved in the data model.

## Output Artifacts

- `data/output/normalized/nodes.csv`
- `data/output/normalized/edges.csv`
- `data/output/analysis/responsibility_attribution.csv`
- `data/output/analysis/attributed_emission_relationships.csv`
- `data/output/analysis/path_examples.csv`
- `data/output/analysis/top_responsible_entities.csv`
- `data/output/neo4j/*.csv`

## Design Rationale

The repo starts with DataFrame-based logic because the attribution math is easier to inspect and test there. The graph representation is added after the ownership tables are normalized so that graph edges mirror the same arithmetic used by the tabular pipeline.
