# Goals and Features — Ownership Responsibility Graph

## Goal

Demonstrate weighted responsibility attribution through corporate ownership hierarchies — tracing from emitting assets through ownership chains to ultimate beneficial owners, with a dual-layer architecture: NetworkX as the source-of-truth computation layer and Neo4j as the graph-database deployment layer.

The repo shows how to model, traverse, and attribute emissions responsibility across complex multi-level ownership structures in a way that is deterministic, inspectable, and portable to a graph database.

## What This Solves

Corporate ownership structures frequently obscure who ultimately bears responsibility for asset-level emissions. Simple direct-ownership tables miss multi-hop chains; naive summation ignores proportional stakes. This repo shows the graph modelling and attribution arithmetic needed to resolve beneficial ownership correctly — and how to export that model into Neo4j for Cypher-based querying once the logic is stable.

---

## Features

### Three-Layer Directed Graph Schema

```
(:Entity)-[:OWNS_ENTITY {weight}]->(:Entity)
(:Entity)-[:OWNS_ASSET {weight}]->(:Asset)
(:Asset)-[:HAS_EMISSIONS {weight: 1.0}]->(:EmissionProfile)
```

Ownership weights are decimals in `[0, 1]`, representing proportional stakes.

### Recursive Ownership Traversal

BFS ancestor traversal from a selected root entity through the full ownership hierarchy:
- Visits all descendant entities recursively
- Cycle detection prevents infinite loops in circular ownership structures
- Traversal path is preserved for attribution tracing

### Compound Ownership Share Calculation

Attribution arithmetic accounts for multi-hop ownership dilution:

| Ownership type | Formula |
|---|---|
| Root direct asset | `compound_share = asset_ownership_share` |
| Descendant-owned asset | `compound_share = entity_ownership_share × asset_ownership_share` |
| Attributed emissions | `compound_share × total_tco2e` |

`entity_ownership_share` is the cumulative product of edge weights along the path from the root to the asset-holding entity.

### NetworkX Source-of-Truth Layer

NetworkX DiGraph is the primary computation layer:
- Deterministic local runs with no database dependency
- Explicit, inspectable attribution logic
- Path tracing and root-entity ranking
- Direct verification: tabular outputs match graph edge traversal

Outputs:
- `responsibility_attribution.csv` — per-asset attribution with path and compound share
- `descendants.csv` — full entity hierarchy under the selected root
- `path_examples.csv` — traced ownership chains for specific assets
- `top_responsible_entities.csv` — root entities ranked by total attributed emissions

### Neo4j Deployment Layer

Once the NetworkX model is validated, the graph exports to Neo4j:
- Label-specific node CSVs and relationship CSVs
- `load.cypher` helper script for bulk import
- Live Neo4j runtime path with driver-based loading
- Pre-built Cypher query set for three canonical queries:
  - `asset-trace` — responsibility trace for a specific asset
  - `root-chain` — weighted ownership chain from a root entity
  - `top-entities` — root entities ranked by attributed emissions

Query results are written to `data/output/neo4j_query_results/` as CSVs for offline inspection.

### Graph Analytics

Beyond attribution, the pipeline also computes:

- **Degree centrality** — identifies structurally important entities in the ownership network

### Docker + Multi-Service Compose

Three-service Docker Compose stack:

- `graph` — NetworkX build that writes staged graph artifacts
- `neo4j` — live Neo4j database
- `neo4j-loader` — driver-based loader that reads staged graph artifacts and precomputed attribution relationships from `data/output`

The Compose setup keeps NetworkX as the source of truth and loads current `data/output` artifacts into Neo4j for graph-database queries.

### Kubernetes Deployment

Kubernetes manifests under `k8s/` run the same demo flow with orchestration primitives:

- `neo4j` — single-replica StatefulSet with persistent data and logs
- `ownership-graph-build` — Job that writes staged graph artifacts to the shared `graph-output` PVC
- `ownership-graph-analyze` — Job that writes the attribution artifacts required by the loader
- `ownership-graph-load-neo4j` — Job that loads the graph into Neo4j over the in-cluster Bolt service

Credentials are provided through a Secret, Neo4j connection defaults through a ConfigMap, and the pipeline artifacts move across Jobs through the shared PVC.

### Verified Sample Outputs

Checked-in sample outputs with known correct attribution totals:
- `ENT_001` total attributed emissions: `336.0`
- `ENT_040` total attributed emissions: `557.5`
- `AST_100` split: `ENT_001 → ENT_010 → ENT_011 → AST_100 = 240`, `ENT_040 → AST_100 = 400`

These serve as regression anchors for the attribution logic.

---

## What This Repo Does Not Cover

- Document acquisition or filing retrieval (see `document-acquisition-workbench`)
- Lakehouse storage and SCD-tracked ownership history (see `entity-data-lakehouse`)
- Analytics UI over the ownership graph (see `entity-insight-studio`)
