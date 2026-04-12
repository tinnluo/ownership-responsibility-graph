# Ownership Responsibility Graph

Public-safe knowledge graph demo for tracing responsibility from emitting assets through entity ownership hierarchies to parent entities.

## Problem Statement

Ownership structures often obscure who ultimately bears responsibility for asset-level emissions. This repo demonstrates a narrow, reproducible workflow that:

- models entity, asset, and emissions relationships as a graph
- traverses current ownership hierarchies recursively
- combines entity ownership and direct asset ownership into compound responsibility shares
- exports the result for both in-memory analysis with NetworkX and graph-database loading with Neo4j

## Why This Repo Exists

This is a portfolio repo for graph modeling, weighted attribution logic, and graph-ready data architecture. It is intentionally small, public-safe, and architecture-first.

The implementation is inspired by a production ownership-attribution workflow, but rebuilt here with public-safe terminology, hand-curated sample data, and no internal schemas or services.

## Graph Schema

The demo uses a three-layer directed graph:

- `(:Entity)-[:OWNS_ENTITY {weight}]->(:Entity)`
- `(:Entity)-[:OWNS_ASSET {weight}]->(:Asset)`
- `(:Asset)-[:HAS_EMISSIONS {weight: 1.0}]->(:EmissionProfile)`

Ownership weights are stored as decimals in `[0, 1]`.

## Weighting And Attribution Logic

The pipeline preserves four core behaviors:

1. Traverse current entity ownership recursively from a selected root entity.
2. Stop recursion when a cycle would repeat a node.
3. Join descendant entities to direct asset ownership only.
4. Aggregate duplicate owner-asset rows before attribution.

Attribution arithmetic:

- root direct asset: `compound_ownership_share = asset_ownership_share`
- descendant-owned asset: `compound_ownership_share = entity_ownership_share * asset_ownership_share`
- `attributed_emissions = compound_ownership_share * total_tco2e`

`entity_ownership_share` is the cumulative product of ownership weights along the entity path from the selected root to the asset-holding entity.

## Why NetworkX First

NetworkX is the first implementation layer because it keeps the attribution logic explicit and easy to inspect:

- deterministic local runs
- no database dependency for the core workflow
- simple path tracing and root-entity ranking
- direct comparison between tabular outputs and graph edges

## Why Neo4j Next

Neo4j export is included to show the next deployment step once the model is stable:

- label-specific node CSVs
- relationship CSVs
- a `load.cypher` helper script
- example graph queries for responsibility tracing
- a live Neo4j runtime path with driver-based loading and canned query commands

## Local Run Steps

```bash
uv sync --extra dev
uv run ownership-graph-build --data-dir data/sample --output-dir data/output
uv run ownership-graph-analyze --data-dir data/sample --output-dir data/output --root-entity-id ENT_001 --asset-id AST_100
uv run ownership-graph-export-neo4j --input-dir data/output --output-dir data/output/neo4j
uv run ownership-graph-load-neo4j --input-dir data/output --uri bolt://localhost:7687 --username neo4j --password neo4jpassword --database neo4j --wipe
uv run ownership-graph-query-neo4j --query-set top-entities --output-dir data/output/neo4j_query_results --uri bolt://localhost:7687 --username neo4j --password neo4jpassword --database neo4j
```

If `uv` is unavailable, install the package in editable mode and run the same scripts with `python3`.

## Quick Start with Docker

Build the image:

```bash
docker build -t ownership-responsibility-graph .
```

Run the default graph build with Docker Compose:

```bash
docker compose up --build
```

The compose service mounts `data/` and `scripts/` from the host, so generated outputs land in the host `data/output/` directory.

To start Neo4j locally and load the current outputs:

```bash
cp .env.example .env
docker compose up --build graph neo4j neo4j-loader
```

That workflow keeps NetworkX as the source of truth, then loads the staged graph and the precomputed attribution relationships into Neo4j. The database is available at `bolt://localhost:7687` and the Browser UI is at `http://localhost:7474`.

## Kubernetes deployment

Kubernetes manifests are available under [`k8s/`](k8s/). They run Neo4j as a StatefulSet, run build and analysis Jobs against a shared `graph-output` PersistentVolumeClaim, then load the staged graph and precomputed attribution relationships with a loader Job.

Prerequisites:

- minikube or kind
- `kubectl`
- a local `ownership-responsibility-graph:latest` image loaded into the cluster

```bash
docker build -t ownership-responsibility-graph:latest .
minikube image load ownership-responsibility-graph:latest
# For a named kind cluster: kind load docker-image ownership-responsibility-graph:latest --name ownership-graph

kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/neo4j/pvc.yaml
kubectl apply -f k8s/neo4j/service.yaml
kubectl apply -f k8s/neo4j/statefulset.yaml
kubectl rollout status statefulset/neo4j -n ownership-graph --timeout=180s

kubectl apply -f k8s/graph/job.yaml
kubectl wait --for=condition=complete job/ownership-graph-build -n ownership-graph --timeout=120s

kubectl apply -f k8s/graph/analyze-job.yaml
kubectl wait --for=condition=complete job/ownership-graph-analyze -n ownership-graph --timeout=120s

kubectl apply -f k8s/neo4j-loader/job.yaml
kubectl wait --for=condition=complete job/ownership-graph-load-neo4j -n ownership-graph --timeout=180s
```

See [`k8s/README.md`](k8s/README.md) for dry-run validation, monitoring, port-forwarding, and teardown commands.

## Sample Outputs

The checked-in demo fixture produces:

- `ENT_001` total attributed emissions: `336.0`
- `ENT_040` total attributed emissions: `557.5`
- `AST_100` attribution split:
  `ENT_001 -> ENT_010 -> ENT_011 -> AST_100 = 0.4 * 0.6 * 1000 = 240`
  `ENT_040 -> AST_100 = 0.4 * 1000 = 400`

Committed example artifacts:

- [`docs/examples/responsibility_attribution_sample.csv`](docs/examples/responsibility_attribution_sample.csv)
- [`docs/examples/top_responsible_entities_sample.csv`](docs/examples/top_responsible_entities_sample.csv)

## Example Use Cases

1. Trace responsibility for one asset by inspecting `path_examples.csv` for `AST_100`.
2. Show the weighted ownership chain from `ENT_001` through subsidiaries to assets via `descendants.csv` and `responsibility_attribution.csv`.
3. Rank top entities by attributed emissions using `top_responsible_entities.csv`.

## Example Cypher Queries

The Neo4j export writes `queries.cypher` with examples for:

- tracing responsibility for `AST_100`
- showing weighted ownership chains from `ENT_001`
- ranking root entities by attributed emissions

The live Neo4j query command writes CSV result sets under `data/output/neo4j_query_results/` for:

- `asset-trace`
- `root-chain`
- `top-entities`

## Repo Layout

```text
ownership-responsibility-graph/
├── data/sample/
├── docs/
├── ownership_graph/
├── scripts/
└── tests/
```
