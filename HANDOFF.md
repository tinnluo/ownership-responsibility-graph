# HANDOFF

## Repo

- Target path: `/Users/lxt/Documents/portfolio/ownership-responsibility-graph`
- Intended public GitHub repo: `tinnlo/ownership-responsibility-graph`

## Mission

Build a public-safe knowledge graph demo that traces responsibility from emitting assets through ownership hierarchies to parent entities.

The repo should show:

- graph data modeling
- weighted ownership and attribution logic
- NetworkX-based graph construction and analysis
- Neo4j export and query readiness

This is not a generic graph-algorithms sandbox. It is a narrow portfolio demo for asset ownership, entity hierarchy, and responsibility tracing.

This handoff is based on the actual ownership-composition logic used in the source project notebook:

- `/Users/lxt/Documents/ForwardAnalytics/code/FA_data_pipeline/03_golden_layer_dw/entity_ownership/calculate_compound_ownership.ipynb`

and the reusable hierarchy traversal pattern in:

- `/Users/lxt/Documents/ForwardAnalytics/code/FA_data_pipeline/04_semantic_models/entity_subsidiary_aggregator.py`

## Portfolio Positioning

This repo should support the existing job-search story:

- climate-tech data architecture
- graph modeling for ownership and responsibility
- production-style system design, but with public-safe data

It should align with:

- `Job Seeking Plan 2026.md`
- `Near-Term Learning Plan.md`

The strongest message is:

- “I can model complex ownership networks, attribute responsibility through weighted graph paths, and move from in-memory graph analysis to graph-database deployment.”

## Public Framing

Use neutral, public-safe terminology:

- `entity`
- `asset`
- `ownership edge`
- `responsibility attribution`
- `emissions profile`
- `weighted path`

Do not include:

- `Forward Analytics`
- `FA`
- `fa_`
- any proprietary business vocabulary
- internal data schemas
- internal file paths
- internal storage or service URLs

## Source Logic To Preserve

The public demo should preserve these architectural ideas from the original notebook and traversal module, while rewriting the implementation cleanly:

- start from current entity-ownership relationships
- traverse subsidiaries recursively from a selected parent entity
- detect and stop on circular ownership paths
- join subsidiary entities to direct asset ownership records only
- include the parent entity's direct assets at hierarchy level `0`
- aggregate duplicate owner-asset rows by summing ownership shares
- join asset ownership to asset-level emissions
- compute compound responsibility from ownership percentages

The original compound-share logic is:

- if the parent directly owns the asset, use direct asset ownership share
- if a subsidiary owns the asset, use `entity_share * asset_share`

This demo may generalize naming, but it should preserve that core attribution pattern.

## Core Demo Story

The graph should answer three questions:

1. Which parent entities ultimately bear responsibility for a given emitting asset?
2. What weighted ownership chain links an asset to top-level entities?
3. Which entities concentrate the most attributed emissions across the graph?

Keep the demo narrow and architectural. Avoid adding UI or LLM features.

## Graph Model

Use a simple three-layer graph:

1. `Entity`
2. `Asset`
3. `EmissionProfile`

Expected edge patterns:

- `(:Entity)-[:OWNS_ENTITY {weight}]->(:Entity)`
- `(:Entity)-[:OWNS_ASSET {weight}]->(:Asset)`
- `(:Asset)-[:HAS_EMISSIONS]->(:EmissionProfile)`

Optional:

- `OPERATES_ASSET` if public sample data supports it cleanly

The weighting logic must be explicit and documented.

The most important public-safe graph interpretation is:

- `OWNS_ENTITY.weight` = entity ownership percentage
- `OWNS_ASSET.weight` = direct asset ownership percentage
- attributed emissions = emissions on the asset multiplied by the compound ownership share along the selected path

Do not hide this inside vague “graph scoring.” Show the arithmetic clearly.

## Implementation Scope

### Phase 1: Minimal runnable graph

Build a small public demo dataset and a reproducible pipeline that:

- loads public CSV fixtures
- normalizes entities, assets, and ownership edges
- builds a directed NetworkX graph
- computes recursive ownership tracing for at least one selected parent entity
- computes weighted responsibility tracing for at least one asset
- outputs human-readable example results

The first runnable slice should mirror the original notebook flow:

1. load entity ownership
2. load direct asset ownership
3. load asset emissions
4. find all subsidiaries for one parent
5. attach owned assets
6. compute compound share
7. attribute emissions

### Phase 2: Graph analytics

Add a focused analysis layer:

- upstream traversal from asset to parent entities
- weighted responsibility allocation
- top responsible entities by attributed emissions
- centrality metrics for selected entity nodes
- optional community detection if it stays clean and well-explained

Do not add many algorithms just for breadth.

Important: graph analytics are secondary to attribution logic.

The original source is strongest in:

- hierarchy traversal
- ownership aggregation
- compound responsibility attribution

Centrality and communities should only be added after that story is complete.

### Phase 3: Neo4j export

Add export artifacts so the graph can be loaded into Neo4j:

- node CSVs
- relationship CSVs
- optional Cypher load script
- example Cypher queries in the README

The point is to show the transition from NetworkX modeling to Neo4j-ready deployment.

## Suggested Repo Structure

```text
ownership-responsibility-graph/
├── README.md
├── HANDOFF.md
├── pyproject.toml
├── data/
│   ├── raw/
│   ├── sample/
│   └── output/
├── ownership_graph/
│   ├── ingest/
│   ├── graph/
│   ├── analysis/
│   ├── export/
│   └── models/
├── scripts/
│   ├── build_demo_graph.py
│   ├── run_analysis.py
│   └── export_neo4j.py
├── docs/
│   └── architecture.md
└── tests/
```

## Data Strategy

Prefer small checked-in public sample data.

Recommended source direction:

- public ownership / asset datasets such as GEM, or another open-access source
- if source licensing or formatting is awkward, create a hand-curated public demo sample derived from open data and document the derivation clearly

Data should be small enough that the repo runs locally with no credentials.

## Required Outputs

At minimum, generate:

- a normalized node list
- a normalized edge list
- one weighted responsibility table
- one traced path example
- one ranking of top responsible entities
- Neo4j-ready node and relationship files

Prefer outputs that mirror the original notebook’s useful columns in generalized form:

- `parent_entity_id`
- `entity_id`
- `asset_id`
- `hierarchy_level`
- `entity_ownership_share`
- `asset_ownership_share`
- `compound_ownership_share`
- `asset_emissions`
- `attributed_emissions`

## README Requirements

The README should be portfolio-grade and architecture-first.

Required sections:

- problem statement
- why this repo exists
- graph schema
- weighting and attribution logic
- why NetworkX first
- why Neo4j next
- local run steps
- sample outputs
- what was generalized or removed

Include three example query/use cases:

1. trace responsibility for one asset
2. show weighted ownership chain from a selected parent through subsidiaries to assets
3. rank top entities by attributed emissions

Also include a small section with example Cypher queries.

The README should explicitly mention that the public demo is inspired by a production ownership-attribution workflow, but rebuilt with public-safe data and terminology.

## Acceptance Criteria

The repo is ready when:

- it runs locally with public sample data only
- there are no company or internal-path leaks
- NetworkX graph construction is reproducible
- weighted attribution logic is documented and test-covered
- Neo4j export files are generated successfully
- README clearly explains architecture and tradeoffs
- one or two sample outputs are committed for portfolio readability

## Suggested Build Order

1. Create repo scaffold and Python package
2. Add small public sample dataset
3. Implement recursive hierarchy traversal with cycle protection
4. Build normalized entity and asset ownership joins
5. Implement compound ownership and attributed-emissions calculation
6. Build NetworkX graph and analysis outputs
7. Add Neo4j export
8. Write README and architecture doc
9. Add tests and public-safety keyword scrub

## What To Avoid

- no company-specific vocabulary
- no fake “production platform” claims
- no unnecessary frontend
- no agent framework features in this repo
- no overbuilt graph science section with weak narrative value

## Good Final Positioning

If implemented well, this repo should support statements like:

- “Built a public knowledge-graph demo for ownership and emissions responsibility tracing, from NetworkX-based modeling to Neo4j-ready deployment.”
- “Designed weighted attribution logic across entity ownership and asset relationships using public climate-related sample data.”
