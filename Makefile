PYTHON ?= python3

.PHONY: install build analyze export lint test

install:
	uv sync --extra dev

build:
	uv run ownership-graph-build --data-dir data/sample --output-dir data/output

analyze:
	uv run ownership-graph-analyze --data-dir data/sample --output-dir data/output --root-entity-id ENT_001 --asset-id AST_100

export:
	uv run ownership-graph-export-neo4j --input-dir data/output --output-dir data/output/neo4j

lint:
	uv run ruff check .

test:
	uv run pytest
