PYTHON ?= python3

.PHONY: install build analyze export lint test k8s-dry-run

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

k8s-dry-run:
	kubectl kustomize k8s >/dev/null
	@if kubectl cluster-info >/dev/null 2>&1; then \
		kubectl apply --dry-run=client -k k8s; \
	else \
		echo "No Kubernetes API server reachable; local manifest render succeeded."; \
		echo "Start minikube or kind to run kubectl dry-run validation."; \
	fi
