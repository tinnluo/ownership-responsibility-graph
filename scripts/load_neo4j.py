"""Thin wrapper for the Neo4j load CLI."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    sys.path.insert(0, str(REPO_ROOT))
    from ownership_graph.cli import load_neo4j_main

    load_neo4j_main()


if __name__ == "__main__":
    main()
