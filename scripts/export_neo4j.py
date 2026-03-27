import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    sys.path.insert(0, str(REPO_ROOT))
    from ownership_graph.cli import export_main

    export_main()


if __name__ == "__main__":
    main()
