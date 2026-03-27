from pathlib import Path

import pandas as pd
import pytest

from ownership_graph.ingest.loaders import load_demo_tables

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = ROOT / "data" / "sample"


@pytest.fixture()
def demo_tables():
    return load_demo_tables(SAMPLE_DIR)


@pytest.fixture()
def tmp_sample_dir(tmp_path: Path, demo_tables):
    sample_dir = tmp_path / "sample"
    sample_dir.mkdir()
    demo_tables.entities.to_csv(sample_dir / "entities.csv", index=False)
    demo_tables.assets.to_csv(sample_dir / "assets.csv", index=False)
    demo_tables.entity_ownership.to_csv(sample_dir / "entity_ownership.csv", index=False)
    demo_tables.asset_ownership.to_csv(sample_dir / "asset_ownership.csv", index=False)
    demo_tables.asset_emissions.to_csv(sample_dir / "asset_emissions.csv", index=False)
    return sample_dir


@pytest.fixture()
def read_csv():
    def _read(path: Path) -> pd.DataFrame:
        return pd.read_csv(path)

    return _read
