import pandas as pd
import pytest

from ownership_graph.ingest.loaders import load_demo_tables


def test_load_demo_tables_success(demo_tables):
    assert len(demo_tables.entities) == 6
    assert len(demo_tables.assets) == 4
    assert demo_tables.entity_ownership["is_current"].all()


def test_load_demo_tables_rejects_missing_column(tmp_sample_dir):
    assets = pd.read_csv(tmp_sample_dir / "assets.csv").drop(columns=["technology"])
    assets.to_csv(tmp_sample_dir / "assets.csv", index=False)

    with pytest.raises(ValueError, match="missing required columns"):
        load_demo_tables(tmp_sample_dir)


def test_load_demo_tables_rejects_invalid_share(tmp_sample_dir):
    asset_ownership = pd.read_csv(tmp_sample_dir / "asset_ownership.csv")
    asset_ownership.loc[0, "ownership_share"] = 1.2
    asset_ownership.to_csv(tmp_sample_dir / "asset_ownership.csv", index=False)

    with pytest.raises(ValueError, match="must stay within \\[0, 1\\]"):
        load_demo_tables(tmp_sample_dir)


def test_load_demo_tables_rejects_duplicate_asset_emission_profile(tmp_sample_dir):
    asset_emissions = pd.read_csv(tmp_sample_dir / "asset_emissions.csv")
    duplicated = asset_emissions.iloc[[0]].copy()
    duplicated["emission_profile_id"] = "EP_999"
    asset_emissions = pd.concat([asset_emissions, duplicated], ignore_index=True)
    asset_emissions.to_csv(tmp_sample_dir / "asset_emissions.csv", index=False)

    with pytest.raises(ValueError, match="asset_emissions.asset_id must be unique"):
        load_demo_tables(tmp_sample_dir)
