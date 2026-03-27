"""Load and validate the demo CSV fixtures."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ownership_graph.models.schemas import REQUIRED_COLUMNS, DemoTables


def load_demo_tables(data_dir: str | Path) -> DemoTables:
    """Load and validate the checked-in demo tables."""

    data_path = Path(data_dir)
    tables = {
        "entities": _read_csv(data_path / "entities.csv"),
        "assets": _read_csv(data_path / "assets.csv"),
        "entity_ownership": _read_csv(data_path / "entity_ownership.csv"),
        "asset_ownership": _read_csv(data_path / "asset_ownership.csv"),
        "asset_emissions": _read_csv(data_path / "asset_emissions.csv"),
    }

    for table_name, required_columns in REQUIRED_COLUMNS.items():
        _require_columns(table_name, tables[table_name], required_columns)

    entities = tables["entities"].copy()
    assets = tables["assets"].copy()
    entity_ownership = tables["entity_ownership"].copy()
    asset_ownership = tables["asset_ownership"].copy()
    asset_emissions = tables["asset_emissions"].copy()

    entity_ownership["ownership_share"] = _coerce_share_column(
        entity_ownership, "ownership_share", "entity_ownership"
    )
    asset_ownership["ownership_share"] = _coerce_share_column(
        asset_ownership, "ownership_share", "asset_ownership"
    )
    entity_ownership["is_current"] = entity_ownership["is_current"].map(_parse_bool)
    asset_emissions["reporting_year"] = pd.to_numeric(
        asset_emissions["reporting_year"], errors="raise"
    ).astype(int)
    for column in ["scope_1_tco2e", "scope_2_tco2e", "total_tco2e"]:
        asset_emissions[column] = pd.to_numeric(asset_emissions[column], errors="raise")

    _require_unique(entities, "entity_id", "entities")
    _require_unique(assets, "asset_id", "assets")
    _require_unique(asset_emissions, "emission_profile_id", "asset_emissions")
    _require_unique(asset_emissions, "asset_id", "asset_emissions")

    if entity_ownership["is_current"].isna().any():
        raise ValueError("entity_ownership.is_current contains unsupported boolean values")

    entity_ids = set(entities["entity_id"])
    asset_ids = set(assets["asset_id"])

    _require_subset(
        entity_ownership["parent_entity_id"],
        entity_ids,
        "entity_ownership.parent_entity_id",
    )
    _require_subset(
        entity_ownership["child_entity_id"],
        entity_ids,
        "entity_ownership.child_entity_id",
    )
    _require_subset(asset_ownership["entity_id"], entity_ids, "asset_ownership.entity_id")
    _require_subset(asset_ownership["asset_id"], asset_ids, "asset_ownership.asset_id")
    _require_subset(asset_emissions["asset_id"], asset_ids, "asset_emissions.asset_id")

    direct_owner_values = set(asset_ownership["owner_type"].astype(str).str.lower())
    if direct_owner_values != {"direct_owner"}:
        raise ValueError("asset_ownership.owner_type must contain only 'direct_owner' in this demo")

    return DemoTables(
        entities=entities.sort_values("entity_id").reset_index(drop=True),
        assets=assets.sort_values("asset_id").reset_index(drop=True),
        entity_ownership=entity_ownership.sort_values(
            ["parent_entity_id", "child_entity_id"]
        ).reset_index(drop=True),
        asset_ownership=asset_ownership.sort_values(
            ["entity_id", "asset_id", "asset_unit_id"]
        ).reset_index(drop=True),
        asset_emissions=asset_emissions.sort_values("asset_id").reset_index(drop=True),
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")
    return pd.read_csv(path)


def _require_columns(table_name: str, df: pd.DataFrame, required_columns: list[str]) -> None:
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"{table_name} is missing required columns: {missing_columns}")


def _coerce_share_column(df: pd.DataFrame, column: str, table_name: str) -> pd.Series:
    shares = pd.to_numeric(df[column], errors="raise")
    invalid_mask = (shares < 0) | (shares > 1)
    if invalid_mask.any():
        raise ValueError(f"{table_name}.{column} must stay within [0, 1]")
    return shares


def _parse_bool(value: object) -> bool | None:
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "y", "yes"}:
        return True
    if normalized in {"false", "0", "n", "no"}:
        return False
    return None


def _require_unique(df: pd.DataFrame, column: str, table_name: str) -> None:
    if df[column].duplicated().any():
        raise ValueError(f"{table_name}.{column} must be unique")


def _require_subset(series: pd.Series, allowed_values: set[str], field_name: str) -> None:
    missing_values = sorted(set(series) - allowed_values)
    if missing_values:
        raise ValueError(f"{field_name} contains unknown ids: {missing_values}")
