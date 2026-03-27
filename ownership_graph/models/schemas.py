"""Schema metadata and typed containers for demo tables."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(slots=True)
class DemoTables:
    """Validated source tables used across the demo pipeline."""

    entities: pd.DataFrame
    assets: pd.DataFrame
    entity_ownership: pd.DataFrame
    asset_ownership: pd.DataFrame
    asset_emissions: pd.DataFrame


@dataclass(slots=True)
class OutputArtifacts:
    """Key output locations written by the CLI."""

    normalized_dir: Path
    staged_dir: Path
    analysis_dir: Path
    neo4j_dir: Path


REQUIRED_COLUMNS = {
    "entities": ["entity_id", "entity_name", "country_iso3", "entity_kind"],
    "assets": ["asset_id", "asset_name", "sector", "sub_sector", "technology", "country_iso3"],
    "entity_ownership": [
        "parent_entity_id",
        "child_entity_id",
        "ownership_share",
        "as_of_date",
        "is_current",
    ],
    "asset_ownership": ["entity_id", "asset_id", "ownership_share", "owner_type", "asset_unit_id"],
    "asset_emissions": [
        "emission_profile_id",
        "asset_id",
        "reporting_year",
        "scope_1_tco2e",
        "scope_2_tco2e",
        "total_tco2e",
    ],
}
