import pytest

from ownership_graph.analysis.attribution import (
    aggregate_direct_asset_ownership,
    build_responsibility_table,
    rank_root_entities,
)
from ownership_graph.analysis.hierarchy import trace_entity_hierarchy


def test_aggregate_direct_asset_ownership_sums_duplicate_rows(demo_tables):
    aggregated = aggregate_direct_asset_ownership(demo_tables.asset_ownership)
    row = aggregated[
        (aggregated["entity_id"] == "ENT_011") & (aggregated["asset_id"] == "AST_100")
    ].iloc[0]
    assert row["asset_ownership_share"] == pytest.approx(0.6)


def test_build_responsibility_table_returns_expected_attribution(demo_tables):
    descendants = trace_entity_hierarchy(demo_tables.entity_ownership, "ENT_001")
    responsibility = build_responsibility_table(
        "ENT_001", descendants, demo_tables.asset_ownership, demo_tables.asset_emissions
    )

    totals = dict(
        zip(responsibility["asset_id"], responsibility["attributed_emissions"], strict=True)
    )
    assert totals["AST_130"] == pytest.approx(24.0)
    assert totals["AST_110"] == pytest.approx(72.0)
    assert totals["AST_100"] == pytest.approx(240.0)


def test_rank_root_entities_matches_expected_totals(demo_tables):
    ranking = rank_root_entities(
        demo_tables.entities,
        demo_tables.entity_ownership,
        demo_tables.asset_ownership,
        demo_tables.asset_emissions,
    )

    totals = dict(
        zip(ranking["root_entity_id"], ranking["total_attributed_emissions"], strict=True)
    )
    assert totals["ENT_001"] == pytest.approx(336.0)
    assert totals["ENT_040"] == pytest.approx(557.5)
    assert ranking.iloc[0]["root_entity_id"] == "ENT_040"
