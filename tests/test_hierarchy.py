import pandas as pd
import pytest

from ownership_graph.analysis.hierarchy import trace_entity_hierarchy


def test_trace_entity_hierarchy_returns_expected_rows(demo_tables):
    descendants = trace_entity_hierarchy(demo_tables.entity_ownership, "ENT_001")

    assert descendants["entity_id"].tolist() == ["ENT_010", "ENT_020", "ENT_011"]
    shares = dict(zip(descendants["entity_id"], descendants["entity_ownership_share"], strict=True))
    assert shares["ENT_010"] == pytest.approx(0.8)
    assert shares["ENT_020"] == pytest.approx(0.6)
    assert shares["ENT_011"] == pytest.approx(0.4)


def test_trace_entity_hierarchy_cuts_off_cycles():
    ownership = pd.DataFrame(
        [
            {
                "parent_entity_id": "ENT_A",
                "child_entity_id": "ENT_B",
                "ownership_share": 0.8,
                "as_of_date": "2026-01-01",
                "is_current": True,
            },
            {
                "parent_entity_id": "ENT_B",
                "child_entity_id": "ENT_A",
                "ownership_share": 0.5,
                "as_of_date": "2026-01-01",
                "is_current": True,
            },
        ]
    )

    descendants = trace_entity_hierarchy(ownership, "ENT_A")
    assert descendants["entity_id"].tolist() == ["ENT_B"]


def test_trace_entity_hierarchy_rejects_multiple_paths():
    ownership = pd.DataFrame(
        [
            {
                "parent_entity_id": "ENT_A",
                "child_entity_id": "ENT_B",
                "ownership_share": 0.5,
                "as_of_date": "2026-01-01",
                "is_current": True,
            },
            {
                "parent_entity_id": "ENT_A",
                "child_entity_id": "ENT_C",
                "ownership_share": 0.5,
                "as_of_date": "2026-01-01",
                "is_current": True,
            },
            {
                "parent_entity_id": "ENT_B",
                "child_entity_id": "ENT_D",
                "ownership_share": 0.5,
                "as_of_date": "2026-01-01",
                "is_current": True,
            },
            {
                "parent_entity_id": "ENT_C",
                "child_entity_id": "ENT_D",
                "ownership_share": 0.5,
                "as_of_date": "2026-01-01",
                "is_current": True,
            },
        ]
    )

    with pytest.raises(ValueError, match="Multiple ownership paths"):
        trace_entity_hierarchy(ownership, "ENT_A")
