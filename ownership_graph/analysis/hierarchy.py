"""Recursive ownership traversal for selected parent entities."""

from __future__ import annotations

from collections import defaultdict

import pandas as pd


def trace_entity_hierarchy(
    entity_ownership_df: pd.DataFrame, root_entity_id: str, max_depth: int = 10
) -> pd.DataFrame:
    """Return reachable descendants with cumulative entity ownership share."""

    current_df = entity_ownership_df[entity_ownership_df["is_current"]].copy()
    adjacency: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in current_df.to_dict(orient="records"):
        adjacency[str(row["parent_entity_id"])].append(row)

    rows: list[dict[str, object]] = []
    emitted_entities: dict[str, str] = {}

    def dfs(
        current_entity_id: str,
        cumulative_share: float,
        path: list[str],
        level: int,
        visited: set[str],
    ) -> None:
        if level > max_depth:
            raise ValueError(f"Traversal for {root_entity_id} exceeded max_depth={max_depth}")

        for edge in adjacency.get(current_entity_id, []):
            child_id = str(edge["child_entity_id"])
            if child_id in visited:
                continue

            child_share = cumulative_share * float(edge["ownership_share"])
            child_path = [*path, child_id]
            path_str = "|".join(child_path)

            existing_path = emitted_entities.get(child_id)
            if existing_path is not None and existing_path != path_str:
                raise ValueError(
                    "Multiple ownership paths detected for "
                    f"{child_id} under {root_entity_id}; v1 requires a single active path"
                )

            emitted_entities[child_id] = path_str
            rows.append(
                {
                    "root_entity_id": root_entity_id,
                    "entity_id": child_id,
                    "parent_entity_id": current_entity_id,
                    "hierarchy_level": level,
                    "entity_path": path_str,
                    "entity_ownership_share": round(child_share, 10),
                }
            )
            dfs(child_id, child_share, child_path, level + 1, {*visited, child_id})

    dfs(root_entity_id, 1.0, [root_entity_id], 1, {root_entity_id})

    if not rows:
        return pd.DataFrame(
            columns=[
                "root_entity_id",
                "entity_id",
                "parent_entity_id",
                "hierarchy_level",
                "entity_path",
                "entity_ownership_share",
            ]
        )

    return pd.DataFrame(rows).sort_values(["hierarchy_level", "entity_id"]).reset_index(drop=True)
