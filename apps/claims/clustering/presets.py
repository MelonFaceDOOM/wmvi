"""Named clustering presets (hierarchy / flat)."""

from __future__ import annotations

from typing import Any

# Best bake-off config from embedding_lab experiments (run 6).
HIERARCHY_PRESETS: dict[str, dict[str, Any]] = {
    "default": {
        "leaf_algorithm": "kmeans",
        "leaf_params": {"n_clusters": 800, "reduce": "none"},
        "narrative_algorithm": "agglomerative",
        "narrative_params": {"n_clusters": 25, "reduce": "none"},
    },
    "hdbscan-default": {
        "leaf_algorithm": "hdbscan",
        "leaf_params": {
            "reduce": "pca",
            "n_components": 50,
            "min_cluster_size": 8,
            "min_samples": 5,
        },
        "narrative_algorithm": "agglomerative",
        "narrative_params": {"n_clusters": 12, "reduce": "none"},
    },
}


def list_hierarchy_presets() -> list[str]:
    return sorted(HIERARCHY_PRESETS.keys())


def get_hierarchy_preset(name: str) -> dict[str, Any]:
    key = (name or "").strip().casefold()
    if key not in HIERARCHY_PRESETS:
        raise ValueError(
            f"Unknown hierarchy preset {name!r}; choose from {list_hierarchy_presets()}"
        )
    preset = HIERARCHY_PRESETS[key]
    return {
        "leaf_algorithm": preset["leaf_algorithm"],
        "leaf_params": dict(preset["leaf_params"]),
        "narrative_algorithm": preset["narrative_algorithm"],
        "narrative_params": dict(preset["narrative_params"]),
    }
