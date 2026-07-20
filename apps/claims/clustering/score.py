"""Composite clustering objective with degenerate-config guardrails.

Used by the cluster-tuning CLI so an agent can maximize a single scalar while
seeing raw metrics and penalty flags.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DEFAULT_WEIGHTS: dict[str, float] = {
    "cohesion": 0.25,
    "separation": 0.25,
    "query": 0.40,
    "coverage": 0.10,
}

DEFAULT_GUARDS: dict[str, float] = {
    "max_largest_share": 0.5,
    "max_singleton_frac": 0.5,
    "min_coverage": 0.5,
}

DEFAULT_HIERARCHY_WEIGHTS: dict[str, float] = {
    "leaf_cohesion": 0.30,
    "leaf_coverage": 0.15,
    "narrative_query": 0.40,
    "narrative_separation": 0.15,
}

DEFAULT_HIERARCHY_GUARDS: dict[str, float] = {
    "max_narrative_largest_share": 0.5,
    "max_leaf_singleton_frac": 0.5,
    "min_leaf_coverage": 0.5,
}


@dataclass(frozen=True)
class ObjectiveWeights:
    cohesion: float = 0.25
    separation: float = 0.25
    query: float = 0.40
    coverage: float = 0.10

    def as_dict(self) -> dict[str, float]:
        return {
            "cohesion": self.cohesion,
            "separation": self.separation,
            "query": self.query,
            "coverage": self.coverage,
        }


@dataclass(frozen=True)
class ObjectiveGuards:
    max_largest_share: float = 0.5
    max_singleton_frac: float = 0.5
    min_coverage: float = 0.5

    def as_dict(self) -> dict[str, float]:
        return {
            "max_largest_share": self.max_largest_share,
            "max_singleton_frac": self.max_singleton_frac,
            "min_coverage": self.min_coverage,
        }


@dataclass(frozen=True)
class HierarchyWeights:
    leaf_cohesion: float = 0.30
    leaf_coverage: float = 0.15
    narrative_query: float = 0.40
    narrative_separation: float = 0.15

    def as_dict(self) -> dict[str, float]:
        return {
            "leaf_cohesion": self.leaf_cohesion,
            "leaf_coverage": self.leaf_coverage,
            "narrative_query": self.narrative_query,
            "narrative_separation": self.narrative_separation,
        }


@dataclass(frozen=True)
class HierarchyGuards:
    max_narrative_largest_share: float = 0.5
    max_leaf_singleton_frac: float = 0.5
    min_leaf_coverage: float = 0.5

    def as_dict(self) -> dict[str, float]:
        return {
            "max_narrative_largest_share": self.max_narrative_largest_share,
            "max_leaf_singleton_frac": self.max_leaf_singleton_frac,
            "min_leaf_coverage": self.min_leaf_coverage,
        }


def _clip01(value: float | None, *, default: float = 0.0) -> float:
    if value is None:
        return default
    return float(max(0.0, min(1.0, value)))


def derived_cluster_shape(metrics: dict[str, Any]) -> dict[str, float]:
    """Compute largest_cluster_share and singleton_frac from metrics payload.

    Prefer ``shape_from_labels`` when full labels are available: metrics may
    truncate ``per_cluster`` to the largest 20 clusters.
    """
    per = list(metrics.get("per_cluster") or [])
    n_points = int(metrics.get("n_points") or 0)
    n_noise = int(metrics.get("n_noise") or 0)
    assigned = max(n_points - n_noise, 0)
    if assigned <= 0 or not per:
        return {"largest_cluster_share": 0.0, "singleton_frac": 0.0}

    sizes = [int(row.get("size") or 0) for row in per]
    largest = max(sizes) if sizes else 0
    singletons = sum(1 for s in sizes if s == 1)
    n_clusters = max(int(metrics.get("n_clusters") or len(sizes)), 1)
    return {
        "largest_cluster_share": round(largest / assigned, 4),
        "singleton_frac": round(singletons / n_clusters, 4),
    }


def shape_from_labels(labels: Any) -> dict[str, float]:
    """largest_cluster_share / singleton_frac from a full labels array."""
    import numpy as np

    arr = np.asarray(labels, dtype=int)
    if arr.size == 0:
        return {"largest_cluster_share": 0.0, "singleton_frac": 0.0}
    assigned = arr[arr != -1]
    if assigned.size == 0:
        return {"largest_cluster_share": 0.0, "singleton_frac": 0.0}
    _, counts = np.unique(assigned, return_counts=True)
    n_clusters = int(counts.size)
    largest = int(counts.max()) if n_clusters else 0
    singletons = int(np.sum(counts == 1))
    return {
        "largest_cluster_share": round(largest / int(assigned.size), 4),
        "singleton_frac": round(singletons / max(n_clusters, 1), 4),
    }


def compute_objective(
    metrics: dict[str, Any],
    *,
    eval_score: float | None,
    weights: ObjectiveWeights | dict[str, float] | None = None,
    guards: ObjectiveGuards | dict[str, float] | None = None,
    largest_cluster_share: float | None = None,
    singleton_frac: float | None = None,
) -> dict[str, Any]:
    """Return objective scalar plus components, penalties, and flags.

    Args:
        metrics: Output of ``cluster_metrics.compute_cluster_metrics`` (or compatible).
        eval_score: ``mean_dominant_cluster_share`` from the query eval suite.
        weights: Component weights (cohesion/separation/query/coverage).
        guards: Degenerate-config thresholds.
        largest_cluster_share: Optional override; else derived from metrics.
        singleton_frac: Optional override; else derived from metrics.
    """
    if isinstance(weights, dict):
        w = ObjectiveWeights(
            cohesion=float(weights.get("cohesion", DEFAULT_WEIGHTS["cohesion"])),
            separation=float(weights.get("separation", DEFAULT_WEIGHTS["separation"])),
            query=float(weights.get("query", DEFAULT_WEIGHTS["query"])),
            coverage=float(weights.get("coverage", DEFAULT_WEIGHTS["coverage"])),
        )
    else:
        w = weights or ObjectiveWeights()

    if isinstance(guards, dict):
        g = ObjectiveGuards(
            max_largest_share=float(guards.get("max_largest_share", DEFAULT_GUARDS["max_largest_share"])),
            max_singleton_frac=float(guards.get("max_singleton_frac", DEFAULT_GUARDS["max_singleton_frac"])),
            min_coverage=float(guards.get("min_coverage", DEFAULT_GUARDS["min_coverage"])),
        )
    else:
        g = guards or ObjectiveGuards()

    shape = derived_cluster_shape(metrics)
    largest_share = float(
        largest_cluster_share if largest_cluster_share is not None else shape["largest_cluster_share"]
    )
    singleton = float(singleton_frac if singleton_frac is not None else shape["singleton_frac"])

    cohesion = _clip01(metrics.get("mean_intra_cosine"))
    sil = metrics.get("mean_silhouette_cosine")
    separation = _clip01(((float(sil) + 1.0) / 2.0) if sil is not None else None, default=0.5)
    query_cohesion = _clip01(eval_score)
    coverage = _clip01(
        (float(metrics.get("coverage_pct") or 0.0) / 100.0) if metrics.get("coverage_pct") is not None else None
    )

    components = {
        "cohesion": round(cohesion, 4),
        "separation": round(separation, 4),
        "query_cohesion": round(query_cohesion, 4),
        "coverage": round(coverage, 4),
    }

    objective_base = (
        w.cohesion * cohesion
        + w.separation * separation
        + w.query * query_cohesion
        + w.coverage * coverage
    )

    flags: list[str] = []
    penalties: dict[str, float] = {}
    n_clusters = int(metrics.get("n_clusters") or 0)

    if n_clusters < 2:
        flags.append("degenerate_too_few")
        return {
            "objective": 0.0,
            "objective_base": round(float(objective_base), 4),
            "components": components,
            "penalties": {"degenerate_too_few": 1.0},
            "flags": flags,
            "largest_cluster_share": round(largest_share, 4),
            "singleton_frac": round(singleton, 4),
            "weights": w.as_dict(),
            "guards": g.as_dict(),
        }

    penalty_total = 0.0
    if largest_share > g.max_largest_share:
        # Scale from 0 at threshold to 1 when share == 1.
        denom = max(1.0 - g.max_largest_share, 1e-9)
        p = (largest_share - g.max_largest_share) / denom
        p = float(max(0.0, min(1.0, p)))
        penalties["giant_cluster"] = round(p, 4)
        flags.append("giant_cluster")
        penalty_total += 0.5 * p

    if singleton > g.max_singleton_frac:
        denom = max(1.0 - g.max_singleton_frac, 1e-9)
        p = (singleton - g.max_singleton_frac) / denom
        p = float(max(0.0, min(1.0, p)))
        penalties["too_many_singletons"] = round(p, 4)
        flags.append("too_many_singletons")
        penalty_total += 0.35 * p

    if coverage < g.min_coverage:
        denom = max(g.min_coverage, 1e-9)
        p = (g.min_coverage - coverage) / denom
        p = float(max(0.0, min(1.0, p)))
        penalties["low_coverage"] = round(p, 4)
        flags.append("low_coverage")
        penalty_total += 0.4 * p

    objective = max(0.0, float(objective_base) - penalty_total)
    return {
        "objective": round(objective, 4),
        "objective_base": round(float(objective_base), 4),
        "components": components,
        "penalties": penalties,
        "flags": flags,
        "largest_cluster_share": round(largest_share, 4),
        "singleton_frac": round(singleton, 4),
        "weights": w.as_dict(),
        "guards": g.as_dict(),
    }


def compute_hierarchy_objective(
    leaf_metrics: dict[str, Any],
    narrative_metrics: dict[str, Any],
    *,
    eval_score: float | None,
    weights: HierarchyWeights | dict[str, float] | None = None,
    guards: HierarchyGuards | dict[str, float] | None = None,
    leaf_singleton_frac: float | None = None,
    narrative_largest_share: float | None = None,
) -> dict[str, Any]:
    """Two-level objective: leaf tightness/coverage + narrative query/separation.

    Args:
        leaf_metrics: ``compute_cluster_metrics`` on leaf labels.
        narrative_metrics: ``compute_cluster_metrics`` on point-level narrative labels.
        eval_score: mean dominant-cluster share against narrative labels.
        weights / guards: optional overrides.
        leaf_singleton_frac / narrative_largest_share: optional shape overrides.
    """
    if isinstance(weights, dict):
        w = HierarchyWeights(
            leaf_cohesion=float(weights.get("leaf_cohesion", DEFAULT_HIERARCHY_WEIGHTS["leaf_cohesion"])),
            leaf_coverage=float(weights.get("leaf_coverage", DEFAULT_HIERARCHY_WEIGHTS["leaf_coverage"])),
            narrative_query=float(
                weights.get("narrative_query", DEFAULT_HIERARCHY_WEIGHTS["narrative_query"])
            ),
            narrative_separation=float(
                weights.get("narrative_separation", DEFAULT_HIERARCHY_WEIGHTS["narrative_separation"])
            ),
        )
    else:
        w = weights or HierarchyWeights()

    if isinstance(guards, dict):
        g = HierarchyGuards(
            max_narrative_largest_share=float(
                guards.get(
                    "max_narrative_largest_share",
                    DEFAULT_HIERARCHY_GUARDS["max_narrative_largest_share"],
                )
            ),
            max_leaf_singleton_frac=float(
                guards.get(
                    "max_leaf_singleton_frac",
                    DEFAULT_HIERARCHY_GUARDS["max_leaf_singleton_frac"],
                )
            ),
            min_leaf_coverage=float(
                guards.get("min_leaf_coverage", DEFAULT_HIERARCHY_GUARDS["min_leaf_coverage"])
            ),
        )
    else:
        g = guards or HierarchyGuards()

    leaf_shape = derived_cluster_shape(leaf_metrics)
    nar_shape = derived_cluster_shape(narrative_metrics)
    leaf_singleton = float(
        leaf_singleton_frac if leaf_singleton_frac is not None else leaf_shape["singleton_frac"]
    )
    nar_largest = float(
        narrative_largest_share
        if narrative_largest_share is not None
        else nar_shape["largest_cluster_share"]
    )

    leaf_cohesion = _clip01(leaf_metrics.get("mean_intra_cosine"))
    leaf_coverage = _clip01(
        (float(leaf_metrics.get("coverage_pct") or 0.0) / 100.0)
        if leaf_metrics.get("coverage_pct") is not None
        else None
    )
    narrative_query = _clip01(eval_score)
    sil = narrative_metrics.get("mean_silhouette_cosine")
    narrative_separation = _clip01(
        ((float(sil) + 1.0) / 2.0) if sil is not None else None,
        default=0.5,
    )

    components = {
        "leaf_cohesion": round(leaf_cohesion, 4),
        "leaf_coverage": round(leaf_coverage, 4),
        "narrative_query": round(narrative_query, 4),
        "narrative_separation": round(narrative_separation, 4),
    }

    objective_base = (
        w.leaf_cohesion * leaf_cohesion
        + w.leaf_coverage * leaf_coverage
        + w.narrative_query * narrative_query
        + w.narrative_separation * narrative_separation
    )

    flags: list[str] = []
    penalties: dict[str, float] = {}
    n_leaves = int(leaf_metrics.get("n_clusters") or 0)
    n_narratives = int(narrative_metrics.get("n_clusters") or 0)

    if n_leaves < 2 or n_narratives < 2:
        flags.append("degenerate_too_few")
        return {
            "objective": 0.0,
            "objective_base": round(float(objective_base), 4),
            "components": components,
            "penalties": {"degenerate_too_few": 1.0},
            "flags": flags,
            "leaf_singleton_frac": round(leaf_singleton, 4),
            "narrative_largest_share": round(nar_largest, 4),
            "weights": w.as_dict(),
            "guards": g.as_dict(),
        }

    penalty_total = 0.0
    if nar_largest > g.max_narrative_largest_share:
        denom = max(1.0 - g.max_narrative_largest_share, 1e-9)
        p = (nar_largest - g.max_narrative_largest_share) / denom
        p = float(max(0.0, min(1.0, p)))
        penalties["giant_narrative"] = round(p, 4)
        flags.append("giant_narrative")
        penalty_total += 0.5 * p

    if leaf_singleton > g.max_leaf_singleton_frac:
        denom = max(1.0 - g.max_leaf_singleton_frac, 1e-9)
        p = (leaf_singleton - g.max_leaf_singleton_frac) / denom
        p = float(max(0.0, min(1.0, p)))
        penalties["too_many_leaf_singletons"] = round(p, 4)
        flags.append("too_many_leaf_singletons")
        penalty_total += 0.35 * p

    if leaf_coverage < g.min_leaf_coverage:
        denom = max(g.min_leaf_coverage, 1e-9)
        p = (g.min_leaf_coverage - leaf_coverage) / denom
        p = float(max(0.0, min(1.0, p)))
        penalties["low_leaf_coverage"] = round(p, 4)
        flags.append("low_leaf_coverage")
        penalty_total += 0.4 * p

    objective = max(0.0, float(objective_base) - penalty_total)
    return {
        "objective": round(objective, 4),
        "objective_base": round(float(objective_base), 4),
        "components": components,
        "penalties": penalties,
        "flags": flags,
        "leaf_singleton_frac": round(leaf_singleton, 4),
        "narrative_largest_share": round(nar_largest, 4),
        "weights": w.as_dict(),
        "guards": g.as_dict(),
    }
