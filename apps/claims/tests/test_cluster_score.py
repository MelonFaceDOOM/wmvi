"""Tests for apps.claims.clustering.score."""

from __future__ import annotations

from apps.claims.clustering.score import (
    ObjectiveGuards,
    ObjectiveWeights,
    compute_hierarchy_objective,
    compute_objective,
    shape_from_labels,
)
import numpy as np


def test_compute_objective_basic():
    metrics = {
        "mean_intra_cosine": 0.8,
        "mean_silhouette_cosine": 0.4,
        "coverage_pct": 90.0,
    }
    score = compute_objective(
        metrics,
        eval_score=0.7,
        weights=ObjectiveWeights(),
        guards=ObjectiveGuards(),
        largest_cluster_share=0.2,
        singleton_frac=0.1,
    )
    assert "objective" in score
    assert 0.0 <= float(score["objective"]) <= 1.5


def test_shape_from_labels():
    labels = np.array([0, 0, 0, 1, 1, -1], dtype=int)
    shape = shape_from_labels(labels)
    assert shape["largest_cluster_share"] > 0
    assert "singleton_frac" in shape


def test_hierarchy_objective():
    leaf = {"mean_intra_cosine": 0.85, "coverage_pct": 80.0, "mean_silhouette_cosine": 0.3}
    nar = {"mean_intra_cosine": 0.6, "coverage_pct": 80.0, "mean_silhouette_cosine": 0.2}
    from apps.claims.clustering.score import HierarchyGuards, HierarchyWeights

    score = compute_hierarchy_objective(
        leaf,
        nar,
        eval_score=0.65,
        weights=HierarchyWeights(),
        guards=HierarchyGuards(),
        leaf_singleton_frac=0.05,
        narrative_largest_share=0.3,
    )
    assert "objective" in score
