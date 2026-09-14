"""Tests for apps.claims.clustering.score."""

from __future__ import annotations

from apps.claims.clustering.score import (
    ObjectiveGuards,
    ObjectiveWeights,
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

