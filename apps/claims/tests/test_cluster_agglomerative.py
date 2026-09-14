"""Tests for agglomerative clustering (n_clusters and distance_threshold)."""

from __future__ import annotations

import numpy as np

from apps.claims.clustering import cluster as clustering


def test_agglomerative_distance_threshold_on_blobs():
    rng = np.random.default_rng(0)
    blobs = []
    for offset in ((0.0, 0.0, 0.0, 0.0), (6.0, 0.0, 0.0, 0.0), (0.0, 6.0, 0.0, 0.0)):
        noise = rng.normal(0, 0.02, size=(8, 4)).astype(np.float32)
        blobs.append(noise + np.asarray(offset, dtype=np.float32))
    vectors = np.vstack(blobs)

    one = clustering.run_clustering(
        vectors,
        algorithm="agglomerative",
        params={"reduce": "none", "distance_threshold": 100.0},
        seed=0,
    )
    assert one.n_clusters == 1

    three = clustering.run_clustering(
        vectors,
        algorithm="agglomerative",
        params={"reduce": "none", "distance_threshold": 1.0},
        seed=0,
    )
    assert three.n_clusters == 3

    many = clustering.run_clustering(
        vectors,
        algorithm="agglomerative",
        params={"reduce": "none", "n_clusters": 3},
        seed=0,
    )
    assert many.n_clusters == 3
