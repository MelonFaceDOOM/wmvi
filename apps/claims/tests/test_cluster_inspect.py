"""Inspect helpers for apps.claims clustering CLI."""

from __future__ import annotations

import numpy as np

from apps.claims.cli.cluster_cmd import select_cluster_ids, sample_cluster_members
from apps.claims.clustering import cluster as clustering


def test_select_and_sample():
    rng = np.random.default_rng(0)
    vectors = rng.normal(size=(40, 8)).astype(np.float32)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True) + 1e-9
    result = clustering.run_clustering(
        vectors, algorithm="kmeans", params={"n_clusters": 4, "reduce": "none"}, seed=0
    )
    from apps.claims.cli.cluster_cmd import _cluster_stats

    stats = _cluster_stats(vectors, result.labels, seed=0)
    ids = select_cluster_ids(stats, mode="largest", n_clusters=2, min_size=1)
    assert len(ids) == 2
    texts = [f"c{i}" for i in range(40)]
    sample = sample_cluster_members(
        vectors=vectors,
        labels=result.labels,
        claim_texts=texts,
        cluster_id=ids[0],
        n_per_cluster=3,
        seed=0,
    )
    assert sample["size"] > 0
    assert sample["samples"]
