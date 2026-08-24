"""Tests for apps.claims.clustering.hierarchy (parity with embedding_lab)."""

from __future__ import annotations

import numpy as np

from apps.claims.clustering.hierarchy import (
    build_hierarchy,
    nested_hierarchy_payload,
    parent_id_for_cluster,
)


def _make_blob_vectors():
    rng = np.random.default_rng(0)
    centers = np.array(
        [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
        ],
        dtype=np.float32,
    )
    rows = []
    for c in centers:
        noise = rng.normal(0, 0.02, size=(6, 4)).astype(np.float32)
        rows.append(c + noise)
    vectors = np.vstack(rows)
    vectors = vectors / (np.linalg.norm(vectors, axis=1, keepdims=True) + 1e-9)
    return vectors


def test_build_hierarchy_maps_points_to_narratives():
    vectors = _make_blob_vectors()
    h = build_hierarchy(
        vectors,
        leaf_algorithm="kmeans",
        leaf_params={"n_clusters": 6, "reduce": "none"},
        narrative_algorithm="kmeans",
        narrative_params={"n_clusters": 3, "reduce": "none"},
        seed=0,
    )
    assert h.n_leaves == 6
    assert h.n_narratives == 3
    assert h.leaf_labels.shape[0] == vectors.shape[0]
    assert h.narrative_labels.shape[0] == vectors.shape[0]
    assert int((h.narrative_labels >= 0).sum()) == vectors.shape[0]


def test_nested_hierarchy_payload_structure():
    vectors = _make_blob_vectors()
    texts = [f"claim {i}" for i in range(vectors.shape[0])]
    h = build_hierarchy(
        vectors,
        leaf_algorithm="kmeans",
        leaf_params={"n_clusters": 3, "reduce": "none"},
        narrative_algorithm="kmeans",
        narrative_params={"n_clusters": 2, "reduce": "none"},
        seed=1,
    )
    payload = nested_hierarchy_payload(
        hierarchy=h,
        vectors=vectors,
        claim_texts=texts,
        n_samples_per_leaf=2,
        seed=1,
    )
    assert isinstance(payload, list)
    assert payload
    assert "narrative_id" in payload[0]
    assert "leaves" in payload[0]


def test_parent_id_for_cluster():
    vectors = _make_blob_vectors()
    h = build_hierarchy(
        vectors,
        leaf_algorithm="kmeans",
        leaf_params={"n_clusters": 3, "reduce": "none"},
        narrative_algorithm="kmeans",
        narrative_params={"n_clusters": 2, "reduce": "none"},
        seed=2,
    )
    leaf_id = int(h.leaf_ids[0])
    parent = parent_id_for_cluster(
        child_labels=h.leaf_labels,
        parent_labels=h.narrative_labels,
        cluster_id=leaf_id,
        vectors=vectors,
    )
    assert parent is not None
    assert int(parent) >= 0


def test_agglomerative_distance_threshold_on_blobs():
    from apps.claims.clustering import cluster as clustering

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


def test_orphan_leaves_below_narrative_cosine():
    rng = np.random.default_rng(1)
    a = rng.normal(0, 0.02, size=(8, 4)).astype(np.float32) + np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
    b = rng.normal(0, 0.02, size=(8, 4)).astype(np.float32) + np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32)
    vectors = np.vstack([a, b])
    vectors = vectors / (np.linalg.norm(vectors, axis=1, keepdims=True) + 1e-9)

    forced = build_hierarchy(
        vectors,
        leaf_algorithm="kmeans",
        leaf_params={"n_clusters": 2, "reduce": "none"},
        narrative_algorithm="agglomerative",
        narrative_params={"n_clusters": 1, "reduce": "none"},
        seed=0,
    )
    assert forced.n_narratives == 1
    assert int((forced.narrative_labels == -1).sum()) == 0

    orphaned = build_hierarchy(
        vectors,
        leaf_algorithm="kmeans",
        leaf_params={"n_clusters": 2, "reduce": "none"},
        narrative_algorithm="agglomerative",
        narrative_params={
            "n_clusters": 1,
            "reduce": "none",
            "min_leaf_narrative_cosine": 0.5,
        },
        seed=0,
    )
    assert -1 in orphaned.leaf_to_narrative.values()
    assert int((orphaned.narrative_labels == -1).sum()) >= 8
    assert orphaned.n_narratives == 1
