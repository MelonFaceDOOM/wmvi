"""Bottom-up two-level clustering: tight leaf paraphrases -> narrative groups.

Leaf pass clusters all claim vectors. Narrative pass clusters leaf medoids only,
then maps each leaf (and its members) onto a narrative id.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from apps.claims.clustering import cluster as clustering
from apps.claims.clustering.metrics import (
    _cluster_medoid,
    _per_cluster_mean_intra_cosine,
)

_ORPHAN_PARAM = "min_leaf_narrative_cosine"


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    a = np.asarray(a, dtype=np.float64).reshape(-1)
    b = np.asarray(b, dtype=np.float64).reshape(-1)
    na = float(np.linalg.norm(a))
    nb = float(np.linalg.norm(b))
    if na <= 1e-12 or nb <= 1e-12:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


DEFAULT_LEAF_ALGORITHM = "hdbscan"
DEFAULT_LEAF_PARAMS: dict[str, Any] = {
    "reduce": "pca",
    "n_components": 50,
    "min_cluster_size": 8,
    "min_samples": 5,
}

DEFAULT_NARRATIVE_ALGORITHM = "agglomerative"
DEFAULT_NARRATIVE_PARAMS: dict[str, Any] = {
    "reduce": "none",
    "n_clusters": 12,
}


@dataclass
class HierarchyResult:
    leaf_labels: np.ndarray  # (n_points,), -1 = noise
    narrative_labels: np.ndarray  # (n_points,); -1 if leaf noise or orphaned
    leaf_ids: list[int]  # ordered leaf cluster ids (excludes noise)
    leaf_medoid_idxs: list[int]  # parallel to leaf_ids
    leaf_to_narrative: dict[int, int]  # leaf_id -> narrative_id (-1 if narrative noise/orphan)
    narrative_to_leaves: dict[int, list[int]]  # narrative_id -> leaf_ids
    leaf_result: clustering.ClusterResult
    narrative_result: clustering.ClusterResult | None
    n_leaves: int
    n_narratives: int
    n_leaf_noise: int


def build_hierarchy(
    vectors: np.ndarray,
    *,
    leaf_algorithm: str = DEFAULT_LEAF_ALGORITHM,
    leaf_params: dict[str, Any] | None = None,
    narrative_algorithm: str = DEFAULT_NARRATIVE_ALGORITHM,
    narrative_params: dict[str, Any] | None = None,
    seed: int = 0,
) -> HierarchyResult:
    """Cluster points into leaves, then cluster leaf medoids into narratives."""
    vectors = np.asarray(vectors, dtype=np.float32)
    n = int(vectors.shape[0])
    leaf_params = dict(leaf_params or DEFAULT_LEAF_PARAMS)
    narrative_params = dict(narrative_params or DEFAULT_NARRATIVE_PARAMS)
    cluster_narrative_params = dict(narrative_params)
    orphan_cutoff = cluster_narrative_params.pop(_ORPHAN_PARAM, None)

    leaf_result = clustering.run_clustering(
        vectors,
        algorithm=leaf_algorithm,
        params=leaf_params,
        seed=seed,
    )
    leaf_labels = np.asarray(leaf_result.labels, dtype=int)

    leaf_ids = sorted({int(x) for x in leaf_labels.tolist() if int(x) != -1})
    leaf_medoid_idxs: list[int] = []
    for lid in leaf_ids:
        idx = np.where(leaf_labels == lid)[0]
        leaf_medoid_idxs.append(int(_cluster_medoid(vectors, idx)))

    narrative_labels = np.full(n, -1, dtype=int)
    leaf_to_narrative: dict[int, int] = {}
    narrative_to_leaves: dict[int, list[int]] = {}
    narrative_result: clustering.ClusterResult | None = None

    if not leaf_ids:
        return HierarchyResult(
            leaf_labels=leaf_labels,
            narrative_labels=narrative_labels,
            leaf_ids=[],
            leaf_medoid_idxs=[],
            leaf_to_narrative={},
            narrative_to_leaves={},
            leaf_result=leaf_result,
            narrative_result=None,
            n_leaves=0,
            n_narratives=0,
            n_leaf_noise=int(leaf_result.n_noise),
        )

    medoid_matrix = vectors[np.asarray(leaf_medoid_idxs, dtype=int)]
    narrative_result = clustering.run_clustering(
        medoid_matrix,
        algorithm=narrative_algorithm,
        params=cluster_narrative_params,
        seed=seed,
    )
    medoid_narrative = np.asarray(narrative_result.labels, dtype=int)

    for i, lid in enumerate(leaf_ids):
        nid = int(medoid_narrative[i])
        leaf_to_narrative[lid] = nid
        if nid != -1:
            narrative_to_leaves.setdefault(nid, []).append(lid)
            member_idx = np.where(leaf_labels == lid)[0]
            narrative_labels[member_idx] = nid

    if orphan_cutoff is not None:
        cutoff = float(orphan_cutoff)
        lid_to_medoid_idx = {lid: leaf_medoid_idxs[i] for i, lid in enumerate(leaf_ids)}
        for nid in list(narrative_to_leaves.keys()):
            member_idx = np.where(narrative_labels == nid)[0]
            if member_idx.size == 0:
                narrative_to_leaves.pop(nid, None)
                continue
            nar_medoid_idx = int(_cluster_medoid(vectors, member_idx))
            nar_vec = vectors[nar_medoid_idx]
            kept: list[int] = []
            for lid in narrative_to_leaves[nid]:
                leaf_vec = vectors[int(lid_to_medoid_idx[lid])]
                if _cosine_similarity(leaf_vec, nar_vec) < cutoff:
                    leaf_to_narrative[lid] = -1
                    narrative_labels[np.where(leaf_labels == lid)[0]] = -1
                else:
                    kept.append(lid)
            if kept:
                narrative_to_leaves[nid] = kept
            else:
                narrative_to_leaves.pop(nid, None)

    return HierarchyResult(
        leaf_labels=leaf_labels,
        narrative_labels=narrative_labels,
        leaf_ids=leaf_ids,
        leaf_medoid_idxs=leaf_medoid_idxs,
        leaf_to_narrative=leaf_to_narrative,
        narrative_to_leaves=narrative_to_leaves,
        leaf_result=leaf_result,
        narrative_result=narrative_result,
        n_leaves=len(leaf_ids),
        n_narratives=len(narrative_to_leaves),
        n_leaf_noise=int(leaf_result.n_noise),
    )


def parent_id_for_cluster(
    *,
    child_labels: np.ndarray,
    parent_labels: np.ndarray,
    cluster_id: int,
    vectors: np.ndarray | None = None,
) -> int | None:
    """Map a child cluster id to its parent via the child medoid (or first member).

    Returns None for empty clusters; returns -1 when the parent label is noise.
    """
    child_labels = np.asarray(child_labels, dtype=int)
    parent_labels = np.asarray(parent_labels, dtype=int)
    if child_labels.shape != parent_labels.shape:
        raise ValueError(
            f"child/parent label length mismatch: {child_labels.shape[0]} vs {parent_labels.shape[0]}"
        )
    idx = np.where(child_labels == int(cluster_id))[0]
    if idx.size == 0:
        return None
    if cluster_id == -1:
        # Noise has no single parent; report majority parent among noise points if any.
        parents = parent_labels[idx]
        assigned = parents[parents != -1]
        if assigned.size == 0:
            return -1
        vals, counts = np.unique(assigned, return_counts=True)
        return int(vals[int(np.argmax(counts))])
    if vectors is not None:
        medoid = int(_cluster_medoid(np.asarray(vectors), idx))
        return int(parent_labels[medoid])
    return int(parent_labels[int(idx[0])])


def nested_hierarchy_payload(
    *,
    hierarchy: HierarchyResult,
    vectors: np.ndarray,
    claim_texts: list[str],
    n_samples_per_leaf: int = 3,
    seed: int = 0,
) -> list[dict[str, Any]]:
    """Build narratives[] -> leaves[] with medoid + sample claim texts for CLI output."""
    vectors = np.asarray(vectors)
    rng = np.random.default_rng(seed)
    narratives: list[dict[str, Any]] = []

    for nid in sorted(hierarchy.narrative_to_leaves.keys()):
        leaf_ids = hierarchy.narrative_to_leaves[nid]
        leaves_out: list[dict[str, Any]] = []
        narrative_size = 0
        for lid in leaf_ids:
            idx = np.where(hierarchy.leaf_labels == lid)[0]
            size = int(idx.size)
            narrative_size += size
            medoid_i = hierarchy.leaf_ids.index(lid)
            medoid_idx = hierarchy.leaf_medoid_idxs[medoid_i]
            intra = _per_cluster_mean_intra_cosine(vectors, idx, rng)
            remaining = [int(i) for i in idx.tolist() if int(i) != int(medoid_idx)]
            k = min(max(0, int(n_samples_per_leaf) - 1), len(remaining))
            pick = rng.choice(np.asarray(remaining, dtype=int), size=k, replace=False) if k else []
            sample_idxs = [int(medoid_idx)] + [int(i) for i in np.asarray(pick).tolist()]
            leaves_out.append(
                {
                    "leaf_id": int(lid),
                    "size": size,
                    "mean_intra_cosine": round(float(intra), 4),
                    "medoid_idx": int(medoid_idx),
                    "medoid_claim_text": (
                        claim_texts[medoid_idx] if 0 <= medoid_idx < len(claim_texts) else ""
                    ),
                    "sample_claim_texts": [
                        claim_texts[i] if 0 <= i < len(claim_texts) else "" for i in sample_idxs
                    ],
                }
            )
        narratives.append(
            {
                "narrative_id": int(nid),
                "n_leaves": len(leaf_ids),
                "size": narrative_size,
                "leaves": leaves_out,
            }
        )

    narratives.sort(key=lambda row: (-int(row["size"]), int(row["narrative_id"])))
    return narratives
