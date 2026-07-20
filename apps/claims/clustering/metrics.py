"""Tightness metrics for cluster runs (computed in full embedding space)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

_PAIRWISE_CAP = 200


def metrics_path_for(labels_path: str | Path) -> Path:
    """Sidecar path: cluster_3.npy -> cluster_metrics_3.json."""
    p = Path(labels_path)
    stem = p.stem  # cluster_{profile_id}
    if stem.startswith("cluster_"):
        suffix = stem[len("cluster_") :]
        return p.with_name(f"cluster_metrics_{suffix}.json")
    return p.with_name(f"{stem}_metrics.json")


def write_metrics(path: Path, metrics: dict[str, Any]) -> None:
    path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")


def read_metrics(path: str | Path) -> dict[str, Any] | None:
    p = Path(path)
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _per_cluster_mean_intra_cosine(vectors: np.ndarray, idx: np.ndarray, rng: np.random.Generator) -> float:
    m = int(idx.size)
    if m <= 1:
        return 1.0
    sub = vectors[idx]
    if m > _PAIRWISE_CAP:
        pick = rng.choice(m, size=_PAIRWISE_CAP, replace=False)
        sub = sub[pick]
    # Normalized vectors: dot product is cosine similarity.
    sims = sub @ sub.T
    tri = sims[np.triu_indices(len(sub), k=1)]
    return float(np.mean(tri)) if tri.size else 1.0


def _cluster_medoid(vectors: np.ndarray, idx: np.ndarray) -> int:
    sub = vectors[idx]
    if len(idx) == 1:
        return int(idx[0])
    sims = sub @ sub.T
    mean_sim = sims.mean(axis=1)
    return int(idx[int(np.argmax(mean_sim))])


def compute_cluster_metrics(
    vectors: np.ndarray,
    labels: np.ndarray,
    *,
    sample_size: int = 5000,
    seed: int = 0,
) -> dict[str, Any]:
    """Compute global and per-cluster tightness metrics in embedding space."""
    vectors = np.asarray(vectors, dtype=np.float64)
    labels = np.asarray(labels, dtype=int)
    n = int(labels.size)
    if n == 0:
        return {
            "n_points": 0,
            "n_clusters": 0,
            "n_noise": 0,
            "coverage_pct": 0.0,
            "per_cluster": [],
        }

    rng = np.random.default_rng(seed)
    n_noise = int(np.sum(labels == -1))
    cluster_ids = sorted({int(x) for x in labels.tolist() if int(x) != -1})
    n_clusters = len(cluster_ids)
    coverage_pct = round(100.0 * (n - n_noise) / n, 2) if n else 0.0

    per_cluster: list[dict[str, Any]] = []
    weighted_intra = 0.0
    total_assigned = 0

    for cid in cluster_ids:
        idx = np.where(labels == cid)[0]
        size = int(idx.size)
        intra = _per_cluster_mean_intra_cosine(vectors, idx, rng)
        medoid = _cluster_medoid(vectors, idx)
        per_cluster.append(
            {
                "cluster_id": cid,
                "size": size,
                "mean_intra_cosine": round(intra, 4),
                "representative_idx": medoid,
            }
        )
        weighted_intra += intra * size
        total_assigned += size

    per_cluster.sort(key=lambda row: (-row["size"], row["cluster_id"]))

    sizes = [row["size"] for row in per_cluster]
    size_median = float(np.median(sizes)) if sizes else 0.0
    size_p90 = float(np.percentile(sizes, 90)) if sizes else 0.0
    size_max = max(sizes) if sizes else 0

    mean_intra = round(weighted_intra / total_assigned, 4) if total_assigned else None

    silhouette = None
    assigned_mask = labels != -1
    if n_clusters >= 2 and int(np.sum(assigned_mask)) >= 3:
        from sklearn.metrics import silhouette_score

        sub_n = int(np.sum(assigned_mask))
        if sub_n > sample_size:
            pick = rng.choice(np.where(assigned_mask)[0], size=sample_size, replace=False)
        else:
            pick = np.where(assigned_mask)[0]
        try:
            silhouette = round(
                float(
                    silhouette_score(
                        vectors[pick],
                        labels[pick],
                        metric="cosine",
                        sample_size=min(len(pick), sample_size),
                    )
                ),
                4,
            )
        except Exception:
            silhouette = None

    return {
        "n_points": n,
        "n_clusters": n_clusters,
        "n_noise": n_noise,
        "coverage_pct": coverage_pct,
        "size_median": round(size_median, 1),
        "size_p90": round(size_p90, 1),
        "size_max": size_max,
        "mean_intra_cosine": mean_intra,
        "mean_silhouette_cosine": silhouette,
        "per_cluster": per_cluster[:20],
    }
