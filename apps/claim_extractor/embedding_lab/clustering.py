"""Clustering of full-dimensional embedding vectors (sklearn only).

Algorithms:
- ``kmeans``: MiniBatchKMeans for large sets, KMeans otherwise. Param: ``n_clusters``.
- ``dbscan``: density-based. Params: ``eps``, ``min_samples`` (label -1 = noise).
- ``agglomerative``: Ward linkage; guarded to small sets (O(n^2) memory).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

CLUSTER_ALGORITHMS: tuple[str, ...] = ("kmeans", "dbscan", "agglomerative")

AGGLOMERATIVE_MAX_POINTS = 12000
_MINIBATCH_THRESHOLD = 20000

DEFAULT_PARAMS: dict[str, dict[str, Any]] = {
    "kmeans": {"n_clusters": 12},
    "dbscan": {"eps": 0.35, "min_samples": 5},
    "agglomerative": {"n_clusters": 12},
}


@dataclass
class ClusterResult:
    labels: np.ndarray  # shape (n,), int; -1 means noise (dbscan)
    n_clusters: int
    n_noise: int


def default_params(algorithm: str) -> dict[str, Any]:
    return dict(DEFAULT_PARAMS.get(algorithm, {}))


def _summarize(labels: np.ndarray) -> ClusterResult:
    labels = np.asarray(labels, dtype=int)
    unique = set(int(x) for x in labels.tolist())
    n_noise = int(np.sum(labels == -1))
    n_clusters = len([u for u in unique if u != -1])
    return ClusterResult(labels=labels, n_clusters=n_clusters, n_noise=n_noise)


def run_clustering(vectors: np.ndarray, *, algorithm: str, params: dict[str, Any], seed: int = 0) -> ClusterResult:
    n = int(vectors.shape[0])
    if n == 0:
        return ClusterResult(labels=np.zeros((0,), dtype=int), n_clusters=0, n_noise=0)

    if algorithm == "kmeans":
        k = max(1, int(params.get("n_clusters", 12)))
        k = min(k, n)
        if n >= _MINIBATCH_THRESHOLD:
            from sklearn.cluster import MiniBatchKMeans

            model = MiniBatchKMeans(n_clusters=k, random_state=seed, n_init="auto", batch_size=4096)
        else:
            from sklearn.cluster import KMeans

            model = KMeans(n_clusters=k, random_state=seed, n_init="auto")
        labels = model.fit_predict(vectors)
        return _summarize(labels)

    if algorithm == "dbscan":
        from sklearn.cluster import DBSCAN

        eps = float(params.get("eps", 0.35))
        min_samples = int(params.get("min_samples", 5))
        # Vectors are normalized -> cosine distance is meaningful.
        labels = DBSCAN(eps=eps, min_samples=min_samples, metric="cosine").fit_predict(vectors)
        return _summarize(labels)

    if algorithm == "agglomerative":
        if n > AGGLOMERATIVE_MAX_POINTS:
            raise ValueError(
                f"Agglomerative clustering needs O(n^2) memory; {n} claims exceeds the "
                f"{AGGLOMERATIVE_MAX_POINTS} cap. Use kmeans/dbscan, or a smaller source set."
            )
        from sklearn.cluster import AgglomerativeClustering

        k = max(1, min(int(params.get("n_clusters", 12)), n))
        labels = AgglomerativeClustering(n_clusters=k).fit_predict(vectors)
        return _summarize(labels)

    raise ValueError(f"Unknown clustering algorithm: {algorithm!r}")


def cluster_sizes(labels: np.ndarray) -> dict[int, int]:
    """Count members per cluster label (including noise at -1)."""
    labels = np.asarray(labels, dtype=int)
    sizes: dict[int, int] = {}
    for label in labels.tolist():
        cid = int(label)
        sizes[cid] = sizes.get(cid, 0) + 1
    return sizes


def name_clusters_tfidf(
    claim_texts: list[str],
    labels: np.ndarray,
    *,
    top_k: int = 4,
) -> dict[int, str]:
    """Auto-name clusters from terms distinctive to each cluster vs the others.

    Plain per-cluster TF-IDF over concatenated cluster documents tends to pick the
    same domain vocabulary (e.g. measles, vaccine) in every cluster for homogeneous
    corpora. We instead score terms by within-cluster frequency times an IDF factor
    that penalizes terms appearing in many clusters (c-TF-IDF style).
    """
    labels = np.asarray(labels, dtype=int)
    names: dict[int, str] = {}
    if labels.size == 0:
        return names

    cluster_ids = sorted({int(x) for x in labels.tolist()})
    docs: dict[int, str] = {}
    for cid in cluster_ids:
        if cid == -1:
            names[-1] = "noise"
            continue
        idx = np.where(labels == cid)[0]
        texts = [claim_texts[int(i)] for i in idx if int(i) < len(claim_texts)]
        docs[cid] = " ".join(texts)

    if not docs:
        return names

    from sklearn.feature_extraction.text import CountVectorizer

    ids = list(docs.keys())
    vectorizer = CountVectorizer(stop_words="english", max_features=5000, min_df=1)
    counts = vectorizer.fit_transform([docs[cid] for cid in ids]).toarray().astype(np.float64)
    feature_names = vectorizer.get_feature_names_out()
    n_clusters = counts.shape[0]

    # How many clusters contain each term (document frequency across clusters).
    df = (counts > 0).sum(axis=0)
    idf_contrast = np.log((n_clusters + 1.0) / (df + 1.0)) + 1.0

    for j, cid in enumerate(ids):
        row = counts[j]
        total = float(row.sum())
        if total <= 0:
            names[cid] = f"Cluster {cid}"
            continue
        tf = row / total
        scores = tf * idf_contrast
        top_idx = scores.argsort()[-top_k:][::-1]
        terms = [str(feature_names[i]) for i in top_idx if scores[i] > 0]
        names[cid] = ", ".join(terms[:top_k]) if terms else f"Cluster {cid}"
    return names
