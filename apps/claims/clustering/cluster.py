"""Clustering of embedding vectors (sklearn + graph communities).

Algorithms:
- ``kmeans``: MiniBatchKMeans / KMeans. Param: ``n_clusters``.
- ``dbscan``: density-based on cosine distance. Params: ``eps``, ``min_samples``.
- ``agglomerative``: Ward linkage; ``n_clusters`` or ``distance_threshold`` (not both).
  Guarded to small sets (O(n^2) memory).
- ``hdbscan``: variable-density clusters via sklearn HDBSCAN (optional PCA/UMAP prep).
- ``knn_louvain``: kNN graph + Louvain communities (optional PCA/UMAP prep).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from apps.claims.clustering import prep as cluster_prep

CLUSTER_ALGORITHMS: tuple[str, ...] = (
    "kmeans",
    "dbscan",
    "agglomerative",
    "hdbscan",
    "knn_louvain",
)

AGGLOMERATIVE_MAX_POINTS = 12000
_MINIBATCH_THRESHOLD = 20000

_SHARED_DEFAULTS: dict[str, Any] = {
    "reduce": "none",
    "n_components": 50,
}

DEFAULT_PARAMS: dict[str, dict[str, Any]] = {
    "kmeans": {**_SHARED_DEFAULTS, "n_clusters": 12},
    "dbscan": {**_SHARED_DEFAULTS, "eps": 0.35, "min_samples": 5},
    "agglomerative": {**_SHARED_DEFAULTS, "n_clusters": 12},
    "hdbscan": {**_SHARED_DEFAULTS, "reduce": "pca", "min_cluster_size": 15, "min_samples": 5},
    "knn_louvain": {**_SHARED_DEFAULTS, "reduce": "pca", "k_neighbors": 15},
}

CLUSTER_PRESETS: dict[str, dict[str, Any]] = {
    "kmeans-12": {"name": "kmeans-12", "algorithm": "kmeans", "params": {"n_clusters": 12, "reduce": "none"}},
    "kmeans-500": {"name": "kmeans-500", "algorithm": "kmeans", "params": {"n_clusters": 500, "reduce": "none"}},
    "hdbscan-pca50": {
        "name": "hdbscan-pca50",
        "algorithm": "hdbscan",
        "params": {"reduce": "pca", "n_components": 50, "min_cluster_size": 15, "min_samples": 5},
    },
    "hdbscan-pca50-small": {
        "name": "hdbscan-pca50-small",
        "algorithm": "hdbscan",
        "params": {"reduce": "pca", "n_components": 50, "min_cluster_size": 8, "min_samples": 5},
    },
    "louvain-knn15": {
        "name": "louvain-knn15",
        "algorithm": "knn_louvain",
        "params": {"reduce": "pca", "n_components": 50, "k_neighbors": 15},
    },
}


@dataclass
class ClusterResult:
    labels: np.ndarray  # shape (n,), int; -1 means noise
    n_clusters: int
    n_noise: int
    prep_meta: dict[str, Any] | None = None


def default_params(algorithm: str) -> dict[str, Any]:
    return dict(DEFAULT_PARAMS.get(algorithm, {}))


def _summarize(labels: np.ndarray, *, prep_meta: dict[str, Any] | None = None) -> ClusterResult:
    labels = np.asarray(labels, dtype=int)
    unique = set(int(x) for x in labels.tolist())
    n_noise = int(np.sum(labels == -1))
    n_clusters = len([u for u in unique if u != -1])
    return ClusterResult(labels=labels, n_clusters=n_clusters, n_noise=n_noise, prep_meta=prep_meta)


def _prepare(vectors: np.ndarray, params: dict[str, Any], seed: int) -> tuple[np.ndarray, dict[str, Any] | None]:
    reduce = str(params.get("reduce", "none"))
    if reduce == "none":
        return vectors, None
    prepared, meta = cluster_prep.prepare_vectors(
        vectors,
        reduce=reduce,
        n_components=int(params.get("n_components", 50)),
        seed=seed,
    )
    return prepared, meta


def _run_hdbscan(vectors: np.ndarray, params: dict[str, Any]) -> np.ndarray:
    from sklearn.cluster import HDBSCAN

    min_cluster_size = max(2, int(params.get("min_cluster_size", 15)))
    min_samples = max(1, int(params.get("min_samples", 5)))
    model = HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples, metric="euclidean")
    return model.fit_predict(vectors)


def _run_knn_louvain(vectors: np.ndarray, params: dict[str, Any], seed: int) -> np.ndarray:
    import networkx as nx
    from sklearn.neighbors import NearestNeighbors

    n = int(vectors.shape[0])
    if n == 0:
        return np.zeros((0,), dtype=int)
    if n == 1:
        return np.zeros((1,), dtype=int)

    k = max(2, min(int(params.get("k_neighbors", 15)), n - 1))
    nn = NearestNeighbors(n_neighbors=k + 1, metric="cosine", algorithm="brute")
    nn.fit(vectors)
    _, indices = nn.kneighbors(vectors)

    graph = nx.Graph()
    graph.add_nodes_from(range(n))
    for i in range(n):
        for j in indices[i]:
            j = int(j)
            if j != i:
                graph.add_edge(i, j)

    communities = nx.community.louvain_communities(graph, seed=seed)
    labels = np.full(n, -1, dtype=int)
    for cid, comm in enumerate(communities):
        for node in comm:
            labels[int(node)] = cid
    return labels


def run_clustering(vectors: np.ndarray, *, algorithm: str, params: dict[str, Any], seed: int = 0) -> ClusterResult:
    n = int(vectors.shape[0])
    if n == 0:
        return ClusterResult(labels=np.zeros((0,), dtype=int), n_clusters=0, n_noise=0)

    work, prep_meta = _prepare(vectors, params, seed)

    if algorithm == "kmeans":
        k = max(1, int(params.get("n_clusters", 12)))
        k = min(k, n)
        if n >= _MINIBATCH_THRESHOLD:
            from sklearn.cluster import MiniBatchKMeans

            model = MiniBatchKMeans(n_clusters=k, random_state=seed, n_init="auto", batch_size=4096)
        else:
            from sklearn.cluster import KMeans

            model = KMeans(n_clusters=k, random_state=seed, n_init="auto")
        labels = model.fit_predict(work)
        return _summarize(labels, prep_meta=prep_meta)

    if algorithm == "dbscan":
        from sklearn.cluster import DBSCAN

        eps = float(params.get("eps", 0.35))
        min_samples = int(params.get("min_samples", 5))
        if prep_meta is not None:
            labels = DBSCAN(eps=eps, min_samples=min_samples, metric="euclidean").fit_predict(work)
        else:
            labels = DBSCAN(eps=eps, min_samples=min_samples, metric="cosine").fit_predict(work)
        return _summarize(labels, prep_meta=prep_meta)

    if algorithm == "agglomerative":
        if n > AGGLOMERATIVE_MAX_POINTS:
            raise ValueError(
                f"Agglomerative clustering needs O(n^2) memory; {n} claims exceeds the "
                f"{AGGLOMERATIVE_MAX_POINTS} cap. Use kmeans/hdbscan, or a smaller source set."
            )
        from sklearn.cluster import AgglomerativeClustering

        thresh = params.get("distance_threshold")
        if thresh is not None:
            labels = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=float(thresh),
                linkage="ward",
            ).fit_predict(work)
        else:
            k = max(1, min(int(params.get("n_clusters", 12)), n))
            labels = AgglomerativeClustering(n_clusters=k).fit_predict(work)
        return _summarize(labels, prep_meta=prep_meta)

    if algorithm == "hdbscan":
        labels = _run_hdbscan(work, params)
        return _summarize(labels, prep_meta=prep_meta)

    if algorithm == "knn_louvain":
        labels = _run_knn_louvain(work, params, seed)
        return _summarize(labels, prep_meta=prep_meta)

    raise ValueError(f"Unknown clustering algorithm: {algorithm!r}")


def cluster_sizes(labels: np.ndarray) -> dict[int, int]:
    """Count members per cluster label (including noise at -1)."""
    labels = np.asarray(labels, dtype=int)
    sizes: dict[int, int] = {}
    for label in labels.tolist():
        cid = int(label)
        sizes[cid] = sizes.get(cid, 0) + 1
    return sizes


def _pick_distinctive_terms(
    scores: np.ndarray,
    feature_names: np.ndarray,
    *,
    top_k: int,
) -> list[str]:
    """Rank terms by c-TF-IDF score; prefer multi-word bigrams for readable names."""
    ranked = sorted(
        ((float(scores[i]), str(feature_names[i])) for i in range(len(scores)) if scores[i] > 0),
        key=lambda x: -x[0],
    )
    bigrams = [term for _, term in ranked if " " in term]
    unigrams = [term for _, term in ranked if " " not in term]
    chosen = bigrams[:top_k]
    if len(chosen) < top_k:
        chosen.extend(unigrams[: top_k - len(chosen)])
    return chosen[:top_k]


def name_clusters_tfidf(
    claim_texts: list[str],
    labels: np.ndarray,
    *,
    top_k: int = 4,
) -> dict[int, str]:
    """Auto-name clusters from terms distinctive to each cluster vs the others.

    Uses unigrams + bigrams (c-TF-IDF); multi-word phrases are preferred in the
    final label when their scores are competitive.
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
    vectorizer = CountVectorizer(
        stop_words="english",
        max_features=10000,
        min_df=1,
        ngram_range=(1, 2),
    )
    counts = vectorizer.fit_transform([docs[cid] for cid in ids]).toarray().astype(np.float64)
    feature_names = vectorizer.get_feature_names_out()
    n_clusters = counts.shape[0]

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
        terms = _pick_distinctive_terms(scores, feature_names, top_k=top_k)
        names[cid] = ", ".join(terms) if terms else f"Cluster {cid}"
    return names
