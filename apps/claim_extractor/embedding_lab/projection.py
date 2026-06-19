"""2D projection of embedding vectors for the Explore graph.

Clustering happens on the full-dimensional vectors elsewhere; this module only
produces display coordinates. PCA is the dependency-free default; t-SNE is
offered but subsampled because it does not scale to ~100k points.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

PROJECTION_METHODS: tuple[str, ...] = ("pca", "tsne")
DEFAULT_TSNE_MAX_POINTS = 5000


@dataclass
class Projection:
    indices: np.ndarray  # row indices (into the full vector array) that were projected
    coords: np.ndarray  # shape (len(indices), 2)
    method: str
    subsampled: bool


def project_2d(
    vectors: np.ndarray,
    *,
    method: str = "pca",
    tsne_max_points: int = DEFAULT_TSNE_MAX_POINTS,
    seed: int = 0,
) -> Projection:
    n = int(vectors.shape[0])
    if n == 0:
        return Projection(indices=np.arange(0), coords=np.zeros((0, 2)), method=method, subsampled=False)

    if method == "pca":
        from sklearn.decomposition import PCA

        comps = 2 if vectors.shape[1] >= 2 else 1
        coords = PCA(n_components=comps, random_state=seed).fit_transform(vectors)
        if coords.shape[1] == 1:
            coords = np.hstack([coords, np.zeros((coords.shape[0], 1))])
        return Projection(indices=np.arange(n), coords=coords, method=method, subsampled=False)

    if method == "tsne":
        from sklearn.manifold import TSNE

        subsampled = n > tsne_max_points
        if subsampled:
            rng = np.random.default_rng(seed)
            indices = np.sort(rng.choice(n, size=tsne_max_points, replace=False))
        else:
            indices = np.arange(n)
        sub = vectors[indices]
        perplexity = float(min(30.0, max(5.0, (len(indices) - 1) / 3.0)))
        coords = TSNE(n_components=2, random_state=seed, perplexity=perplexity, init="pca").fit_transform(sub)
        return Projection(indices=indices, coords=coords, method=method, subsampled=subsampled)

    raise ValueError(f"Unknown projection method: {method!r}")
