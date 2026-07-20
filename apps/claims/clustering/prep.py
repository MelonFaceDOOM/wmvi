"""Optional dimensionality reduction before clustering (PCA / UMAP)."""

from __future__ import annotations

from typing import Any

import numpy as np

REDUCE_METHODS: tuple[str, ...] = ("none", "pca", "umap")
UMAP_FIT_SAMPLE_SIZE = 20000


def _effective_n_components(n: int, dim: int, requested: int) -> int:
    return max(1, min(int(requested), n - 1, dim))


def prepare_vectors(
    vectors: np.ndarray,
    *,
    reduce: str = "none",
    n_components: int = 50,
    seed: int = 0,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Return vectors ready for clustering plus metadata for metrics sidecar."""
    vectors = np.asarray(vectors, dtype=np.float64)
    n, dim = vectors.shape
    meta: dict[str, Any] = {
        "reduce": reduce,
        "n_components": int(n_components),
        "input_dim": int(dim),
        "subsampled_fit": False,
    }
    if reduce == "none" or n == 0:
        meta["output_dim"] = int(dim)
        return vectors, meta

    comps = _effective_n_components(n, dim, n_components)
    meta["n_components"] = comps

    if reduce == "pca":
        from sklearn.decomposition import PCA

        out = PCA(n_components=comps, random_state=seed).fit_transform(vectors)
        meta["output_dim"] = int(out.shape[1])
        return out.astype(np.float64, copy=False), meta

    if reduce == "umap":
        import umap

        subsampled = n > UMAP_FIT_SAMPLE_SIZE
        meta["subsampled_fit"] = subsampled
        if subsampled:
            rng = np.random.default_rng(seed)
            fit_idx = np.sort(rng.choice(n, size=UMAP_FIT_SAMPLE_SIZE, replace=False))
            fit_vectors = vectors[fit_idx]
        else:
            fit_vectors = vectors

        reducer = umap.UMAP(
            n_components=comps,
            metric="cosine",
            random_state=seed,
            n_jobs=1,
        )
        reducer.fit(fit_vectors)
        out = reducer.transform(vectors)
        meta["output_dim"] = int(out.shape[1])
        return np.asarray(out, dtype=np.float64), meta

    raise ValueError(f"Unknown reduce method: {reduce!r}")
