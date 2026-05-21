"""Frozen BGE encoder (sentence-transformers)."""

from __future__ import annotations

from typing import Any

import numpy as np

from apps.claim_extractor.learned.constants import DEFAULT_ENCODER_MODEL_ID


def load_sentence_transformer(model_id: str | None = None) -> Any:
    from sentence_transformers import SentenceTransformer

    mid = model_id or DEFAULT_ENCODER_MODEL_ID
    return SentenceTransformer(mid)


def encode_texts(
    model: Any,
    texts: list[str],
    *,
    batch_size: int = 32,
    normalize_embeddings: bool = True,
) -> np.ndarray:
    arr = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=normalize_embeddings,
        show_progress_bar=len(texts) > 256,
        convert_to_numpy=True,
    )
    return np.asarray(arr, dtype=np.float64)
