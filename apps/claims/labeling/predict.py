"""Load Ridge artifact and run inference."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from apps.claims.embedding.encode import encode_texts, load_sentence_transformer
from apps.claims.labeling.artifact import METRICS_FILE, RIDGE_HEAD_FILE, load_meta

_encoder_cache: dict[str, Any] = {}


def _get_encoder(model_id: str) -> Any:
    if model_id not in _encoder_cache:
        _encoder_cache[model_id] = load_sentence_transformer(model_id)
    return _encoder_cache[model_id]


def clear_encoder_cache() -> None:
    _encoder_cache.clear()


def clamp_score_01(x: float) -> float:
    return float(max(0.0, min(1.0, x)))


class FieldPredictor:
    """One field: frozen BGE embeddings + fitted Ridge head."""

    def __init__(
        self,
        artifact_dir: Path,
        *,
        ridge: Any,
        meta: Any,
        batch_size: int = 32,
    ) -> None:
        self.artifact_dir = artifact_dir.resolve()
        self._ridge = ridge
        self._meta = meta
        self.batch_size = batch_size

    @classmethod
    def load(cls, artifact_dir: Path, *, batch_size: int = 32) -> FieldPredictor:
        import joblib

        artifact_dir = artifact_dir.resolve()
        meta = load_meta(artifact_dir)
        ridge = joblib.load(artifact_dir / RIDGE_HEAD_FILE)
        return cls(artifact_dir, ridge=ridge, meta=meta, batch_size=batch_size)

    def predict_scores(self, texts: list[str]) -> list[float]:
        if not texts:
            return []
        enc = _get_encoder(self._meta.encoder_model_id)
        X = encode_texts(
            enc,
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=self._meta.normalize_embeddings,
        )
        raw = self._ridge.predict(X)
        return [clamp_score_01(float(v)) for v in raw]


def load_train_metrics(artifact_dir: Path) -> dict[str, Any]:
    p = artifact_dir / METRICS_FILE
    if not p.is_file():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))
