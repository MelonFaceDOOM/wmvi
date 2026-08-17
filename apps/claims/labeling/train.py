"""Train Ridge on frozen encoder embeddings from (text, y) pairs."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims.embedding.encode import DEFAULT_ENCODER_MODEL_ID, encode_texts, load_sentence_transformer
from apps.claims.labeling.artifact import ArtifactMeta, save_artifact_dir


def _mae_rmse_r(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[float, float, float | None]:
    err = y_pred - y_true
    mae = float(np.mean(np.abs(err)))
    rmse = float(math.sqrt(float(np.mean(err**2))))
    pearson: float | None = None
    if len(y_true) >= 2 and float(np.std(y_true)) > 1e-12 and float(np.std(y_pred)) > 1e-12:
        pearson = float(np.corrcoef(y_true, y_pred)[0, 1])
    return mae, rmse, pearson


def binary_metrics(y_true: np.ndarray, y_pred: np.ndarray, *, threshold: float = 0.5) -> dict[str, Any]:
    yt = (np.asarray(y_true, dtype=np.float64) >= threshold).astype(int)
    yp = (np.asarray(y_pred, dtype=np.float64) >= threshold).astype(int)
    tp = int(np.sum((yt == 1) & (yp == 1)))
    tn = int(np.sum((yt == 0) & (yp == 0)))
    fp = int(np.sum((yt == 0) & (yp == 1)))
    fn = int(np.sum((yt == 1) & (yp == 0)))
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    return {
        "threshold": threshold,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "precision": round(prec, 4),
        "recall": round(rec, 4),
        "f1": round(f1, 4),
        "accuracy": round((tp + tn) / max(len(yt), 1), 4),
    }


def run_train_from_pairs(
    *,
    texts: list[str],
    ys: list[float],
    out_dir: Path,
    head_name: str,
    input_var_keys: list[str] | None = None,
    val_ratio: float = 0.0,
    seed: int = 0,
    ridge_alpha: float = 1.0,
    batch_size: int = 32,
    encoder_model_id: str | None = None,
    manifest: dict[str, Any] | None = None,
    value_type: str = "float",
) -> dict[str, Any]:
    from sklearn.linear_model import Ridge
    from sklearn.model_selection import train_test_split

    enc_id = encoder_model_id or DEFAULT_ENCODER_MODEL_ID
    if len(texts) != len(ys):
        raise ValueError("texts and ys length mismatch")
    if len(texts) < 3:
        raise ValueError(f"Need at least 3 training rows; got {len(texts)}")
    if out_dir.exists():
        raise FileExistsError(f"Model output already exists (immutable): {out_dir}")

    X_idx = np.arange(len(texts))
    if val_ratio <= 0 or val_ratio >= 1:
        train_idx, val_idx = X_idx, np.array([], dtype=int)
    else:
        train_idx, val_idx = train_test_split(
            X_idx,
            test_size=val_ratio,
            random_state=seed,
            shuffle=True,
        )

    train_texts = [texts[i] for i in train_idx]
    y_train = np.array([ys[i] for i in train_idx], dtype=np.float64)
    val_texts = [texts[i] for i in val_idx] if len(val_idx) else []
    y_val = np.array([ys[i] for i in val_idx], dtype=np.float64) if len(val_idx) else np.array([])

    st = load_sentence_transformer(enc_id)
    norm_emb = True
    X_train = encode_texts(st, train_texts, batch_size=batch_size, normalize_embeddings=norm_emb)
    X_val = (
        encode_texts(st, val_texts, batch_size=batch_size, normalize_embeddings=norm_emb)
        if val_texts
        else np.zeros((0, X_train.shape[1]), dtype=np.float64)
    )

    ridge = Ridge(alpha=ridge_alpha, random_state=seed)
    ridge.fit(X_train, y_train)

    y_hat_train = np.clip(ridge.predict(X_train), 0.0, 1.0)
    mae_tr, rmse_tr, r_tr = _mae_rmse_r(y_train, y_hat_train)

    metrics: dict[str, Any] = {
        "head_name": head_name,
        "n_train": int(len(train_idx)),
        "n_val": int(len(val_idx)),
        "train_mae": mae_tr,
        "train_rmse": rmse_tr,
        "train_pearson": r_tr,
        "value_type": value_type,
    }
    if value_type == "binary":
        metrics["train_binary"] = binary_metrics(y_train, y_hat_train)
    if len(val_idx):
        y_hat_val = np.clip(ridge.predict(X_val), 0.0, 1.0)
        mae_v, rmse_v, r_v = _mae_rmse_r(y_val, y_hat_val)
        metrics["val_mae"] = mae_v
        metrics["val_rmse"] = rmse_v
        metrics["val_pearson"] = r_v
        if value_type == "binary":
            metrics["val_binary"] = binary_metrics(y_val, y_hat_val)

    meta = ArtifactMeta(
        encoder_model_id=enc_id,
        head_kind="ridge",
        normalize_embeddings=norm_emb,
        head_name=head_name,
        input_var_keys=list(input_var_keys or ["CLAIM"]),
    )
    train_config: dict[str, Any] = {
        "head_name": head_name,
        "input_var_keys": list(input_var_keys or ["CLAIM"]),
        "val_ratio": val_ratio,
        "seed": seed,
        "ridge_alpha": ridge_alpha,
        "batch_size": batch_size,
        "encoder_model_id": enc_id,
        "n_examples": len(texts),
        "value_type": value_type,
    }
    save_artifact_dir(
        out_dir,
        meta=meta,
        train_config=train_config,
        metrics=metrics,
        ridge_head=ridge,
        manifest=manifest,
    )
    return metrics
