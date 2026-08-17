"""Frozen SentenceTransformer encoder shared by labeling and embedding."""

from __future__ import annotations

import logging
import re
import warnings
from typing import Any

import numpy as np

DEFAULT_ENCODER_MODEL_ID = "BAAI/bge-small-en-v1.5"
DEFAULT_MAX_SEQ_LENGTH = 512

_log = logging.getLogger(__name__)

# Heuristic: warn when loading large models on CPU
_LARGE_MODEL_RE = re.compile(r"(?i)(?:^|[-_/])(?:[4-9]|[1-9]\d+)[Bb](?:$|[-_/])")


def _looks_large_model(model_id: str) -> bool:
    return bool(_LARGE_MODEL_RE.search(model_id or ""))


def _torch_dtype(name: str | None) -> Any | None:
    if name is None or name == "" or name == "auto":
        return None
    import torch

    mapping = {
        "bfloat16": torch.bfloat16,
        "bf16": torch.bfloat16,
        "float16": torch.float16,
        "fp16": torch.float16,
        "float32": torch.float32,
        "fp32": torch.float32,
    }
    key = str(name).strip().lower()
    if key not in mapping:
        raise ValueError(f"Unknown dtype {name!r}; expected auto|bfloat16|float16|float32")
    return mapping[key]


def resolve_device(device: str | None = None) -> str:
    """Resolve ``auto|cuda|cpu`` to a concrete device string."""
    raw = (device or "auto").strip().lower()
    if raw in ("", "auto"):
        try:
            import torch

            return "cuda" if torch.cuda.is_available() else "cpu"
        except Exception:  # noqa: BLE001
            return "cpu"
    if raw in ("cuda", "cpu"):
        if raw == "cuda":
            import torch

            if not torch.cuda.is_available():
                raise ValueError("device=cuda requested but torch.cuda.is_available() is False")
        return raw
    raise ValueError(f"Unknown device {device!r}; expected auto|cuda|cpu")


def resolve_load_kwargs(
    model_id: str,
    *,
    device: str | None = None,
    dtype: str | None = "auto",
    max_seq_length: int | None = DEFAULT_MAX_SEQ_LENGTH,
    model_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve SentenceTransformer load options without downloading weights.

    Returns a dict with keys: ``device``, ``dtype_name``, ``torch_dtype``,
    ``max_seq_length``, ``model_kwargs``, ``warn_cpu_large``.
    """
    resolved_device = resolve_device(device)
    dtype_raw = (dtype or "auto").strip().lower()
    dtype_name: str | None
    torch_dt: Any | None
    if dtype_raw in ("", "auto"):
        if resolved_device == "cuda":
            dtype_name = "bfloat16"
            torch_dt = _torch_dtype("bfloat16")
        else:
            dtype_name = None
            torch_dt = None
    else:
        dtype_name = dtype_raw
        torch_dt = _torch_dtype(dtype_raw)

    mk: dict[str, Any] = dict(model_kwargs or {})
    if torch_dt is not None and "torch_dtype" not in mk:
        mk["torch_dtype"] = torch_dt
    if resolved_device == "cuda" and "attn_implementation" not in mk:
        mk["attn_implementation"] = "sdpa"

    seq = DEFAULT_MAX_SEQ_LENGTH if max_seq_length is None else int(max_seq_length)
    if seq < 1:
        raise ValueError(f"max_seq_length must be >= 1; got {seq}")

    return {
        "device": resolved_device,
        "dtype_name": dtype_name,
        "torch_dtype": torch_dt,
        "max_seq_length": seq,
        "model_kwargs": mk,
        "warn_cpu_large": resolved_device == "cpu" and _looks_large_model(model_id),
    }


def load_sentence_transformer(
    model_id: str | None = None,
    *,
    device: str | None = None,
    dtype: str | None = "auto",
    max_seq_length: int | None = DEFAULT_MAX_SEQ_LENGTH,
    model_kwargs: dict[str, Any] | None = None,
) -> Any:
    from sentence_transformers import SentenceTransformer

    mid = model_id or DEFAULT_ENCODER_MODEL_ID
    opts = resolve_load_kwargs(
        mid,
        device=device,
        dtype=dtype,
        max_seq_length=max_seq_length,
        model_kwargs=model_kwargs,
    )
    if opts["warn_cpu_large"]:
        warnings.warn(
            f"Loading large model {mid!r} on CPU; expect very slow encode / possible OOM. "
            "Use a CUDA device for Qwen3-Embedding-8B and similar sizes.",
            UserWarning,
            stacklevel=2,
        )
        _log.warning("large model on CPU: %s", mid)

    st_kwargs: dict[str, Any] = {"device": opts["device"]}
    if opts["model_kwargs"]:
        st_kwargs["model_kwargs"] = opts["model_kwargs"]
    model = SentenceTransformer(mid, **st_kwargs)
    model.max_seq_length = int(opts["max_seq_length"])
    # Stash resolved options for callers that persist run metadata
    model._claims_encode_opts = {  # type: ignore[attr-defined]
        "device": opts["device"],
        "dtype": opts["dtype_name"],
        "max_seq_length": opts["max_seq_length"],
    }
    return model


def encode_texts(
    model: Any,
    texts: list[str],
    *,
    batch_size: int = 32,
    normalize_embeddings: bool = True,
    prompt: str | None = None,
) -> np.ndarray:
    kwargs: dict[str, Any] = {
        "batch_size": batch_size,
        "normalize_embeddings": normalize_embeddings,
        "show_progress_bar": len(texts) > 256,
        "convert_to_numpy": True,
    }
    p = (prompt or "").strip()
    if p:
        kwargs["prompt"] = p
    arr = model.encode(texts, **kwargs)
    return np.asarray(arr, dtype=np.float64)
