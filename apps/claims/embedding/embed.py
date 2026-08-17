"""Encode claim groups to a run directory (no Streamlit/SQLite)."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims import io as claims_io
from apps.claims.keys import claim_key
from apps.claims.types import ClaimGroup, EmbedConfig

_ENCODE_CHUNK = 2048


def _peak_ram_mb() -> float | None:
    try:
        import resource

        return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0
    except Exception:
        return None


def _group_to_dict(group: ClaimGroup) -> dict[str, Any]:
    return {
        "group_id": group.group_id,
        "claim_key": claim_key(group.claim_text),
        "claim_text": group.claim_text,
        "count": group.count,
        "sources": [
            {
                "task_id": s.task_id,
                "claim_index": s.claim_index,
                "row_id": s.row_id,
            }
            for s in group.sources
        ],
    }


def _device_info_from_encoder(encoder: Any) -> dict[str, Any]:
    info: dict[str, Any] = {"device": "cpu"}
    try:
        import torch

        device = getattr(encoder, "device", None)
        if device is not None:
            info["device"] = str(device)
        elif torch.cuda.is_available():
            info["device"] = "cuda"
    except Exception:
        pass
    opts = getattr(encoder, "_claims_encode_opts", None)
    if isinstance(opts, dict):
        if opts.get("device"):
            info["device"] = str(opts["device"])
        if opts.get("dtype") is not None:
            info["dtype"] = opts["dtype"]
        if opts.get("max_seq_length") is not None:
            info["max_seq_length"] = int(opts["max_seq_length"])
    return info


def run(
    *,
    config: EmbedConfig,
    groups: list[ClaimGroup],
    source_hash: str,
    source_path: str,
    source_claim_count: int,
    run_dir: Path,
    on_progress: Callable[[int, int, str], None] | None = None,
    filter_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Encode one vector per claim group; write vectors.npy / index.json / metrics.json."""
    from apps.claims.embedding.encode import encode_texts, load_sentence_transformer

    run_dir.mkdir(parents=True, exist_ok=True)
    raw_texts = [g.claim_text for g in groups]
    total = len(raw_texts)
    prompt = (config.doc_instruction or "").strip() or None
    batch_size = max(1, int(config.batch_size))

    encoder = load_sentence_transformer(
        config.model_id,
        device=config.device,
        dtype=config.dtype,
        max_seq_length=config.max_seq_length,
    )
    device_info = _device_info_from_encoder(encoder)
    device = str(device_info["device"])
    dtype_name = device_info.get("dtype")
    max_seq = int(device_info.get("max_seq_length") or config.max_seq_length)
    if on_progress:
        on_progress(0, total, f"Encoding on {device}")

    ram_before = _peak_ram_mb()
    t0 = time.monotonic()
    chunks: list[np.ndarray] = []
    done = 0
    for start in range(0, total, _ENCODE_CHUNK):
        chunk = raw_texts[start : start + _ENCODE_CHUNK]
        vecs = encode_texts(
            encoder,
            chunk,
            batch_size=batch_size,
            normalize_embeddings=config.normalize,
            prompt=prompt,
        )
        chunks.append(np.asarray(vecs, dtype=np.float32))
        done += len(chunk)
        if on_progress:
            on_progress(done, total, f"Encoded {done}/{total}")
    wall_seconds = time.monotonic() - t0

    vectors = (
        np.vstack(chunks).astype(np.float32, copy=False)
        if chunks
        else np.zeros((0, 0), dtype=np.float32)
    )
    vector_dim = int(vectors.shape[1]) if vectors.ndim == 2 and vectors.shape[0] else 0

    vectors_path = run_dir / claims_io.VECTORS_FILE
    np.save(vectors_path, vectors)
    index: dict[str, Any] = {
        "model_id": config.model_id,
        "query_instruction": config.query_instruction,
        "doc_instruction": config.doc_instruction,
        "normalize": config.normalize,
        "batch_size": batch_size,
        "max_seq_length": max_seq,
        "dtype": dtype_name,
        "device": device,
        "source_hash": source_hash,
        "claim_texts": raw_texts,
        "claim_keys": [claim_key(t) for t in raw_texts],
        "groups": [_group_to_dict(g) for g in groups],
        "vector_dim": vector_dim,
    }
    if filter_meta:
        index["filter"] = filter_meta
    claims_io.write_json(run_dir / claims_io.INDEX_FILE, index, indent=None)

    ram_after = _peak_ram_mb()
    artifact_bytes = int(vectors_path.stat().st_size) if vectors_path.is_file() else 0
    metrics = {
        "device": device,
        "dtype": dtype_name,
        "max_seq_length": max_seq,
        "batch_size": batch_size,
        "wall_seconds": round(wall_seconds, 3),
        "claims_per_sec": round(total / wall_seconds, 2) if wall_seconds > 0 else None,
        "peak_ram_mb": round(ram_after, 1) if ram_after is not None else None,
        "ram_delta_mb": (
            round(ram_after - ram_before, 1) if ram_after is not None and ram_before is not None else None
        ),
        "artifact_bytes": artifact_bytes,
        "claim_count": total,
        "source_claim_count": source_claim_count,
        "vector_dim": vector_dim,
        "model_id": config.model_id,
        "doc_instruction": config.doc_instruction,
        "source_path": source_path,
        "source_hash": source_hash,
    }
    if filter_meta:
        metrics["filter"] = filter_meta
    claims_io.write_json(run_dir / claims_io.METRICS_FILE, metrics)
    return metrics
