"""Encode claim texts for an embedding profile and persist vectors + metrics.

Heavy arrays go to disk as float32 ``.npy``; metadata + hardware metrics go to
SQLite and a ``metrics.json`` sidecar. The encoder model is cached process-wide.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import streamlit as st

from apps.claim_extractor.embedding_lab.claims_data import ClaimGroup
from apps.claim_extractor.embedding_lab.db import EmbedProfile
from apps.claim_extractor.learned.encode import encode_texts, load_sentence_transformer

VECTORS_FILE = "vectors.npy"
INDEX_FILE = "index.json"
METRICS_FILE = "metrics.json"
LABELS_FILE_TMPL = "cluster_{cluster_profile_id}.npy"

_ENCODE_CHUNK = 2048


@st.cache_resource(show_spinner=False)
def get_encoder(model_id: str) -> Any:
    """Process-wide cached sentence-transformers model."""
    return load_sentence_transformer(model_id)


def _peak_ram_mb() -> float | None:
    try:
        import resource

        # ru_maxrss is KiB on Linux.
        return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0
    except Exception:
        return None


def probe_cuda() -> dict[str, Any]:
    """Lightweight PyTorch CUDA probe (no model load)."""
    info: dict[str, Any] = {
        "cuda_available": False,
        "device": "cpu",
        "cuda_device_name": None,
        "cuda_device_count": 0,
        "cuda_version": None,
        "torch_version": None,
    }
    try:
        import torch

        info["torch_version"] = torch.__version__
        info["torch_built_cuda"] = torch.version.cuda
        info["cpu_only_wheel"] = torch.version.cuda is None
        if torch.cuda.is_available():
            info["cuda_available"] = True
            info["device"] = "cuda"
            info["cuda_device_count"] = int(torch.cuda.device_count())
            info["cuda_device_name"] = torch.cuda.get_device_name(0)
            info["cuda_version"] = torch.version.cuda
    except Exception as exc:
        info["torch_error"] = str(exc)
    return info


def _device_info_from_encoder(encoder: Any) -> dict[str, Any]:
    """Merge PyTorch CUDA probe with the encoder's actual ``.device``."""
    info = probe_cuda()
    enc_dev = getattr(encoder, "device", None)
    enc_dev_str = str(enc_dev) if enc_dev is not None else "unknown"
    info["encoder_device"] = enc_dev_str
    if "cuda" in enc_dev_str:
        info["device"] = enc_dev_str if ":" in enc_dev_str else "cuda"
    else:
        info["device"] = "cpu"
    return info


def probe_compute_device(
    *,
    model_id: str | None = None,
    run_encode_test: bool = False,
) -> dict[str, Any]:
    """Probe CUDA and optionally load the encoder + run a one-line encode test."""
    info = probe_cuda()
    if not model_id:
        return info

    try:
        encoder = get_encoder(model_id)
        info = _device_info_from_encoder(encoder)
    except Exception as exc:
        info["encoder_error"] = str(exc)
        return info

    if not run_encode_test:
        return info

    try:
        vec = encode_texts(encoder, ["gpu device test"], normalize_embeddings=True)
        info["encode_test_ok"] = True
        info["encode_test_dim"] = int(vec.shape[1])
    except Exception as exc:
        info["encode_test_ok"] = False
        info["encode_error"] = str(exc)
    return info


def format_device_report(info: dict[str, Any]) -> str:
    """Markdown summary for the embedding-lab UI."""
    lines: list[str] = []
    if info.get("torch_error"):
        lines.append(f"PyTorch error: `{info['torch_error']}`")
    elif info.get("cuda_available"):
        lines.append(
            f"**CUDA available** — {info.get('cuda_device_name', '?')} "
            f"({info.get('cuda_device_count', 0)} device(s)"
            + (f", driver CUDA {info['cuda_version']}" if info.get("cuda_version") else "")
            + ")"
        )
    else:
        lines.append("**CUDA not available** — embedding will use CPU.")
        if info.get("cpu_only_wheel"):
            lines.append(
                "This PyTorch build has **no CUDA support** (CPU-only wheel). "
                "`nvidia-smi` can show a GPU, but this Python install cannot use it."
            )
            lines.append(
                "Reinstall from repo root: `pip install -r requirements-torch.txt` "
                "(or see `requirements-torch-cpu.txt` for CPU-only hosts)"
            )
        elif info.get("torch_built_cuda"):
            lines.append(
                f"PyTorch was built for CUDA {info['torch_built_cuda']} but "
                "`torch.cuda.is_available()` is false — check drivers, `CUDA_VISIBLE_DEVICES`, "
                "or whether another process holds the GPU."
            )

    if info.get("torch_version"):
        lines.append(f"PyTorch `{info['torch_version']}`")

    if info.get("encoder_device"):
        lines.append(f"Encoder device: `{info['encoder_device']}`")

    if info.get("encoder_error"):
        lines.append(f"Encoder load failed: `{info['encoder_error']}`")
    elif info.get("encode_test_ok") is True:
        lines.append(f"Encode test: **OK** (dim {info.get('encode_test_dim')})")
    elif info.get("encode_test_ok") is False:
        lines.append(f"Encode test failed: `{info.get('encode_error', '?')}`")

    return "\n\n".join(lines)


def device_progress_message(info: dict[str, Any]) -> str:
    """Short status line shown as soon as the encoder is loaded."""
    enc = info.get("encoder_device") or info.get("device") or "cpu"
    if "cuda" in str(enc):
        name = info.get("cuda_device_name") or enc
        return f"Using GPU — {name} (encoder on {enc})"
    if info.get("cuda_available") and not str(enc).startswith("cuda"):
        return f"Using CPU — encoder on {enc} (CUDA visible to PyTorch but not used)"
    return f"Using CPU — encoder on {enc}"


def _torch_cuda() -> Any | None:
    info = probe_cuda()
    if not info.get("cuda_available"):
        return None
    try:
        import torch

        return torch
    except Exception:
        return None


def _apply_doc_instruction(texts: list[str], instruction: str) -> list[str]:
    instr = (instruction or "").strip()
    if not instr:
        return texts
    return [f"{instr} {t}" for t in texts]


def _group_to_dict(group: ClaimGroup) -> dict[str, Any]:
    return {
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


def normalize_index(index: dict[str, Any]) -> dict[str, Any]:
    """Ensure ``groups`` is present; synthesize from legacy per-row index if needed."""
    if index.get("groups"):
        if not index.get("claim_texts"):
            index["claim_texts"] = [g.get("claim_text", "") for g in index["groups"]]
        return index

    claim_texts = index.get("claim_texts") or []
    task_ids = index.get("task_ids") or []
    claim_indices = index.get("claim_indices") or []
    row_ids = index.get("row_ids") or []
    groups: list[dict[str, Any]] = []
    for i, text in enumerate(claim_texts):
        groups.append(
            {
                "claim_text": text,
                "count": 1,
                "sources": [
                    {
                        "task_id": task_ids[i] if i < len(task_ids) else "?",
                        "claim_index": int(claim_indices[i]) if i < len(claim_indices) else 0,
                        "row_id": row_ids[i] if i < len(row_ids) else f"?:{i}",
                    }
                ],
            }
        )
    index["groups"] = groups
    if not index.get("claim_texts"):
        index["claim_texts"] = claim_texts
    return index


def run_embedding(
    *,
    profile: EmbedProfile,
    groups: list[ClaimGroup],
    source_hash: str,
    source_path: str,
    source_claim_count: int,
    artifact_dir: Path,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    """Encode one vector per claim group, write artifacts, and return metrics."""
    artifact_dir.mkdir(parents=True, exist_ok=True)
    raw_texts = [g.claim_text for g in groups]
    texts = _apply_doc_instruction(raw_texts, profile.doc_instruction)
    total = len(texts)

    encoder = get_encoder(profile.model_id)
    device_info = _device_info_from_encoder(encoder)
    device = str(device_info["device"])
    if on_progress:
        on_progress(0, total, device_progress_message(device_info))

    torch_mod = _torch_cuda()
    if torch_mod is not None:
        try:
            torch_mod.cuda.reset_peak_memory_stats()
        except Exception:
            pass
    ram_before = _peak_ram_mb()

    t0 = time.monotonic()
    chunks: list[np.ndarray] = []
    done = 0
    for start in range(0, total, _ENCODE_CHUNK):
        chunk = texts[start : start + _ENCODE_CHUNK]
        vecs = encode_texts(encoder, chunk, normalize_embeddings=profile.normalize)
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

    vectors_path = artifact_dir / VECTORS_FILE
    np.save(vectors_path, vectors)
    index = {
        "model_id": profile.model_id,
        "source_hash": source_hash,
        "claim_texts": raw_texts,
        "groups": [_group_to_dict(g) for g in groups],
    }
    (artifact_dir / INDEX_FILE).write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")

    ram_after = _peak_ram_mb()
    peak_gpu_mb = None
    if torch_mod is not None:
        try:
            peak_gpu_mb = float(torch_mod.cuda.max_memory_allocated()) / (1024.0 * 1024.0)
        except Exception:
            peak_gpu_mb = None

    artifact_bytes = int(vectors_path.stat().st_size) if vectors_path.is_file() else 0
    metrics = {
        "device": device,
        "wall_seconds": round(wall_seconds, 3),
        "claims_per_sec": round(total / wall_seconds, 2) if wall_seconds > 0 else None,
        "peak_ram_mb": round(ram_after, 1) if ram_after is not None else None,
        "ram_delta_mb": (
            round(ram_after - ram_before, 1) if ram_after is not None and ram_before is not None else None
        ),
        "peak_gpu_mb": round(peak_gpu_mb, 1) if peak_gpu_mb is not None else None,
        "artifact_bytes": artifact_bytes,
        "claim_count": total,
        "source_claim_count": source_claim_count,
        "vector_dim": vector_dim,
        "model_id": profile.model_id,
    }
    (artifact_dir / METRICS_FILE).write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    return metrics


def load_run_arrays(artifact_dir: Path) -> tuple[np.ndarray, dict[str, Any]]:
    """Load vectors + normalized index for an embed run."""
    vectors = np.load(artifact_dir / VECTORS_FILE)
    index = json.loads((artifact_dir / INDEX_FILE).read_text(encoding="utf-8"))
    return np.asarray(vectors, dtype=np.float32), normalize_index(index)


def embed_query(model_id: str, text: str, *, query_instruction: str) -> np.ndarray:
    """Embed a single search phrase, prepending the query instruction (query side only)."""
    encoder = get_encoder(model_id)
    instr = (query_instruction or "").strip()
    payload = f"{instr} {text}".strip() if instr else text
    vec = encode_texts(encoder, [payload], normalize_embeddings=True)
    return np.asarray(vec, dtype=np.float32)[0]
