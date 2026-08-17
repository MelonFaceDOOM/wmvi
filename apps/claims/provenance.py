"""Deterministic IDs and content hashes for training/model artifacts."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_json(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256_text(raw)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_jsonl_keys(path: Path, *, key_field: str = "k") -> str:
    """Hash ordered claim keys from a jsonl annotation file."""
    keys: list[str] = []
    with Path(path).open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            keys.append(str(row.get(key_field) or ""))
    return sha256_json(keys)


def safe_slug(name: str, *, allow_slash: bool = False) -> str:
    raw = (name or "").strip()
    if not raw:
        raise ValueError("name must be non-empty")
    if ".." in raw or raw.startswith("."):
        raise ValueError(f"Invalid name {name!r}")
    if not allow_slash and ("/" in raw or "\\" in raw):
        raise ValueError(f"Invalid name {name!r}: no path separators")
    if allow_slash:
        parts = [p for p in re.split(r"[/\\]+", raw) if p]
        safe_parts = [re.sub(r"[^a-zA-Z0-9._-]+", "_", p).strip("._-") for p in parts]
        if not all(safe_parts):
            raise ValueError(f"Invalid name {name!r}")
        return "/".join(safe_parts)
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", raw).strip("._-")
    if not safe:
        raise ValueError(f"Invalid name {name!r}")
    return safe


def assign_split(
    identity: str,
    *,
    spec_version: int | str,
    eval_frac: float = 0.15,
    seed: int = 0,
) -> str:
    """Deterministic train/eval assignment (used by embedder triplets, not labelers)."""
    ev = max(0.0, min(1.0, float(eval_frac)))
    if ev <= 0.0:
        return "train"
    if ev >= 1.0:
        return "eval"
    payload = f"split|{seed}|{spec_version}|{identity}".encode("utf-8")
    bucket = int(hashlib.sha256(payload).hexdigest()[:8], 16) / float(0xFFFFFFFF)
    return "eval" if bucket < ev else "train"


def label_row_id(
    *,
    intent: str,
    claim_key: str,
    producer_type: str,
    labeled_at: str,
    value: Any,
) -> str:
    return sha256_json(
        {
            "intent": intent,
            "claim_key": claim_key,
            "producer_type": producer_type,
            "labeled_at": labeled_at,
            "value": value,
        }
    )[:24]


def triplet_row_id(
    *,
    intent: str,
    anchor_key: str,
    positive_keys: Iterable[str],
    negative_keys: Iterable[str],
    labeled_at: str,
) -> str:
    return sha256_json(
        {
            "intent": intent,
            "anchor_key": anchor_key,
            "positive_keys": sorted(str(x) for x in positive_keys),
            "negative_keys": sorted(str(x) for x in negative_keys),
            "labeled_at": labeled_at,
        }
    )[:24]


def selected_keys_hash(keys: Iterable[str]) -> str:
    return sha256_json(sorted({str(k) for k in keys if str(k)}))
