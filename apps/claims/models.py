"""Registered embedder models under data/models/<tag>/."""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io

META_FILE = "model.json"


def validate_tag(tag: str) -> str:
    return corpus_mod.validate_model_tag(tag)


def model_dir(tag: str) -> Path:
    return claims_io.models_dir() / validate_tag(tag)


def list_models() -> list[dict[str, Any]]:
    root = claims_io.models_dir()
    if not root.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(root.iterdir()):
        if not p.is_dir() or p.name.startswith("."):
            continue
        meta_path = p / META_FILE
        meta: dict[str, Any] = {}
        if meta_path.is_file():
            try:
                meta = claims_io.read_json(meta_path)
            except Exception:  # noqa: BLE001
                meta = {}
        out.append(
            {
                "tag": p.name,
                "path": str(p.resolve()),
                "source": meta.get("source"),
                "registered_at": meta.get("registered_at"),
                "mode": meta.get("mode"),
            }
        )
    return out


def resolve_model(model_or_tag: str) -> str:
    """Resolve a registered tag to an absolute path; otherwise return as-is."""
    raw = (model_or_tag or "").strip()
    if not raw:
        raise ValueError("model id/path/tag is empty")
    # Absolute / relative filesystem path that exists wins
    p = Path(raw).expanduser()
    if p.exists():
        return str(p.resolve())
    # Registered tag
    try:
        tag = validate_tag(raw)
    except ValueError:
        return raw  # HF id like BAAI/bge-large-en-v1.5
    dest = model_dir(tag)
    if dest.is_dir() and any(dest.iterdir()):
        return str(dest.resolve())
    return raw


def register_model(
    *,
    path: Path,
    tag: str,
    mode: str = "symlink",
    force: bool = False,
) -> dict[str, Any]:
    """Register a local model directory under data/models/<tag>/."""
    src = Path(path).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"Model path not found: {src}")
    tag = validate_tag(tag)
    dest = model_dir(tag)
    claims_io.models_dir().mkdir(parents=True, exist_ok=True)
    if dest.exists() or dest.is_symlink():
        if not force:
            raise FileExistsError(f"Model tag already registered: {dest} (pass --force)")
        if dest.is_symlink() or dest.is_file():
            dest.unlink()
        else:
            shutil.rmtree(dest)

    mode = (mode or "symlink").strip().lower()
    if mode == "symlink":
        os.symlink(src, dest, target_is_directory=src.is_dir())
    elif mode == "copy":
        if src.is_dir():
            shutil.copytree(src, dest)
        else:
            dest.mkdir(parents=True)
            shutil.copy2(src, dest / src.name)
    else:
        raise ValueError("mode must be 'symlink' or 'copy'")

    meta = {
        "tag": tag,
        "source": str(src),
        "mode": mode,
        "registered_at": datetime.now(timezone.utc).isoformat(),
    }
    # Write meta next to the link when possible; if dest is a symlink to a dir,
    # write meta inside dest (source tree) — prefer a sidecar outside:
    sidecar = claims_io.models_dir() / f"{tag}.json"
    claims_io.write_json(sidecar, meta)
    # Also try writing inside if dest is a real directory we own
    if dest.is_dir() and not dest.is_symlink():
        claims_io.write_json(dest / META_FILE, meta)
    return {"tag": tag, "path": str(dest.resolve()), **meta}
