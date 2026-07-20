"""File-mode run export/import (zip of vectors + index + metrics)."""

from __future__ import annotations

import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io

MANIFEST_FILE = "manifest.json"
BUNDLE_FILES = (
    claims_io.VECTORS_FILE,
    claims_io.INDEX_FILE,
    claims_io.METRICS_FILE,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_manifest(run_dir: Path, metrics: dict[str, Any] | None = None) -> dict[str, Any]:
    metrics = metrics or {}
    if not metrics:
        metrics_path = run_dir / claims_io.METRICS_FILE
        if metrics_path.is_file():
            metrics = claims_io.read_json(metrics_path)
    return {
        "format": "claims-run-v1",
        "created_at": _utc_now_iso(),
        "source_run_dir": str(run_dir.resolve()),
        "model_id": metrics.get("model_id"),
        "source_hash": metrics.get("source_hash"),
        "claim_count": metrics.get("claim_count"),
        "vector_dim": metrics.get("vector_dim"),
    }


def export_run(*, run_dir: Path, out_zip: Path) -> dict[str, Any]:
    run_dir = Path(run_dir)
    if not run_dir.is_dir():
        raise FileNotFoundError(f"Run dir not found: {run_dir}")
    missing = [name for name in BUNDLE_FILES if not (run_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Run dir missing required files: {missing}")

    metrics = claims_io.read_json(run_dir / claims_io.METRICS_FILE)
    manifest = build_manifest(run_dir, metrics)
    out_zip = Path(out_zip)
    out_zip.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(MANIFEST_FILE, json.dumps(manifest, indent=2) + "\n")
        for name in BUNDLE_FILES:
            zf.write(run_dir / name, arcname=name)

    return {
        "ok": True,
        "out": str(out_zip.resolve()),
        "run_dir": str(run_dir.resolve()),
        "manifest": manifest,
    }


def import_run(
    *,
    from_zip: Path,
    run_name: str,
    force: bool = False,
) -> dict[str, Any]:
    from_zip = Path(from_zip)
    if not from_zip.is_file():
        raise FileNotFoundError(f"Bundle not found: {from_zip}")

    dest = claims_io.runs_dir() / run_name
    if dest.exists() and not force:
        raise FileExistsError(f"Run already exists: {dest} (pass --force to overwrite)")
    if dest.exists() and force:
        import shutil

        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(from_zip, "r") as zf:
        names = set(zf.namelist())
        missing = [n for n in BUNDLE_FILES if n not in names]
        if missing:
            raise ValueError(f"Bundle missing required members: {missing}")
        for name in BUNDLE_FILES:
            zf.extract(name, path=dest)
        if MANIFEST_FILE in names:
            zf.extract(MANIFEST_FILE, path=dest)
        else:
            metrics = claims_io.read_json(dest / claims_io.METRICS_FILE)
            claims_io.write_json(dest / MANIFEST_FILE, build_manifest(dest, metrics))

    manifest = claims_io.read_json(dest / MANIFEST_FILE)
    return {
        "ok": True,
        "run_name": run_name,
        "run_dir": str(dest.resolve()),
        "manifest": manifest,
    }
