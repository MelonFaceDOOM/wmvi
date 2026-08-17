"""Immutable labeler model versions and aliases under data/models/labelers/."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims import provenance as prov
from apps.claims.labeling.artifact import ARTIFACT_META, MANIFEST_FILE, RIDGE_HEAD_FILE


def intent_models_dir(intent: str) -> Path:
    return claims_io.labeler_models_dir() / prov.safe_slug(intent)


def model_dir(intent: str, version: str) -> Path:
    return intent_models_dir(intent) / prov.safe_slug(version)


def alias_path(intent: str, alias: str = "active") -> Path:
    return intent_models_dir(intent) / f"{prov.safe_slug(alias)}.json"


def list_versions(intent: str) -> list[dict[str, Any]]:
    root = intent_models_dir(intent)
    if not root.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(root.iterdir()):
        if not p.is_dir() or p.name.startswith("."):
            continue
        if not (p / RIDGE_HEAD_FILE).is_file() and not (p / ARTIFACT_META).is_file():
            continue
        manifest: dict[str, Any] = {}
        mp = p / MANIFEST_FILE
        if mp.is_file():
            try:
                manifest = claims_io.read_json(mp)
            except Exception:  # noqa: BLE001
                manifest = {}
        out.append(
            {
                "intent": prov.safe_slug(intent),
                "version": p.name,
                "path": str(p.resolve()),
                "created_at": manifest.get("created_at"),
                "dataset_version": manifest.get("dataset_version"),
                "model_hash": manifest.get("model_hash"),
            }
        )
    return out


def resolve_model_ref(ref: str) -> Path:
    """Resolve ``intent/version``, ``intent@alias``, absolute path, or intent (→ active)."""
    raw = (ref or "").strip()
    if not raw:
        raise ValueError("model ref is empty")
    p = Path(raw).expanduser()
    if p.is_dir() and (p / RIDGE_HEAD_FILE).is_file():
        return p.resolve()

    intent: str
    version: str | None = None
    if "@" in raw:
        intent, alias = raw.split("@", 1)
        return resolve_alias(intent, alias.strip() or "active")
    if "/" in raw:
        intent, version = raw.split("/", 1)
        return model_dir(intent, version)
    # bare intent → active alias
    return resolve_alias(raw, "active")


def resolve_alias(intent: str, alias: str = "active") -> Path:
    path = alias_path(intent, alias)
    if not path.is_file():
        raise FileNotFoundError(f"Missing labeler alias {intent!r}@{alias}: {path}")
    data = claims_io.read_json(path)
    version = str(data.get("version") or "")
    if not version:
        raise ValueError(f"Alias file missing version: {path}")
    dest = model_dir(intent, version)
    if not dest.is_dir():
        raise FileNotFoundError(f"Alias points to missing model: {dest}")
    return dest.resolve()


def set_alias(
    intent: str,
    version: str,
    *,
    alias: str = "active",
    force: bool = True,
) -> dict[str, Any]:
    dest = model_dir(intent, version)
    if not dest.is_dir():
        raise FileNotFoundError(f"Model version not found: {dest}")
    path = alias_path(intent, alias)
    if path.exists() and not force:
        raise FileExistsError(f"Alias already exists: {path}")
    payload = {
        "intent": prov.safe_slug(intent),
        "alias": prov.safe_slug(alias),
        "version": prov.safe_slug(version),
        "path": str(dest.resolve()),
        "updated_at": prov.utc_now(),
    }
    # Include model_hash when available
    mp = dest / MANIFEST_FILE
    if mp.is_file():
        try:
            payload["model_hash"] = claims_io.read_json(mp).get("model_hash")
        except Exception:  # noqa: BLE001
            pass
    claims_io.write_json(path, payload)
    return payload


def write_model_manifest(
    intent: str,
    version: str,
    *,
    dataset_version: str,
    dataset_hash: str,
    spec_hash: str,
    encoder_model_id: str,
    train_config: dict[str, Any],
    metrics: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    dest = model_dir(intent, version)
    if not dest.is_dir():
        raise FileNotFoundError(f"Model dir missing: {dest}")
    # Content hash over artifact files
    pieces: list[str] = []
    for name in (ARTIFACT_META, RIDGE_HEAD_FILE, "train_config.json", "metrics.json"):
        fp = dest / name
        if fp.is_file():
            pieces.append(prov.sha256_file(fp))
    manifest: dict[str, Any] = {
        "intent": prov.safe_slug(intent),
        "version": prov.safe_slug(version),
        "kind": "labeler",
        "created_at": prov.utc_now(),
        "dataset_version": dataset_version,
        "dataset_hash": dataset_hash,
        "spec_hash": spec_hash,
        "encoder_model_id": encoder_model_id,
        "train_config": train_config,
        "metrics": metrics,
        "model_hash": prov.sha256_json(pieces),
        "path": str(dest.resolve()),
    }
    if extra:
        manifest.update(extra)
    claims_io.write_json(dest / MANIFEST_FILE, manifest)
    return manifest
