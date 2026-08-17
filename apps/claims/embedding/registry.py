"""Immutable embedder model versions and aliases under data/models/embedders/."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims import provenance as prov

MANIFEST_FILE = claims_io.MANIFEST_FILE


def intent_models_dir(intent: str) -> Path:
    return claims_io.embedder_models_dir() / prov.safe_slug(intent)


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
        # SentenceTransformer dirs typically have config.json / modules.json
        if not ((p / "config.json").is_file() or (p / "modules.json").is_file() or (p / MANIFEST_FILE).is_file()):
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
                "base_model_id": manifest.get("base_model_id"),
            }
        )
    return out


def resolve_model_ref(ref: str) -> Path | str:
    """Resolve ``intent/version``, ``intent@alias``, path, HF id, or legacy registered tag.

    Returns a Path for local artifacts, or a str HF id / path string for external models.
    """
    raw = (ref or "").strip()
    if not raw:
        raise ValueError("model ref is empty")
    p = Path(raw).expanduser()
    if p.exists():
        return p.resolve()

    if "@" in raw and not raw.startswith("http"):
        intent, alias = raw.split("@", 1)
        return resolve_alias(intent, alias.strip() or "active")
    # intent/version under embedders/ (avoid treating HF org/name as local unless exists)
    if "/" in raw:
        intent, version = raw.split("/", 1)
        dest = model_dir(intent, version)
        if dest.is_dir():
            return dest.resolve()
        # Fall through: may be HF id
        from apps.claims import models as models_mod

        return models_mod.resolve_model(raw)

    # bare intent → active
    alias_file = alias_path(raw, "active")
    if alias_file.is_file():
        return resolve_alias(raw, "active")

    from apps.claims import models as models_mod

    return models_mod.resolve_model(raw)


def resolve_alias(intent: str, alias: str = "active") -> Path:
    path = alias_path(intent, alias)
    if not path.is_file():
        raise FileNotFoundError(f"Missing embedder alias {intent!r}@{alias}: {path}")
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
    base_model_id: str,
    base_model_revision: str | None,
    dataset_version: str,
    dataset_hash: str,
    spec_hash: str,
    loss: str,
    hyperparameters: dict[str, Any],
    metrics: dict[str, Any],
    loss_curve: list[float] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    dest = model_dir(intent, version)
    if not dest.is_dir():
        raise FileNotFoundError(f"Model dir missing: {dest}")
    # Hash a few stable files if present
    pieces: list[str] = []
    for name in ("config.json", "modules.json", "sentence_bert_config.json"):
        fp = dest / name
        if fp.is_file():
            pieces.append(prov.sha256_file(fp))
    manifest: dict[str, Any] = {
        "intent": prov.safe_slug(intent),
        "version": prov.safe_slug(version),
        "kind": "embedder",
        "created_at": prov.utc_now(),
        "base_model_id": base_model_id,
        "base_model_revision": base_model_revision,
        "dataset_version": dataset_version,
        "dataset_hash": dataset_hash,
        "spec_hash": spec_hash,
        "loss": loss,
        "hyperparameters": hyperparameters,
        "metrics": metrics,
        "loss_curve": list(loss_curve or []),
        "model_hash": prov.sha256_json(pieces) if pieces else prov.sha256_json(metrics),
        "path": str(dest.resolve()),
    }
    if extra:
        manifest.update(extra)
    claims_io.write_json(dest / MANIFEST_FILE, manifest)
    return manifest
