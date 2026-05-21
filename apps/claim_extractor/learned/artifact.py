"""On-disk layout for one Ridge head (BGE encoder id + Ridge weights + metadata)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

ARTIFACT_META = "artifact_meta.json"
RIDGE_HEAD_FILE = "ridge_head.joblib"
TRAIN_CONFIG_FILE = "train_config.json"
METRICS_FILE = "metrics.json"


@dataclass
class ArtifactMeta:
    encoder_model_id: str
    head_kind: str  # "ridge"
    normalize_embeddings: bool
    head_name: str = ""
    input_var_keys: list[str] = field(default_factory=list)
    # Legacy field name in older artifacts (maps to head_name when loading)
    score_field: str | None = None


def save_artifact_dir(
    out_dir: Path,
    *,
    meta: ArtifactMeta,
    train_config: dict[str, Any],
    metrics: dict[str, Any],
    ridge_head: Any,
) -> None:
    import joblib

    out_dir.mkdir(parents=True, exist_ok=True)
    payload = asdict(meta)
    (out_dir / ARTIFACT_META).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / TRAIN_CONFIG_FILE).write_text(
        json.dumps(train_config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (out_dir / METRICS_FILE).write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    joblib.dump(ridge_head, out_dir / RIDGE_HEAD_FILE)


def load_meta(out_dir: Path) -> ArtifactMeta:
    raw = json.loads((out_dir / ARTIFACT_META).read_text(encoding="utf-8"))
    head_name = str(raw.get("head_name") or raw.get("score_field") or "")
    keys = raw.get("input_var_keys")
    if not isinstance(keys, list):
        keys = []
    return ArtifactMeta(
        encoder_model_id=str(raw["encoder_model_id"]),
        head_kind=str(raw.get("head_kind") or "ridge"),
        normalize_embeddings=bool(raw.get("normalize_embeddings", True)),
        head_name=head_name,
        input_var_keys=[str(k) for k in keys],
        score_field=raw.get("score_field") if isinstance(raw.get("score_field"), str) else None,
    )
