"""Sidecar annotations keyed by claim_key (or row_id).

Promoted corpus variables only (not candidate model eval dumps).

Layout::

    data/corpora/<corpus>/annotations/<name>.jsonl   # {"k": ..., "v": ...}
    data/corpora/<corpus>/annotations/<name>.meta.json
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from apps.claims import io as claims_io
from apps.claims.keys import claim_key


VALID_SCOPES = ("group", "claim")
VALID_PRODUCER_KINDS = (
    "agent_consensus",
    "agent_label",
    "model_prediction",
    "pipeline",
    "manual",
    "derived",
)
VALID_VALUE_TYPES = ("binary", "float", "int", "string", "categorical")


@dataclass
class AnnotationMeta:
    name: str
    scope: str  # "group" | "claim"
    producer: str
    model: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    source_hash: str | None = None
    count: int = 0
    created_at: str = ""
    # Lifecycle fields (optional for backward compatibility)
    intent: str | None = None
    spec_version: int | None = None
    value_type: str | None = None
    producer_kind: str | None = None
    model_id: str | None = None
    model_hash: str | None = None
    annotation_version: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AnnotationMeta:
        return cls(
            name=str(data["name"]),
            scope=str(data.get("scope") or "group"),
            producer=str(data.get("producer") or ""),
            model=data.get("model"),
            params=dict(data.get("params") or {}),
            source_hash=data.get("source_hash"),
            count=int(data.get("count") or 0),
            created_at=str(data.get("created_at") or ""),
            intent=(str(data["intent"]) if data.get("intent") is not None else None),
            spec_version=(int(data["spec_version"]) if data.get("spec_version") is not None else None),
            value_type=(str(data["value_type"]) if data.get("value_type") is not None else None),
            producer_kind=(
                str(data["producer_kind"]) if data.get("producer_kind") is not None else None
            ),
            model_id=(str(data["model_id"]) if data.get("model_id") is not None else None),
            model_hash=(str(data["model_hash"]) if data.get("model_hash") is not None else None),
            annotation_version=(
                str(data["annotation_version"])
                if data.get("annotation_version") is not None
                else None
            ),
        )


@dataclass
class Annotation:
    meta: AnnotationMeta
    values: dict[str, Any]  # key -> value

    @property
    def name(self) -> str:
        return self.meta.name


def annotations_dir(corpus_root: Path) -> Path:
    return Path(corpus_root) / "annotations"


def annotation_paths(corpus_root: Path, name: str) -> tuple[Path, Path]:
    """Return (jsonl_path, meta_path) for an annotation name."""
    safe = _safe_name(name)
    root = annotations_dir(corpus_root)
    return root / f"{safe}.jsonl", root / f"{safe}.meta.json"


def _safe_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        raise ValueError("annotation name must be non-empty")
    if "/" in n or "\\" in n or ".." in n or n.startswith("."):
        raise ValueError(f"Invalid annotation name {name!r}")
    return n


def write_annotation(
    corpus_root: Path,
    name: str,
    values: dict[str, Any] | Iterable[tuple[str, Any]],
    *,
    scope: str = "group",
    producer: str,
    model: str | None = None,
    params: dict[str, Any] | None = None,
    source_hash: str | None = None,
    force: bool = False,
    intent: str | None = None,
    spec_version: int | None = None,
    value_type: str | None = None,
    producer_kind: str | None = None,
    model_id: str | None = None,
    model_hash: str | None = None,
    annotation_version: str | None = None,
) -> Annotation:
    """Write ``<name>.jsonl`` + ``<name>.meta.json``. Overwrite only with force.

    Keep ``params`` slim — do not store text snapshots or training audit logs.
    Prefer versioned names (e.g. ``epi_value__v1``) over overwriting different models.
    """
    if scope not in VALID_SCOPES:
        raise ValueError(f"scope must be one of {VALID_SCOPES}, got {scope!r}")
    if value_type is not None and value_type not in VALID_VALUE_TYPES:
        raise ValueError(f"value_type must be one of {VALID_VALUE_TYPES}, got {value_type!r}")
    jsonl_path, meta_path = annotation_paths(corpus_root, name)
    if (jsonl_path.exists() or meta_path.exists()) and not force:
        raise FileExistsError(
            f"Annotation {name!r} already exists at {jsonl_path.parent}; pass force=True to overwrite"
        )

    if isinstance(values, dict):
        items = list(values.items())
    else:
        items = list(values)

    # Strip bulky keys from params if callers pass them accidentally
    clean_params = dict(params or {})
    for bulky in ("labeled_texts", "texts", "training_rows", "predictions"):
        clean_params.pop(bulky, None)

    rows = [{"k": str(k), "v": v} for k, v in items]
    meta = AnnotationMeta(
        name=_safe_name(name),
        scope=scope,
        producer=producer,
        model=model,
        params=clean_params,
        source_hash=source_hash,
        count=len(rows),
        created_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        intent=intent,
        spec_version=spec_version,
        value_type=value_type,
        producer_kind=producer_kind,
        model_id=model_id or model,
        model_hash=model_hash,
        annotation_version=annotation_version,
    )
    claims_io.write_jsonl(jsonl_path, rows)
    claims_io.write_json(meta_path, meta.to_dict())
    return Annotation(meta=meta, values={str(k): v for k, v in items})


def read_annotation(corpus_root: Path, name: str) -> Annotation:
    jsonl_path, meta_path = annotation_paths(corpus_root, name)
    if not meta_path.is_file():
        raise FileNotFoundError(f"Missing annotation meta: {meta_path}")
    if not jsonl_path.is_file():
        raise FileNotFoundError(f"Missing annotation data: {jsonl_path}")
    meta = AnnotationMeta.from_dict(claims_io.read_json(meta_path))
    values: dict[str, Any] = {}
    with jsonl_path.open(encoding="utf-8") as fh:
        import json

        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            values[str(row["k"])] = row["v"]
    return Annotation(meta=meta, values=values)


def list_annotations(corpus_root: Path) -> list[AnnotationMeta]:
    root = annotations_dir(corpus_root)
    if not root.is_dir():
        return []
    out: list[AnnotationMeta] = []
    for meta_path in sorted(root.glob("*.meta.json")):
        try:
            out.append(AnnotationMeta.from_dict(claims_io.read_json(meta_path)))
        except Exception:  # noqa: BLE001
            continue
    return out


def remove_annotation(corpus_root: Path, name: str) -> None:
    jsonl_path, meta_path = annotation_paths(corpus_root, name)
    missing = not jsonl_path.exists() and not meta_path.exists()
    if missing:
        raise FileNotFoundError(f"Annotation {name!r} not found under {annotations_dir(corpus_root)}")
    if jsonl_path.exists():
        jsonl_path.unlink()
    if meta_path.exists():
        meta_path.unlink()


def join_into_groups(
    groups: list[dict[str, Any]],
    annotation: Annotation,
    *,
    field_name: str | None = None,
) -> list[dict[str, Any]]:
    """Return shallow copies of group dicts with annotation values attached.

    For ``scope=group``, looks up ``claim_key`` (or computes from ``claim_text``).
    For ``scope=claim``, attaches a ``{field_name}_by_row`` map keyed by ``row_id``.
    """
    field = field_name or annotation.name
    values = annotation.values
    out: list[dict[str, Any]] = []
    if annotation.meta.scope == "group":
        for g in groups:
            row = dict(g)
            key = str(g.get("claim_key") or claim_key(str(g.get("claim_text") or "")))
            if key in values:
                row[field] = values[key]
            out.append(row)
        return out

    # claim (occurrence) scope
    for g in groups:
        row = dict(g)
        by_row: dict[str, Any] = {}
        for src in g.get("sources") or []:
            rid = str(src.get("row_id") or "")
            if rid and rid in values:
                by_row[rid] = values[rid]
        if by_row:
            row[f"{field}_by_row"] = by_row
        out.append(row)
    return out


def annotation_is_fresh(corpus_root: Path, name: str, *, source_hash: str | None) -> bool:
    """True when annotation exists and its meta source_hash matches (if provided)."""
    try:
        ann = read_annotation(corpus_root, name)
    except FileNotFoundError:
        return False
    if source_hash is None:
        return True
    return (ann.meta.source_hash or "") == source_hash
