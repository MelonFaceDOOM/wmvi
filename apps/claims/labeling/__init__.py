"""Labeling intents, label rows, and frozen datasets (file-mode)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims import provenance as prov
from apps.claims.keys import claim_key

VALID_VALUE_TYPES = ("binary", "float")
VALID_SCOPES = ("group", "claim")

DEFAULT_MIN_GOLD_TOTAL = 50
DEFAULT_MIN_GOLD_PER_CLASS = 10
DEFAULT_PROBE_TARGET = 25


@dataclass
class LabelSpec:
    name: str
    version: int
    scope: str = "group"
    value_type: str = "binary"
    instructions: str = ""
    value_range: list[float] = field(default_factory=lambda: [0.0, 1.0])
    labels: dict[str, str] = field(default_factory=dict)
    min_gold_total: int = DEFAULT_MIN_GOLD_TOTAL
    min_gold_per_class: int = DEFAULT_MIN_GOLD_PER_CLASS
    probe_target: int = DEFAULT_PROBE_TARGET
    agent_batch_size: int | None = None
    agent_model: str | None = None
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LabelSpec:
        batch_raw = data.get("agent_batch_size")
        model_raw = data.get("agent_model")
        return cls(
            name=str(data["name"]),
            version=int(data.get("version") or 1),
            scope=str(data.get("scope") or "group"),
            value_type=str(data.get("value_type") or "binary"),
            instructions=str(data.get("instructions") or ""),
            value_range=[float(x) for x in (data.get("value_range") or [0.0, 1.0])],
            labels={str(k): str(v) for k, v in dict(data.get("labels") or {}).items()},
            min_gold_total=int(
                data["min_gold_total"]
                if data.get("min_gold_total") is not None
                else DEFAULT_MIN_GOLD_TOTAL
            ),
            min_gold_per_class=int(
                data["min_gold_per_class"]
                if data.get("min_gold_per_class") is not None
                else DEFAULT_MIN_GOLD_PER_CLASS
            ),
            probe_target=int(
                data["probe_target"] if data.get("probe_target") is not None else DEFAULT_PROBE_TARGET
            ),
            agent_batch_size=(int(batch_raw) if batch_raw is not None else None),
            agent_model=(str(model_raw).strip() or None) if model_raw is not None else None,
            created_at=str(data.get("created_at") or ""),
        )


@dataclass
class LabelRow:
    row_id: str
    claim_key: str
    claim_text: str
    value: float
    intent: str
    spec_version: int
    producer: dict[str, Any]
    labeled_at: str
    reason: str | None = None
    confidence: float | None = None
    corpus: str | None = None
    supersedes: str | None = None
    probe_run_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LabelRow:
        return cls(
            row_id=str(data["row_id"]),
            claim_key=str(data["claim_key"]),
            claim_text=str(data.get("claim_text") or ""),
            value=float(data["value"]),
            intent=str(data["intent"]),
            spec_version=int(data.get("spec_version") or 1),
            producer=dict(data.get("producer") or {}),
            labeled_at=str(data.get("labeled_at") or ""),
            reason=(str(data["reason"]) if data.get("reason") is not None else None),
            confidence=(float(data["confidence"]) if data.get("confidence") is not None else None),
            corpus=(str(data["corpus"]) if data.get("corpus") is not None else None),
            supersedes=(str(data["supersedes"]) if data.get("supersedes") is not None else None),
            probe_run_id=(
                str(data["probe_run_id"]) if data.get("probe_run_id") is not None else None
            ),
        )


def intent_dir(name: str) -> Path:
    return claims_io.labeler_training_dir() / prov.safe_slug(name)


def spec_path(name: str) -> Path:
    return intent_dir(name) / claims_io.SPEC_FILE


def labels_path(name: str) -> Path:
    return intent_dir(name) / claims_io.LABELS_FILE


def datasets_dir(name: str) -> Path:
    return intent_dir(name) / "datasets"


def dataset_manifest_path(name: str, version: str) -> Path:
    return datasets_dir(name) / prov.safe_slug(version) / claims_io.MANIFEST_FILE


def create_intent(
    name: str,
    *,
    instructions: str,
    value_type: str = "binary",
    scope: str = "group",
    labels: dict[str, str] | None = None,
    value_range: list[float] | None = None,
    min_gold_total: int = DEFAULT_MIN_GOLD_TOTAL,
    min_gold_per_class: int = DEFAULT_MIN_GOLD_PER_CLASS,
    probe_target: int = DEFAULT_PROBE_TARGET,
    agent_batch_size: int | None = None,
    agent_model: str | None = None,
    version: int = 1,
    force: bool = False,
) -> LabelSpec:
    if value_type not in VALID_VALUE_TYPES:
        raise ValueError(f"value_type must be one of {VALID_VALUE_TYPES}")
    if scope not in VALID_SCOPES:
        raise ValueError(f"scope must be one of {VALID_SCOPES}")
    slug = prov.safe_slug(name)
    path = spec_path(slug)
    if path.exists() and not force:
        raise FileExistsError(f"Label intent already exists: {path}")
    if value_type == "binary":
        lab = labels or {"0": "no", "1": "yes"}
        vr = value_range or [0.0, 1.0]
    else:
        lab = labels or {}
        vr = value_range or [0.0, 1.0]
    batch = int(agent_batch_size) if agent_batch_size is not None else None
    if batch is not None and batch < 1:
        raise ValueError("agent_batch_size must be >= 1")
    model = (str(agent_model).strip() or None) if agent_model is not None else None
    spec = LabelSpec(
        name=slug,
        version=int(version),
        scope=scope,
        value_type=value_type,
        instructions=instructions.strip(),
        value_range=vr,
        labels=lab,
        min_gold_total=int(min_gold_total),
        min_gold_per_class=int(min_gold_per_class),
        probe_target=int(probe_target),
        agent_batch_size=batch,
        agent_model=model,
        created_at=prov.utc_now(),
    )
    claims_io.ensure_data_dirs()
    intent_dir(slug).mkdir(parents=True, exist_ok=True)
    claims_io.write_json(path, spec.to_dict())
    if not labels_path(slug).exists():
        labels_path(slug).write_text("", encoding="utf-8")
    return spec


def load_spec(name: str) -> LabelSpec:
    path = spec_path(name)
    if not path.is_file():
        raise FileNotFoundError(f"Missing label intent spec: {path}")
    return LabelSpec.from_dict(claims_io.read_json(path))


def list_intents() -> list[dict[str, Any]]:
    root = claims_io.labeler_training_dir()
    if not root.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(root.iterdir()):
        if not p.is_dir() or p.name.startswith("."):
            continue
        try:
            spec = load_spec(p.name)
        except Exception:  # noqa: BLE001
            continue
        n = sum(1 for _ in labels_path(p.name).open(encoding="utf-8")) if labels_path(p.name).is_file() else 0
        out.append({**spec.to_dict(), "n_labels": n, "path": str(p)})
    return out


def validate_value(spec: LabelSpec, value: float) -> float:
    v = float(value)
    lo, hi = float(spec.value_range[0]), float(spec.value_range[1])
    if v < lo or v > hi:
        raise ValueError(f"value {v} outside range [{lo}, {hi}] for intent {spec.name}")
    if spec.value_type == "binary" and v not in (0.0, 1.0):
        if abs(v - 0.0) > 1e-12 and abs(v - 1.0) > 1e-12:
            raise ValueError(f"binary intent {spec.name} requires value 0 or 1, got {v}")
    return v


def make_label_row(
    *,
    spec: LabelSpec,
    text: str,
    value: float,
    producer: dict[str, Any],
    reason: str | None = None,
    confidence: float | None = None,
    corpus: str | None = None,
    claim_key_override: str | None = None,
    supersedes: str | None = None,
    labeled_at: str | None = None,
    probe_run_id: str | None = None,
) -> LabelRow:
    ck = claim_key_override or claim_key(text)
    v = validate_value(spec, value)
    ts = labeled_at or prov.utc_now()
    producer = dict(producer or {})
    if "type" not in producer:
        producer["type"] = "unknown"
    rid = prov.label_row_id(
        intent=spec.name,
        claim_key=ck,
        producer_type=str(producer.get("type") or "unknown"),
        labeled_at=ts,
        value=v,
    )
    return LabelRow(
        row_id=rid,
        claim_key=ck,
        claim_text=text,
        value=v,
        intent=spec.name,
        spec_version=spec.version,
        producer=producer,
        labeled_at=ts,
        reason=reason,
        confidence=confidence,
        corpus=corpus,
        supersedes=supersedes,
        probe_run_id=probe_run_id,
    )


def append_label(row: LabelRow) -> Path:
    path = labels_path(row.intent)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        for existing in claims_io.read_jsonl(path):
            if str(existing.get("row_id")) == row.row_id:
                raise FileExistsError(f"Label row already exists: {row.row_id}")
    claims_io.append_jsonl(path, row.to_dict())
    return path


def load_labels(name: str) -> list[LabelRow]:
    return [LabelRow.from_dict(r) for r in claims_io.read_jsonl(labels_path(name))]


def resolved_labels(name: str) -> dict[str, LabelRow]:
    """Latest non-superseded label per claim_key (append order; later wins if supersedes set)."""
    rows = load_labels(name)
    superseded = {r.supersedes for r in rows if r.supersedes}
    active = [r for r in rows if r.row_id not in superseded]
    out: dict[str, LabelRow] = {}
    for r in active:
        out[r.claim_key] = r
    return out


def freeze_dataset(name: str, version: str, *, force: bool = False) -> dict[str, Any]:
    """Freeze train-only dataset; hard-fail if any gold claim_key leaks into train."""
    from apps.claims.labeling import gold as gold_mod

    spec = load_spec(name)
    resolved = resolved_labels(name)
    if not resolved:
        raise ValueError(f"No labels to freeze for intent {name!r}")
    out_path = dataset_manifest_path(name, version)
    if out_path.exists() and not force:
        raise FileExistsError(f"Dataset already exists: {out_path}")

    gkeys = gold_mod.gold_keys(name)
    train_rows = [r for r in resolved.values() if r.claim_key not in gkeys]
    excluded = [r for r in resolved.values() if r.claim_key in gkeys]
    train_keys = {r.claim_key for r in train_rows}
    leak = train_keys & gkeys
    if leak:
        raise ValueError(
            f"Gold claim_keys leaked into train set for intent {name!r}: "
            f"{sorted(leak)[:5]}"
        )

    train_ids = sorted(r.row_id for r in train_rows)
    by_id = {r.row_id: r for r in train_rows}
    gold_refs: dict[str, Any] = {}
    for corpus in gold_mod.list_gold_corpora(name):
        rows = gold_mod.resolved_gold(name, corpus)
        gold_refs[corpus] = {
            "n": len(rows),
            "hash": gold_mod.gold_hash(name, corpus),
        }

    manifest = {
        "intent": spec.name,
        "spec_version": spec.version,
        "dataset_version": prov.safe_slug(version),
        "created_at": prov.utc_now(),
        "n_total": len(train_rows),
        "n_train": len(train_ids),
        "train_row_ids": train_ids,
        "n_excluded_gold_overlap": len(excluded),
        "gold_refs": gold_refs,
        "labels_hash": prov.sha256_json([by_id[i].to_dict() for i in sorted(by_id)]),
        "spec_hash": prov.sha256_json(spec.to_dict()),
    }
    claims_io.write_json(out_path, manifest)
    return manifest


def load_dataset_manifest(name: str, version: str) -> dict[str, Any]:
    path = dataset_manifest_path(name, version)
    if not path.is_file():
        raise FileNotFoundError(f"Missing dataset manifest: {path}")
    return claims_io.read_json(path)


def dataset_rows(name: str, version: str, *, split: str | None = "train") -> list[LabelRow]:
    """Return frozen train rows. ``split`` must be ``train`` or None (same)."""
    manifest = load_dataset_manifest(name, version)
    if split not in (None, "train"):
        raise ValueError(
            "Datasets are train-only under the gold-eval scheme; "
            "use gold labels for evaluation (split must be 'train' or None)"
        )
    wanted = set(manifest.get("train_row_ids") or [])
    by_id = {r.row_id: r for r in load_labels(name)}
    missing = wanted - set(by_id)
    if missing:
        raise ValueError(f"Dataset references missing label rows: {sorted(missing)[:5]}")
    return [by_id[i] for i in sorted(wanted)]
