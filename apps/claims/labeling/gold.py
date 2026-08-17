"""Human-only gold labels per intent+corpus (honest eval yardstick)."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims import provenance as prov
from apps.claims.keys import claim_key

HUMAN_PRODUCER_TYPE = "human"
DEFAULT_MIN_GOLD_TOTAL = 50
DEFAULT_MIN_GOLD_PER_CLASS = 10


@dataclass
class GoldRow:
    claim_key: str
    claim_text: str
    value: float
    corpus: str
    intent: str
    spec_version: int
    labeled_at: str
    producer: dict[str, Any]
    reason: str | None = None
    sampling: str = "random"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GoldRow:
        return cls(
            claim_key=str(data["claim_key"]),
            claim_text=str(data.get("claim_text") or ""),
            value=float(data["value"]),
            corpus=str(data["corpus"]),
            intent=str(data["intent"]),
            spec_version=int(data.get("spec_version") or 1),
            labeled_at=str(data.get("labeled_at") or ""),
            producer=dict(data.get("producer") or {}),
            reason=(str(data["reason"]) if data.get("reason") is not None else None),
            sampling=str(data.get("sampling") or "random"),
        )


def gold_dir(intent: str) -> Path:
    from apps.claims import labeling as label_data

    return label_data.intent_dir(intent) / "gold"


def gold_path(intent: str, corpus: str) -> Path:
    return gold_dir(intent) / f"{prov.safe_slug(corpus)}.jsonl"


def list_gold_corpora(intent: str) -> list[str]:
    root = gold_dir(intent)
    if not root.is_dir():
        return []
    out: list[str] = []
    for p in sorted(root.glob("*.jsonl")):
        out.append(p.stem)
    return out


def read_gold(intent: str, corpus: str | None = None) -> list[GoldRow]:
    if corpus is not None:
        path = gold_path(intent, corpus)
        if not path.is_file():
            return []
        return [GoldRow.from_dict(r) for r in claims_io.read_jsonl(path)]
    rows: list[GoldRow] = []
    for c in list_gold_corpora(intent):
        rows.extend(read_gold(intent, c))
    return rows


def resolved_gold(intent: str, corpus: str | None = None) -> dict[str, GoldRow]:
    """Latest gold row per claim_key (append order; later wins)."""
    out: dict[str, GoldRow] = {}
    for r in read_gold(intent, corpus):
        out[r.claim_key] = r
    return out


def gold_keys(intent: str, corpus: str | None = None) -> set[str]:
    return set(resolved_gold(intent, corpus).keys())


def gold_hash(intent: str, corpus: str) -> str:
    rows = sorted(resolved_gold(intent, corpus).values(), key=lambda r: r.claim_key)
    return prov.sha256_json([r.to_dict() for r in rows])


def append_gold(row: GoldRow) -> Path:
    producer_type = str((row.producer or {}).get("type") or "")
    if producer_type != HUMAN_PRODUCER_TYPE:
        raise ValueError(
            f"Gold labels require producer.type={HUMAN_PRODUCER_TYPE!r}, got {producer_type!r}"
        )
    path = gold_path(row.intent, row.corpus)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Reject duplicate claim_key in this corpus gold file
    existing = resolved_gold(row.intent, row.corpus)
    if row.claim_key in existing:
        raise FileExistsError(
            f"Gold row already exists for claim_key={row.claim_key} corpus={row.corpus}"
        )
    claims_io.append_jsonl(path, row.to_dict())
    return path


def make_gold_row(
    *,
    intent: str,
    spec_version: int,
    text: str,
    value: float,
    corpus: str,
    reason: str | None = None,
    claim_key_override: str | None = None,
    labeled_at: str | None = None,
    sampling: str = "random",
) -> GoldRow:
    from apps.claims import labeling as label_data

    spec = label_data.load_spec(intent)
    v = label_data.validate_value(spec, value)
    ck = claim_key_override or claim_key(text)
    return GoldRow(
        claim_key=ck,
        claim_text=text,
        value=v,
        corpus=prov.safe_slug(corpus),
        intent=spec.name,
        spec_version=int(spec_version or spec.version),
        labeled_at=labeled_at or prov.utc_now(),
        producer={"type": HUMAN_PRODUCER_TYPE},
        reason=reason,
        sampling=sampling,
    )


def expected_input_hint(spec: Any) -> str:
    """Short human hint for interactive gold labeling (allowed values)."""
    labels = dict(getattr(spec, "labels", None) or {})
    value_type = str(getattr(spec, "value_type", "") or "")
    vr = list(getattr(spec, "value_range", None) or [0.0, 1.0])
    if value_type == "binary":
        if labels:
            parts = [f"{k}={v}" for k, v in sorted(labels.items(), key=lambda kv: float(kv[0]))]
            return f"enter 0 or 1 ({', '.join(parts)})"
        return "enter 0 or 1"
    if labels:
        # Prefer discrete buckets from labels keys (e.g. stance 0/0.25/.../1)
        keys = sorted(labels.keys(), key=lambda k: float(k))
        named = ", ".join(f"{k}={labels[k]}" for k in keys)
        return f"enter one of: {' | '.join(keys)} ({named})"
    lo, hi = float(vr[0]), float(vr[1])
    return f"enter a float in [{lo:g}, {hi:g}]"


def _class_key(
    value: float,
    *,
    value_type: str,
    labels: dict[str, str] | None = None,
) -> str:
    """Canonical class id for gold gate counts.

    When ``labels`` is set (e.g. stance buckets ``0.0``…``1.0``), return the
    matching label key string so counts align with ``expected_classes``.
    """
    if value_type == "binary":
        return "1" if abs(float(value) - 1.0) < 1e-12 else "0"
    v = float(value)
    if labels:
        for k in labels:
            try:
                if abs(float(k) - v) < 1e-12:
                    return str(k)
            except ValueError:
                continue
    # No label match: stable short repr (prefer "0.0"/"1.0" over "0"/"1")
    if abs(v - round(v)) < 1e-12:
        return f"{v:.1f}" if abs(v) < 1e6 else str(int(round(v)))
    return f"{v:g}"


def gold_status(
    intent: str,
    corpus: str | None = None,
    *,
    min_total: int | None = None,
    min_per_class: int | None = None,
) -> dict[str, Any]:
    from apps.claims import labeling as label_data

    spec = label_data.load_spec(intent)
    min_t = int(min_total if min_total is not None else spec.min_gold_total)
    min_c = int(min_per_class if min_per_class is not None else spec.min_gold_per_class)
    labels = dict(spec.labels or {})

    corpora = [corpus] if corpus else list_gold_corpora(intent)
    if corpus and corpus not in corpora and not gold_path(intent, corpus).is_file():
        corpora = [corpus]

    by_corpus: dict[str, Any] = {}
    all_pass = True
    for c in corpora:
        rows = list(resolved_gold(intent, c).values())
        counts = Counter(
            _class_key(r.value, value_type=spec.value_type, labels=labels or None)
            for r in rows
        )
        n = len(rows)
        # Expected classes from spec.labels when present, else observed
        expected = set(str(k) for k in labels.keys()) if labels else set(counts)
        if not expected and spec.value_type == "binary":
            expected = {"0", "1"}
        per_class_ok = all(counts.get(cls, 0) >= min_c for cls in expected) if expected else (n >= min_t)
        # If labels dict empty (float without named buckets), require min_per_class on each observed class
        if not labels and counts:
            per_class_ok = all(n_c >= min_c for n_c in counts.values())
            if not expected:
                expected = set(counts)
        gate_ok = n >= min_t and per_class_ok
        if not gate_ok:
            all_pass = False
        by_corpus[c] = {
            "n_total": n,
            "per_class": dict(sorted(counts.items())),
            "min_gold_total": min_t,
            "min_gold_per_class": min_c,
            "expected_classes": sorted(expected) if expected else sorted(counts),
            "gate_ok": gate_ok,
            "gold_hash": gold_hash(intent, c) if n else "",
        }

    return {
        "intent": intent,
        "corpus": corpus,
        "gate_ok": all_pass if by_corpus else False,
        "corpora": by_corpus,
    }


def require_gold_gate(intent: str, corpus: str) -> dict[str, Any]:
    status = gold_status(intent, corpus)
    corp = status["corpora"].get(corpus) or {}
    if not corp.get("gate_ok"):
        from apps.claims import labeling as label_data

        spec = label_data.load_spec(intent)
        raise ValueError(
            f"Insufficient gold for intent={intent!r} corpus={corpus!r}: "
            f"n_total={corp.get('n_total', 0)} per_class={corp.get('per_class', {})} "
            f"(need total>={corp.get('min_gold_total', spec.min_gold_total)} and "
            f"per_class>={corp.get('min_gold_per_class', spec.min_gold_per_class)})"
        )
    return status
