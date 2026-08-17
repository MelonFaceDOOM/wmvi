"""Human-only gold triplets per similarity intent+corpus (honest eval yardstick)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims import provenance as prov
from apps.claims.keys import claim_key

HUMAN_PRODUCER_TYPE = "human"


@dataclass
class GoldTripletRow:
    """Gold pairwise judgment for one anchor (same core fields as TripletRow)."""

    claim_key: str  # anchor key
    claim_text: str  # anchor text
    positive_keys: list[str]
    positive_texts: list[str]
    negative_keys: list[str]
    negative_texts: list[str]
    corpus: str
    intent: str
    spec_version: int
    labeled_at: str
    producer: dict[str, Any]
    reason: str | None = None
    shown_keys: list[str] = field(default_factory=list)
    run_tag: str | None = None
    sampling: str = "random"

    @property
    def anchor_key(self) -> str:
        return self.claim_key

    @property
    def anchor_text(self) -> str:
        return self.claim_text

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GoldTripletRow:
        ck = str(data.get("claim_key") or data.get("anchor_key") or "")
        text = str(data.get("claim_text") or data.get("anchor_text") or "")
        return cls(
            claim_key=ck,
            claim_text=text,
            positive_keys=[str(x) for x in (data.get("positive_keys") or [])],
            positive_texts=[str(x) for x in (data.get("positive_texts") or [])],
            negative_keys=[str(x) for x in (data.get("negative_keys") or [])],
            negative_texts=[str(x) for x in (data.get("negative_texts") or [])],
            corpus=str(data["corpus"]),
            intent=str(data["intent"]),
            spec_version=int(data.get("spec_version") or 1),
            labeled_at=str(data.get("labeled_at") or ""),
            producer=dict(data.get("producer") or {}),
            reason=(str(data["reason"]) if data.get("reason") is not None else None),
            shown_keys=[str(x) for x in (data.get("shown_keys") or [])],
            run_tag=(str(data["run_tag"]) if data.get("run_tag") is not None else None),
            sampling=str(data.get("sampling") or "random"),
        )


def gold_dir(intent: str) -> Path:
    from apps.claims.embedding import triplets as trip_data

    return trip_data.intent_dir(intent) / "gold"


def gold_path(intent: str, corpus: str) -> Path:
    return gold_dir(intent) / f"{prov.safe_slug(corpus)}.jsonl"


def list_gold_corpora(intent: str) -> list[str]:
    root = gold_dir(intent)
    if not root.is_dir():
        return []
    return [p.stem for p in sorted(root.glob("*.jsonl"))]


def read_gold(intent: str, corpus: str | None = None) -> list[GoldTripletRow]:
    if corpus is not None:
        path = gold_path(intent, corpus)
        if not path.is_file():
            return []
        return [GoldTripletRow.from_dict(r) for r in claims_io.read_jsonl(path)]
    rows: list[GoldTripletRow] = []
    for c in list_gold_corpora(intent):
        rows.extend(read_gold(intent, c))
    return rows


def resolved_gold(intent: str, corpus: str | None = None) -> dict[str, GoldTripletRow]:
    """Latest gold row per anchor claim_key (append order; later wins)."""
    out: dict[str, GoldTripletRow] = {}
    for r in read_gold(intent, corpus):
        out[r.claim_key] = r
    return out


def gold_anchor_keys(intent: str, corpus: str | None = None) -> set[str]:
    return set(resolved_gold(intent, corpus).keys())


def gold_hash(intent: str, corpus: str) -> str:
    rows = sorted(resolved_gold(intent, corpus).values(), key=lambda r: r.claim_key)
    return prov.sha256_json([r.to_dict() for r in rows])


def append_gold(row: GoldTripletRow) -> Path:
    producer_type = str((row.producer or {}).get("type") or "")
    if producer_type != HUMAN_PRODUCER_TYPE:
        raise ValueError(
            f"Gold triplets require producer.type={HUMAN_PRODUCER_TYPE!r}, got {producer_type!r}"
        )
    path = gold_path(row.intent, row.corpus)
    path.parent.mkdir(parents=True, exist_ok=True)
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
    anchor_text: str,
    positive_texts: list[str] | None = None,
    negative_texts: list[str] | None = None,
    corpus: str,
    reason: str | None = None,
    claim_key_override: str | None = None,
    positive_keys_override: list[str] | None = None,
    negative_keys_override: list[str] | None = None,
    shown_keys: list[str] | None = None,
    run_tag: str | None = None,
    labeled_at: str | None = None,
    sampling: str = "random",
) -> GoldTripletRow:
    from apps.claims.embedding import triplets as trip_data

    spec = trip_data.load_spec(intent)
    pos_texts = [str(t) for t in (positive_texts or []) if str(t).strip()]
    neg_texts = [str(t) for t in (negative_texts or []) if str(t).strip()]
    ak = claim_key_override or claim_key(anchor_text)
    pkeys = (
        list(positive_keys_override)
        if positive_keys_override is not None
        else [claim_key(t) for t in pos_texts]
    )
    nkeys = (
        list(negative_keys_override)
        if negative_keys_override is not None
        else [claim_key(t) for t in neg_texts]
    )
    if positive_keys_override is not None and not pos_texts:
        pos_texts = [""] * len(pkeys)
    if negative_keys_override is not None and not neg_texts:
        neg_texts = [""] * len(nkeys)
    if len(pkeys) != len(pos_texts) or len(nkeys) != len(neg_texts):
        raise ValueError("key/text length mismatch for positives or negatives")
    if ak in pkeys or ak in nkeys:
        raise ValueError("positive/negative keys must not include the anchor")
    overlap = set(pkeys) & set(nkeys)
    if overlap:
        raise ValueError(f"positive and negative keys must be disjoint: {sorted(overlap)[:5]}")

    return GoldTripletRow(
        claim_key=ak,
        claim_text=anchor_text,
        positive_keys=list(pkeys),
        positive_texts=list(pos_texts),
        negative_keys=list(nkeys),
        negative_texts=list(neg_texts),
        corpus=prov.safe_slug(corpus),
        intent=spec.name,
        spec_version=int(spec_version or spec.version),
        labeled_at=labeled_at or prov.utc_now(),
        producer={"type": HUMAN_PRODUCER_TYPE},
        reason=reason,
        shown_keys=list(shown_keys or []),
        run_tag=run_tag,
        sampling=sampling,
    )


def gold_status(
    intent: str,
    corpus: str | None = None,
    *,
    min_total: int | None = None,
) -> dict[str, Any]:
    from apps.claims.embedding import triplets as trip_data

    spec = trip_data.load_spec(intent)
    min_t = int(min_total if min_total is not None else spec.min_gold_total)

    corpora = [corpus] if corpus else list_gold_corpora(intent)
    if corpus and corpus not in corpora and not gold_path(intent, corpus).is_file():
        corpora = [corpus]

    by_corpus: dict[str, Any] = {}
    all_pass = True
    for c in corpora:
        rows = list(resolved_gold(intent, c).values())
        n = len(rows)
        n_with_pos = sum(1 for r in rows if r.positive_keys)
        n_with_neg = sum(1 for r in rows if r.negative_keys)
        n_pairwise = sum(1 for r in rows if r.positive_keys and r.negative_keys)
        gate_ok = n >= min_t
        if not gate_ok:
            all_pass = False
        by_corpus[c] = {
            "n_total": n,
            "n_with_pos": n_with_pos,
            "n_with_neg": n_with_neg,
            "n_pairwise": n_pairwise,
            "min_gold_total": min_t,
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
        from apps.claims.embedding import triplets as trip_data

        spec = trip_data.load_spec(intent)
        raise ValueError(
            f"Insufficient gold for similarity intent={intent!r} corpus={corpus!r}: "
            f"n_total={corp.get('n_total', 0)} "
            f"(need total>={corp.get('min_gold_total', spec.min_gold_total)})"
        )
    return status
