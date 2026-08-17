"""Triplet pairwise evaluation (file-mode)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np

from apps.claims import io as claims_io
from apps.claims.types import EmbedConfig, TripletAnchor


@dataclass
class AnchorScore:
    pass_pct: float
    pairs_total: int
    pairs_pass: int
    per_member: dict[str, Any]


def load_triplets_json(path: Path) -> list[TripletAnchor]:
    """Load anchors from JSON list or {anchors: [...]} object."""
    data = claims_io.read_json(path)
    rows = data.get("anchors") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        raise ValueError("Triplets JSON must be a list or {anchors: [...]}")
    out: list[TripletAnchor] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        out.append(
            TripletAnchor(
                id=int(row.get("id", i + 1)),
                text=str(row.get("text") or row.get("anchor") or ""),
                pool=str(row.get("pool") or "eval"),
                category=str(row.get("category") or ""),
                family=str(row.get("family") or ""),
                too_hard=bool(row.get("too_hard", False)),
                positives=[str(x) for x in (row.get("positives") or [])],
                negatives=[str(x) for x in (row.get("negatives") or [])],
            )
        )
    return out


def dump_triplets_json(path: Path, anchors: list[TripletAnchor]) -> None:
    payload = {
        "anchors": [
            {
                "id": a.id,
                "text": a.text,
                "pool": a.pool,
                "category": a.category,
                "family": a.family,
                "too_hard": a.too_hard,
                "positives": list(a.positives or []),
                "negatives": list(a.negatives or []),
            }
            for a in anchors
        ]
    }
    claims_io.write_json(path, payload)


def score_anchor(
    anchor_text: str,
    positives: list[str],
    negatives: list[str],
    *,
    model_id: str = "",
    doc_instruction: str = "",
    normalize: bool = True,
    embed_fn: Callable[[list[str]], np.ndarray] | None = None,
) -> AnchorScore:
    positives = [p.strip() for p in positives if p.strip()]
    negatives = [n.strip() for n in negatives if n.strip()]
    if not positives or not negatives:
        return AnchorScore(pass_pct=0.0, pairs_total=0, pairs_pass=0, per_member={"positives": [], "negatives": []})

    all_texts = [anchor_text] + positives + negatives
    if embed_fn is not None:
        vecs = np.asarray(embed_fn(all_texts), dtype=np.float32)
    else:
        from apps.claims.embedding.encode import encode_texts, load_sentence_transformer

        encoder = load_sentence_transformer(model_id)
        prompt = (doc_instruction or "").strip() or None
        vecs = np.asarray(
            encode_texts(encoder, all_texts, normalize_embeddings=normalize, prompt=prompt),
            dtype=np.float32,
        )
    idx = {t: i for i, t in enumerate(all_texts)}

    def sim(a: str, b: str) -> float:
        return float(np.dot(vecs[idx[a]], vecs[idx[b]]))

    pairs_total = 0
    pairs_pass = 0
    for pos in positives:
        sp = sim(anchor_text, pos)
        for neg in negatives:
            sn = sim(anchor_text, neg)
            pairs_total += 1
            if sp > sn:
                pairs_pass += 1
    pass_pct = pairs_pass / pairs_total if pairs_total else 0.0
    return AnchorScore(
        pass_pct=pass_pct,
        pairs_total=pairs_total,
        pairs_pass=pairs_pass,
        per_member={"positives": [], "negatives": []},
    )


def aggregate_scores(
    anchors: list[TripletAnchor],
    scores: dict[int, AnchorScore],
) -> tuple[float, dict[str, float]]:
    vals = [scores[a.id].pass_pct for a in anchors if a.id in scores]
    overall = float(np.mean(vals)) if vals else 0.0
    by_cat: dict[str, list[float]] = {}
    for a in anchors:
        if a.id not in scores:
            continue
        cat = a.category or "uncategorized"
        by_cat.setdefault(cat, []).append(scores[a.id].pass_pct)
    return overall, {k: float(np.mean(v)) for k, v in by_cat.items()}


def run(
    *,
    anchors: list[TripletAnchor],
    config: EmbedConfig,
    pool: str = "eval",
) -> dict[str, Any]:
    from apps.claims.embedding.encode import encode_texts, load_sentence_transformer

    scorable = [
        a
        for a in anchors
        if (not pool or a.pool == pool)
        and not a.too_hard
        and a.positives
        and a.negatives
    ]
    encoder = load_sentence_transformer(
        config.model_id,
        device=config.device,
        dtype=config.dtype,
        max_seq_length=config.max_seq_length,
    )
    prompt = (config.doc_instruction or "").strip() or None

    def embed_fn(texts: list[str]) -> np.ndarray:
        return np.asarray(
            encode_texts(
                encoder,
                texts,
                batch_size=config.batch_size,
                normalize_embeddings=config.normalize,
                prompt=prompt,
            ),
            dtype=np.float32,
        )

    scores: dict[int, AnchorScore] = {}
    for anchor in scorable:
        scores[anchor.id] = score_anchor(
            anchor.text,
            anchor.positives or [],
            anchor.negatives or [],
            embed_fn=embed_fn,
        )
    overall, by_cat = aggregate_scores(scorable, scores)
    return {
        "pool": pool,
        "n_scored": len(scorable),
        "overall_pass_pct": round(overall, 4),
        "by_category": {k: round(v, 4) for k, v in by_cat.items()},
        "per_anchor": [
            {
                "id": a.id,
                "text": a.text,
                "category": a.category,
                "pass_pct": round(scores[a.id].pass_pct, 4),
                "pairs_pass": scores[a.id].pairs_pass,
                "pairs_total": scores[a.id].pairs_total,
            }
            for a in scorable
        ],
    }
