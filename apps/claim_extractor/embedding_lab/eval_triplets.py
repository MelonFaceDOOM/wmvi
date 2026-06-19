"""Score the artificial triplet eval set against an embedding profile.

Each triplet is (anchor, positive, hard-negative). We embed the texts with the
same (symmetric, doc-side) settings used for the corpus, then measure:

- ``accuracy``    = mean[ sim(anchor, positive) > sim(anchor, negative) ]
- ``mean_margin`` = mean[ sim(anchor, positive) - sim(anchor, negative) ]

Higher is better for both. Margin is more sensitive than accuracy when the model
gets the ordering right but the hard-negative is still uncomfortably close.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from apps.claim_extractor.embedding_lab.db import EmbedProfile, Triplet
from apps.claim_extractor.embedding_lab.embed_runner import get_encoder
from apps.claim_extractor.learned.encode import encode_texts


@dataclass
class TripletScore:
    accuracy: float
    mean_margin: float
    triplet_count: int
    per_triplet: list[dict[str, Any]]


def _doc_text(text: str, instruction: str) -> str:
    instr = (instruction or "").strip()
    return f"{instr} {text}" if instr else text


def score_triplets(triplets: list[Triplet], *, profile: EmbedProfile) -> TripletScore:
    if not triplets:
        return TripletScore(accuracy=0.0, mean_margin=0.0, triplet_count=0, per_triplet=[])

    # Deduplicate texts so each unique string is embedded once.
    unique: dict[str, int] = {}
    ordered: list[str] = []
    for t in triplets:
        for text in (t.anchor, t.positive, t.negative):
            if text not in unique:
                unique[text] = len(ordered)
                ordered.append(text)

    encoder = get_encoder(profile.model_id)
    payloads = [_doc_text(s, profile.doc_instruction) for s in ordered]
    vecs = np.asarray(encode_texts(encoder, payloads, normalize_embeddings=profile.normalize), dtype=np.float32)

    def sim(a: str, b: str) -> float:
        va = vecs[unique[a]]
        vb = vecs[unique[b]]
        return float(np.dot(va, vb))

    per_triplet: list[dict[str, Any]] = []
    correct = 0
    margins: list[float] = []
    for t in triplets:
        sp = sim(t.anchor, t.positive)
        sn = sim(t.anchor, t.negative)
        margin = sp - sn
        is_correct = sp > sn
        if is_correct:
            correct += 1
        margins.append(margin)
        per_triplet.append(
            {
                "anchor": t.anchor,
                "positive": t.positive,
                "negative": t.negative,
                "sim_positive": round(sp, 4),
                "sim_negative": round(sn, 4),
                "margin": round(margin, 4),
                "correct": is_correct,
            }
        )

    n = len(triplets)
    return TripletScore(
        accuracy=correct / n,
        mean_margin=float(np.mean(margins)) if margins else 0.0,
        triplet_count=n,
        per_triplet=per_triplet,
    )
