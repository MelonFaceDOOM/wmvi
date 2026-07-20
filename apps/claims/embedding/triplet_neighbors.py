"""Nearest-neighbor lookup for corpus claim vectors."""

from __future__ import annotations

import numpy as np


def neighbors_for_claim_index(
    claim_index: int,
    *,
    vectors: np.ndarray,
    claim_texts: list[str],
    top_k: int = 10,
) -> list[tuple[int, float, str]]:
    if claim_index < 0 or claim_index >= len(vectors):
        return []
    q = vectors[claim_index]
    scores = vectors @ q
    order = np.argsort(-scores)
    out: list[tuple[int, float, str]] = []
    for i in order:
        i = int(i)
        if i == claim_index:
            continue
        txt = claim_texts[i] if i < len(claim_texts) else ""
        if not txt or not txt.strip():
            continue
        out.append((i, float(scores[i]), txt))
        if len(out) >= top_k:
            break
    return out


def format_neighbors_list(neighbors: list[tuple[int, float, str]]) -> str:
    lines: list[str] = []
    for n, (idx, score, text) in enumerate(neighbors, start=1):
        one_line = " ".join(text.split())
        lines.append(f"{n}. score={score:.4f} (idx={idx}): {one_line}")
    return "\n".join(lines) if lines else "(no neighbors)"
