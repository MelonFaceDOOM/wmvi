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
    """Top neighbors for a corpus claim using its stored embedding vector."""
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


def neighbors_for_query_vector(
    query: np.ndarray,
    *,
    vectors: np.ndarray,
    claim_texts: list[str],
    top_k: int = 15,
    exclude_text: str | None = None,
) -> list[tuple[int, float, str]]:
    """Top neighbors for an arbitrary query vector (cosine via dot if L2-normalized)."""
    q = np.asarray(query, dtype=np.float32).reshape(-1)
    if q.size == 0 or len(vectors) == 0:
        return []
    scores = vectors @ q
    k = int(min(max(top_k * 2, top_k), len(scores)))
    order = np.argsort(-scores)[:k]
    out: list[tuple[int, float, str]] = []
    excl = (exclude_text or "").strip()
    for i in order:
        i = int(i)
        txt = claim_texts[i] if i < len(claim_texts) else ""
        if not txt or not txt.strip():
            continue
        if excl and txt.strip() == excl:
            continue
        out.append((i, float(scores[i]), txt))
        if len(out) >= top_k:
            break
    return out


def neighbors_for_query_text(
    query_text: str,
    *,
    vectors: np.ndarray,
    claim_texts: list[str],
    model_id: str,
    query_instruction: str = "",
    doc_instruction: str = "",
    top_k: int = 15,
    device: str | None = None,
    dtype: str | None = "auto",
    max_seq_length: int | None = None,
) -> list[tuple[int, float, str]]:
    """Top neighbors for arbitrary text (embed query, then search corpus).

    Prefer ``doc_instruction`` (same prompt used to build ``vectors``) for
    symmetric claim–claim search.
    """
    from apps.claims.clustering.query_eval import embed_query

    q = embed_query(
        model_id,
        query_text,
        doc_instruction=doc_instruction,
        query_instruction=query_instruction,
        device=device,
        dtype=dtype,
        max_seq_length=max_seq_length,
    )
    return neighbors_for_query_vector(
        q,
        vectors=vectors,
        claim_texts=claim_texts,
        top_k=top_k,
        exclude_text=query_text,
    )


def format_neighbors_list(neighbors: list[tuple[int, float, str]]) -> str:
    lines: list[str] = []
    for n, (idx, score, text) in enumerate(neighbors, start=1):
        one_line = " ".join(text.split())
        lines.append(f"{n}. score={score:.4f} (idx={idx}): {one_line}")
    return "\n".join(lines) if lines else "(no neighbors)"
