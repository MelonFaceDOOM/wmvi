"""Punct → trim → explode posts into claim-extractable chunk rows."""

from __future__ import annotations

from typing import Any

from nlp.punct import needs_punctuation, remap_hits_to_text, restore_punctuation
from nlp.trim import trim_sentence_boundary


def prepare_post(post: dict[str, Any]) -> dict[str, Any]:
    """Apply punct (if eligible) + trim; return post with chunk fields.

    Sets:
      - ``punctuation_restored`` (bool)
      - ``text_punct`` (when restored)
      - ``sentence_boundary_chunks`` / ``sentence_boundary_chunk_count``
      - ``trim_source_text`` / ``hits_for_trim`` when working text differs from original
    """
    row = dict(post)
    text = str(row.get("text") or "")
    hits = row.get("hits") if isinstance(row.get("hits"), list) else []
    working = text
    working_hits = [dict(h) for h in hits if isinstance(h, dict)]

    if text.strip() and needs_punctuation(text):
        restored, did = restore_punctuation(text, force=True)
        if did:
            row["text_punct"] = restored
            row["punctuation_restored"] = True
            working = restored
            working_hits = remap_hits_to_text(text, restored, working_hits)
        else:
            row["punctuation_restored"] = False
    else:
        row["punctuation_restored"] = False

    chunks = trim_sentence_boundary(working, working_hits)
    row["sentence_boundary_chunks"] = chunks
    row["sentence_boundary_chunk_count"] = len(chunks)
    if working != text:
        row["trim_source_text"] = working
        row["hits_for_trim"] = working_hits
    return row


def prepare_posts(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [prepare_post(p) for p in posts if isinstance(p, dict)]


def iter_chunk_rows(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Explode prepared posts into one extract row per non-empty chunk.

    Each row has ``source_post_id``, ``sentence_boundary_chunk_index``,
    ``sentence_boundary_chunk_count``, and ``text`` = chunk. The full
    ``sentence_boundary_chunks`` list is dropped from chunk rows.
    """
    out: list[dict[str, Any]] = []
    for post in posts:
        if not isinstance(post, dict):
            continue
        chunks = post.get("sentence_boundary_chunks")
        if not isinstance(chunks, list):
            continue
        chunk_count = len(chunks)
        for idx, chunk in enumerate(chunks):
            if not isinstance(chunk, str) or not chunk.strip():
                continue
            chunk_post = dict(post)
            chunk_post.pop("sentence_boundary_chunks", None)
            chunk_post["source_post_id"] = post.get("post_id")
            chunk_post["sentence_boundary_chunk_index"] = idx
            chunk_post["sentence_boundary_chunk_count"] = chunk_count
            chunk_post["text"] = chunk
            out.append(chunk_post)
    return out


def prepare_and_explode(posts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(prepared_posts, chunk_rows)``."""
    prepared = prepare_posts(posts)
    return prepared, iter_chunk_rows(prepared)
