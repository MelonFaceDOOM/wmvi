"""Claims posts-JSON adapters for sentence-boundary trim (impl in ``nlp.trim``)."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from nlp.trim import (  # noqa: F401 — re-export public algorithm APIs
    CHUNK_CHAR_LIMIT,
    FAR_HIT_GAP_CHARS,
    MAX_CHARS_AFTER,
    MAX_CHARS_BEFORE,
    MAX_SENTENCES,
    SENTENCES_AFTER,
    SENTENCES_BEFORE,
    build_contexts_for_post,
    syntok_sentence_spans,
    trim_sentence_boundary,
)


def trim_posts_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Add sentence-boundary chunks to each post in a posts JSON object."""
    out = copy.deepcopy(payload)
    posts = out.get("posts")
    if not isinstance(posts, list):
        raise ValueError("Input JSON must contain top-level 'posts' list.")
    trimmed: list[dict[str, Any]] = []
    for post in posts:
        if not isinstance(post, dict):
            continue
        body = post.get("text")
        hits = post.get("hits")
        chunks = trim_sentence_boundary(
            body if isinstance(body, str) else "",
            hits if isinstance(hits, list) else [],
        )
        row = dict(post)
        row["sentence_boundary_chunks"] = chunks
        row["sentence_boundary_chunk_count"] = len(chunks)
        trimmed.append(row)
    out["posts"] = trimmed
    return out


def run(*, posts_path: Path, out_path: Path) -> dict[str, Any]:
    """Trim posts JSON and write result. Returns a small summary dict."""
    raw = json.loads(posts_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Input JSON must be an object with 'posts'.")
    result = trim_posts_payload(raw)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    n = len(result.get("posts") or [])
    return {"ok": True, "out": str(out_path), "post_count": n}
