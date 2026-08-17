"""Canonical nested claims-JSON readers for the file-mode pipeline and labs.

Expected ``claims.json`` shape (measles / nest writer)::

    { terms, since, until, model, posts: [
        { post_id, platform, text, hits, chunks: [
            { chunk_index, text, task_id, claim_extraction_disposition,
              claims: [{ claim }] }
        ]}
    ]}
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

# Chunks with these dispositions are never treated as usable claim sources.
_SKIP_DISPOSITIONS = frozenset({"terminal_failure", "retryable_failure", "unprocessed"})


@dataclass
class ClaimRecord:
    """One extracted claim in context of its parent post + chunk."""

    task_id: str
    claim_index: int
    post_row: dict[str, Any]
    claim: dict[str, Any]
    input_text: str | None = None


def stable_task_id(row: dict[str, Any]) -> str:
    tid = row.get("task_id")
    if tid is not None and str(tid).strip():
        return str(tid)
    src = row.get("source_post_id", row.get("post_id"))
    idx = row.get("sentence_boundary_chunk_index", row.get("chunk_index"))
    if src is not None and idx is not None:
        return f"{src}:{idx}"
    return str(row.get("post_id", "unknown"))


def input_text_for_row(row: dict[str, Any]) -> str:
    """Best-effort extraction text (platform title prefixes)."""
    text = row.get("text_coreference_resolved")
    if not isinstance(text, str) or not text.strip():
        text = row.get("text")
    if not isinstance(text, str):
        return ""
    platform = str(row.get("platform", "unknown"))
    if platform == "reddit_submission":
        return f"Submission title: {row.get('reddit_submission_title') or 'Unknown'}\n\n{text}"
    if platform == "reddit_comment":
        return f"Reddit comment context title: {row.get('reddit_comment_submission_title') or 'Unknown'}\n\n{text}"
    if platform == "youtube_video":
        return f"YouTube video title: {row.get('youtube_video_title') or 'Unknown'}\n\n{text}"
    if platform == "podcast_episode":
        return f"Podcast name: {row.get('podcast_name') or 'Unknown'}\n\n{text}"
    return text


def context_row_for_chunk(post: dict[str, Any], chunk: dict[str, Any]) -> dict[str, Any]:
    """Merge post metadata with chunk text/task_id for downstream consumers."""
    ctx = {k: v for k, v in post.items() if k != "chunks"}
    tid = chunk.get("task_id")
    if tid is None or not str(tid).strip():
        tid = stable_task_id({**ctx, **chunk})
    ctx["task_id"] = str(tid)
    chunk_text = chunk.get("text")
    if isinstance(chunk_text, str) and chunk_text.strip():
        ctx["text"] = chunk_text
    if chunk.get("chunk_index") is not None:
        ctx["chunk_index"] = chunk.get("chunk_index")
    return ctx


def chunk_is_usable(chunk: dict[str, Any]) -> bool:
    """True when the chunk has a claims list and is not a hard failure / unprocessed."""
    if not isinstance(chunk, dict):
        return False
    disp = chunk.get("claim_extraction_disposition")
    if disp in _SKIP_DISPOSITIONS:
        return False
    claims = chunk.get("claims")
    return isinstance(claims, list) and len(claims) > 0


def iter_success_claim_records(
    posts: list[dict[str, Any]],
    *,
    max_posts: int | None = None,
    max_claims: int | None = None,
) -> Iterator[ClaimRecord]:
    """Yield ClaimRecord for nested posts→chunks→claims with usable chunks."""
    posts_seen = 0
    claims_emitted = 0
    for post in posts:
        if not isinstance(post, dict):
            continue
        chunks = post.get("chunks")
        if not isinstance(chunks, list):
            continue
        usable = [c for c in chunks if isinstance(c, dict) and chunk_is_usable(c)]
        if not usable:
            continue
        if max_posts is not None and posts_seen >= max_posts:
            break
        posts_seen += 1
        for chunk in usable:
            ctx = context_row_for_chunk(post, chunk)
            tid = str(ctx["task_id"])
            itext = input_text_for_row(ctx) or None
            claims = chunk.get("claims") or []
            for i, c in enumerate(claims):
                if max_claims is not None and claims_emitted >= max_claims:
                    return
                if not isinstance(c, dict):
                    continue
                claims_emitted += 1
                yield ClaimRecord(
                    task_id=tid,
                    claim_index=i,
                    post_row=ctx,
                    claim=c,
                    input_text=itext,
                )


def count_nested_claims(posts: list[dict[str, Any]]) -> tuple[int, int]:
    """Return (post_count, claim_count) walking nested chunks (all claim entries)."""
    n_claims = 0
    for post in posts:
        if not isinstance(post, dict):
            continue
        chunks = post.get("chunks")
        if not isinstance(chunks, list):
            continue
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            claims = chunk.get("claims")
            if isinstance(claims, list):
                n_claims += len(claims)
    return len([p for p in posts if isinstance(p, dict)]), n_claims


def load_posts_from_claims_json(path: Path | str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load ``{ ... meta, posts: [...] }`` JSON; returns (payload, posts list)."""
    p = Path(path)
    payload = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Expected JSON object at top level.")
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("Expected top-level 'posts' list.")
    cleaned = [x for x in posts if isinstance(x, dict)]
    return payload, cleaned
