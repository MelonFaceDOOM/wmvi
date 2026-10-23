"""Load post-chunks from claims.json and sample them for extraction."""

from __future__ import annotations

import random
from collections import defaultdict
from pathlib import Path
from typing import Any

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims.claims_data import load_posts_from_claims_json, stable_task_id
from apps.claims.events.mention import document_id

DEFAULT_SAMPLE_SEED = 17
DEFAULT_PLATFORM_FLOOR = 50
DEFAULT_MAX_BODY_CHARS = 12_000

# Title belongs to this post (not a parent thread).
TITLE_FIELDS = {
    "reddit_submission": "reddit_submission_title",
    "youtube_video": "youtube_video_title",
}

CHANNEL_FIELDS = {
    "telegram_post": "telegram_channel",
    "podcast_episode": "podcast_name",
}


def _optional_str(val: Any) -> str | None:
    text = str(val or "").strip()
    return text or None


def native_title(post: dict[str, Any]) -> str | None:
    key = TITLE_FIELDS.get(str(post.get("platform") or ""))
    if not key:
        return None
    return _optional_str(post.get(key))


def native_channel(post: dict[str, Any]) -> str | None:
    key = CHANNEL_FIELDS.get(str(post.get("platform") or ""))
    if not key:
        return None
    return _optional_str(post.get(key))


def thread_title(post: dict[str, Any]) -> str | None:
    """Parent-thread title (metadata only; never sent to the LLM)."""
    return _optional_str(post.get("reddit_comment_submission_title"))


def chunk_body(chunk: dict[str, Any], post: dict[str, Any]) -> str:
    text = chunk.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    fallback = post.get("text")
    if isinstance(fallback, str):
        return fallback.strip()
    return ""


def _posted_at(post: dict[str, Any]) -> str | None:
    return _optional_str(post.get("created_at_ts"))


def iter_documents(
    posts: list[dict[str, Any]],
    *,
    corpus: str,
    max_body_chars: int = DEFAULT_MAX_BODY_CHARS,
) -> list[dict[str, Any]]:
    """Every chunk with non-empty text. Does not require successful claim extraction."""
    out: list[dict[str, Any]] = []
    for post in posts:
        if not isinstance(post, dict):
            continue
        chunks = post.get("chunks")
        if not isinstance(chunks, list) or not chunks:
            body = _optional_str(post.get("text"))
            if not body:
                continue
            chunks = [{"chunk_index": 0, "text": body, "task_id": stable_task_id(post)}]
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            body = chunk_body(chunk, post)
            if not body:
                continue
            truncated = False
            if max_body_chars and len(body) > max_body_chars:
                body = body[: max_body_chars]
                truncated = True
            chunk_index = int(chunk.get("chunk_index") or 0)
            post_id = post.get("post_id")
            ctx = {k: v for k, v in post.items() if k != "chunks"}
            ctx["chunk_index"] = chunk_index
            task_id = str(chunk.get("task_id") or "").strip() or stable_task_id(ctx)
            out.append(
                {
                    "doc_id": document_id(post_id, chunk_index),
                    "corpus": corpus,
                    "post_id": post_id,
                    "chunk_index": chunk_index,
                    "task_id": task_id,
                    "platform": str(post.get("platform") or "unknown"),
                    "posted_at": _posted_at(post),
                    "title": native_title(post),
                    "body": body,
                    "channel": native_channel(post),
                    "thread_title": thread_title(post),
                    "url": _optional_str(post.get("url")),
                    "truncated": truncated,
                }
            )
    return out


def allocate_stratified(
    counts: dict[str, int],
    n: int,
    *,
    floor: int = DEFAULT_PLATFORM_FLOOR,
) -> dict[str, int]:
    """Floor per large platform, remainder proportional to leftover capacity."""
    plats = sorted(counts)
    alloc = {p: 0 for p in plats}
    if not plats or n <= 0:
        return alloc

    n_big = sum(1 for p in plats if counts[p] >= floor)
    use_floor = floor
    if n_big * floor > n:
        use_floor = max(1, n // max(1, len(plats)))

    for p in plats:
        if counts[p] >= use_floor:
            alloc[p] = min(use_floor, counts[p])

    while sum(alloc.values()) > n:
        donors = [p for p in plats if alloc[p] > 0]
        if not donors:
            break
        donors.sort(key=lambda p: (-alloc[p], p))
        alloc[donors[0]] -= 1

    remain = n - sum(alloc.values())
    cap = {p: counts[p] - alloc[p] for p in plats}
    total_cap = sum(cap.values())
    if remain <= 0 or total_cap <= 0:
        return alloc

    # Largest-remainder on leftover capacity.
    raw = []
    for p in plats:
        share = remain * (cap[p] / total_cap) if total_cap else 0.0
        whole = min(cap[p], int(share))
        raw.append((share - whole, p, whole))
    extra = {p: whole for _frac, p, whole in raw}
    leftover = remain - sum(extra.values())
    raw.sort(key=lambda t: (-t[0], t[1]))
    for _frac, p, _whole in raw:
        if leftover <= 0:
            break
        if extra[p] < cap[p]:
            extra[p] += 1
            leftover -= 1
    # If still leftover (caps), fill any remaining capacity in platform order.
    if leftover > 0:
        for p in plats:
            room = cap[p] - extra[p]
            take = min(room, leftover)
            extra[p] += take
            leftover -= take
            if leftover <= 0:
                break
    for p in plats:
        alloc[p] += extra[p]
    return alloc


def stratified_sample(
    docs: list[dict[str, Any]],
    n: int | None,
    *,
    seed: int = DEFAULT_SAMPLE_SEED,
    floor: int = DEFAULT_PLATFORM_FLOOR,
) -> list[dict[str, Any]]:
    if n is None or n >= len(docs):
        return list(docs)
    if n <= 0:
        return []
    by_plat: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for doc in docs:
        by_plat[str(doc.get("platform") or "unknown")].append(doc)
    counts = {p: len(rows) for p, rows in by_plat.items()}
    alloc = allocate_stratified(counts, int(n), floor=floor)
    rng = random.Random(int(seed))
    chosen: list[dict[str, Any]] = []
    for plat in sorted(by_plat):
        k = alloc.get(plat, 0)
        rows = by_plat[plat]
        if k <= 0:
            continue
        if k >= len(rows):
            chosen.extend(rows)
        else:
            chosen.extend(rng.sample(rows, k))
    chosen.sort(key=lambda d: (str(d.get("platform") or ""), str(d.get("doc_id") or "")))
    return chosen


def load_corpus_documents(corpus: str, *, max_body_chars: int = DEFAULT_MAX_BODY_CHARS) -> list[dict[str, Any]]:
    paths = corpus_mod.get_corpus(corpus)
    if not paths.claims.is_file():
        raise FileNotFoundError(f"Missing claims.json at {paths.claims}")
    _payload, posts = load_posts_from_claims_json(paths.claims)
    return iter_documents(posts, corpus=corpus, max_body_chars=max_body_chars)


def write_documents(path: Path, docs: list[dict[str, Any]]) -> None:
    claims_io.write_jsonl(path, docs)


def load_documents(path: Path) -> list[dict[str, Any]]:
    return claims_io.read_jsonl(path)
