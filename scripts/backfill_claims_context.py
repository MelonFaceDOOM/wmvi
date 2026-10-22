"""Fill platform context fields on an existing claims.json.

Does not re-fetch post text, hits, or chunks, and does not re-run claim
extraction. Run on a machine that can reach Postgres::

    python -m scripts.backfill_claims_context \\
      --claims PATH_TO_CLAIMS.json \\
      --prod

``--dry-run`` prints fill counts and leaves the file unchanged.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from db.db import close_pool, getcursor, init_pool
from scripts.get_posts_for_search_term import CONTEXT_FIELDS, _sql_fetch_post_context_for_ids

# Fields that should be filled for that platform. Others stay null.
PLATFORM_CONTEXT_FIELDS: dict[str, tuple[str, ...]] = {
    "reddit_submission": ("reddit_submission_title", "reddit_subreddit"),
    "reddit_comment": ("reddit_comment_submission_title", "reddit_subreddit"),
    "youtube_video": ("youtube_video_title", "youtube_channel_title"),
    "youtube_comment": ("youtube_video_title", "youtube_channel_title"),
    "podcast_episode": ("podcast_name", "podcast_episode_title"),
    "telegram_post": ("telegram_channel_name",),
}

_CONTEXT_CHUNK = 2000


def _text(val: Any) -> str | None:
    if val is None:
        return None
    text = str(val).strip()
    return text or None


def apply_post_context(post: dict[str, Any], context: dict[str, Any]) -> None:
    """Set context fields from the database.

    A non-empty value already on the post is kept when the database value is
    empty, so a missed join cannot wipe a title the file already has.
    """
    for key in CONTEXT_FIELDS:
        incoming = _text(context.get(key))
        if incoming:
            post[key] = incoming
        elif not _text(post.get(key)):
            post[key] = None


def merge_claims_context(
    posts: list[dict[str, Any]],
    by_id: dict[int, dict[str, Any]],
) -> dict[str, int]:
    """Attach context onto ``posts`` in place. Returns updated and missing counts."""
    updated = 0
    missing = 0
    for post in posts:
        if not isinstance(post, dict):
            missing += 1
            continue
        try:
            post_id = int(post.get("post_id"))
        except (TypeError, ValueError):
            missing += 1
            continue
        context = by_id.get(post_id)
        if context is None:
            missing += 1
            continue
        apply_post_context(post, context)
        updated += 1
    return {"updated": updated, "missing": missing}


def context_fill_lines(posts: list[dict[str, Any]]) -> list[str]:
    by_platform: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for post in posts:
        if isinstance(post, dict):
            by_platform[str(post.get("platform") or "unknown")].append(post)
    lines: list[str] = []
    for platform, rows in sorted(by_platform.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        keys = PLATFORM_CONTEXT_FIELDS.get(platform)
        if not keys:
            continue
        n = len(rows)
        parts = [
            f"{key}={sum(1 for row in rows if _text(row.get(key)))}/{n}"
            for key in keys
        ]
        lines.append(f"{platform} n={n} " + " ".join(parts))
    return lines


def fetch_post_context(
    post_ids: list[int],
    *,
    chunk_size: int = _CONTEXT_CHUNK,
) -> dict[int, dict[str, Any]]:
    """Load context columns for ``post_ids``. Requires an open pool."""
    sql = _sql_fetch_post_context_for_ids()
    step = max(1, int(chunk_size))
    out: dict[int, dict[str, Any]] = {}
    ids = [int(i) for i in post_ids]
    with getcursor() as cur:
        for start in range(0, len(ids), step):
            batch = ids[start : start + step]
            cur.execute(sql, (batch,))
            for row in cur.fetchall():
                post_id = int(row[0])
                values = tuple(row[2:])
                if len(values) != len(CONTEXT_FIELDS):
                    raise ValueError(
                        f"context row has {len(values)} fields, expected {len(CONTEXT_FIELDS)}"
                    )
                out[post_id] = dict(zip(CONTEXT_FIELDS, values))
            print(
                f"[context] {min(start + step, len(ids))}/{len(ids)} posts",
                flush=True,
            )
    return out


def _post_ids(posts: list[Any]) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()
    for post in posts:
        if not isinstance(post, dict):
            continue
        try:
            post_id = int(post.get("post_id"))
        except (TypeError, ValueError):
            continue
        if post_id not in seen:
            seen.add(post_id)
            ids.append(post_id)
    return ids


def _write_claims(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.replace(tmp, path)


def load_claims(path: Path) -> dict[str, Any]:
    """Parse ``path`` once. ``json.load`` avoids holding the raw file text too."""
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} is not a JSON object")
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError(f"{path} has no posts list")
    return payload


def backfill_claims(
    path: Path,
    by_id: dict[int, dict[str, Any]],
    *,
    payload: dict[str, Any] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Merge ``by_id`` into ``payload`` and rewrite ``path`` unless ``dry_run``.

    Pass the object already returned by :func:`load_claims`. A second parse of a
    multi-hundred-megabyte claims file is what got the process killed.
    """
    if payload is None:
        payload = load_claims(path)
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError(f"{path} has no posts list")
    stats = merge_claims_context(posts, by_id)
    by_id.clear()
    lines = context_fill_lines(posts)
    for line in lines:
        print(line, flush=True)
    print(
        f"updated={stats['updated']} missing={stats['missing']} dry_run={dry_run}",
        flush=True,
    )
    if not dry_run:
        _write_claims(path, payload)
        print(f"wrote {path}", flush=True)
    return {"lines": lines, **stats, "dry_run": dry_run}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Add platform context fields to an existing claims.json")
    ap.add_argument("--claims", type=Path, required=True, help="Nested claims.json to update in place")
    ap.add_argument("--prod", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="Print fill counts and do not write")
    args = ap.parse_args(argv)

    path = args.claims
    if not path.is_file():
        raise SystemExit(f"claims file not found: {path}")

    payload = load_claims(path)
    ids = _post_ids(payload["posts"])
    print(f"[context] {len(ids)} post ids in {path}", flush=True)

    init_pool(prefix="prod" if args.prod else "dev")
    try:
        by_id = fetch_post_context(ids)
    finally:
        close_pool()
    del ids

    backfill_claims(path, by_id, payload=payload, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
