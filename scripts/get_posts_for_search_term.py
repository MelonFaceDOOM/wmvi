"""Fetch posts that matched one or more vaccine search terms.

Already supports a list of terms. Date range filters on
``COALESCE(created_at_ts, date_entered)`` (post time, falling back to ingest time).

Matching ids are resolved once (term-hit index, then a date filter on id
chunks), and posts are hydrated in pages from that list. Only one hydrated
slice is in memory at a time; the next slice is queried after the caller
finishes the current one. The same ``--since`` / ``--until`` / ``--terms``
request can cover any number of posts.

For fetch + punct/trim + claim extract + upload in one command, use
``python -m scripts.get_posts_extract_upload``.

To dump taxonomy term names for ``--terms-file``::

    python -m scripts.list_vaccine_terms --prod --out my_terms.txt
    python -m scripts.list_vaccine_terms --prod --subset core_search_terms

Library::

    from scripts.get_posts_for_search_term import iter_post_batches

    for batch in iter_post_batches(["measles", "mmr"], since=..., until=...):
        ...

CLI::

    python -m scripts.get_posts_for_search_term \\
      --terms measles "mmr vaccine" --since 2024-01-01 --until 2025-01-01 \\
      --out posts.json

    # After fetch, also PUT to nitwitch (needs NITWITCH_UPLOAD_* in .env):
    python -m scripts.get_posts_for_search_term \\
      --terms measles --prod --out measles_posts.json --upload

Browse uploads at https://nitwitch.com/dl/uploads/ then seed a corpus with
``python -m apps.claims corpus copy-posts --name measles --from … --create``.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

import psycopg2
from dotenv import load_dotenv

from db.db import close_pool, getcursor, init_pool

# Matching ids are resolved once up front (a few MB even for large runs), then
# hydrated DEFAULT_ROW_FETCH_SIZE at a time. Joining hits to the posts_all view
# with a date range is a near-full scan of the view; looking up explicit ids is
# a primary-key probe per branch, so the date filter only runs on id chunks.
# Cursors close before a slice is yielded.
DEFAULT_ROW_FETCH_SIZE = 200
_ID_FILTER_CHUNK = 5000
_PAGE_CHAR_BUDGET = 8_000_000
_HYDRATE_ATTEMPTS = 3
_HYDRATE_RETRY_S = (2, 5)


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_utc_datetime(raw: str | datetime | None) -> datetime | None:
    """Parse ISO date/datetime (YYYY-MM-DD or full ISO). Naive values are treated as UTC."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _ensure_utc(raw)
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        raise ValueError(f"Invalid datetime {raw!r}: use YYYY-MM-DD or ISO-8601") from exc
    return _ensure_utc(dt)


def _json_val(v: Any) -> Any:
    if isinstance(v, datetime):
        dt = _ensure_utc(v)
        return dt.isoformat() if dt is not None else None
    return v


def _post_time_expr(alias: str = "p") -> str:
    return f"COALESCE({alias}.created_at_ts, {alias}.date_entered)"


def _date_filter_sql(alias: str = "p") -> tuple[str, list[Any]]:
    """Placeholder — actual params filled by callers via build_date_params."""
    expr = _post_time_expr(alias)
    # always emit both clauses; pass NULL for unused bounds
    return (
        f" AND (%s::timestamptz IS NULL OR {expr} >= %s::timestamptz)"
        f" AND (%s::timestamptz IS NULL OR {expr} < %s::timestamptz)",
        [],
    )


def build_date_params(
    since: datetime | None,
    until: datetime | None,
) -> tuple[Any, Any, Any, Any]:
    """Params for the dual NULL-tolerant date clauses (since, since, until, until)."""
    s = _ensure_utc(since)
    u = _ensure_utc(until)
    return (s, s, u, u)


def _sql_hit_post_ids() -> str:
    return """
        SELECT DISTINCT ph.post_id
        FROM matches.post_term_hit ph
        WHERE ph.term_id IN (
            SELECT id FROM taxonomy.vaccine_term WHERE name = ANY(%s)
        )
    """


def _sql_filter_post_ids() -> str:
    date_sql, _ = _date_filter_sql("p")
    return f"""
        SELECT p.post_id
        FROM sm.posts_all p
        WHERE p.post_id = ANY(%s)
        {date_sql}
    """


# Order matches _context_select_sql(). Hydrate and the claims backfill both read it.
CONTEXT_FIELDS = (
    "reddit_submission_title",
    "reddit_comment_submission_title",
    "reddit_subreddit",
    "telegram_channel",
    "telegram_channel_name",
    "youtube_video_title",
    "youtube_channel_title",
    "podcast_name",
    "podcast_episode_title",
)

_POST_COLUMNS = (
    "post_id",
    "platform",
    "key1",
    "key2",
    "date_entered",
    "created_at_ts",
    "text",
    "tsv_en",
    "is_en",
    "primary_metric",
    "url",
)


def _post_context_joins() -> str:
    """Joins for platform titles, subreddit, channel, and show/episode names.

    Reddit submission ids and comment link_ids are both stored as ``t3_…``.
    YouTube comment ``key1`` is the video id. Telegram's display title is not
    stored; ``telegram_channel_name`` is the public username.
    """
    return """
        LEFT JOIN sm.reddit_submission rs_meta
          ON p.platform = 'reddit_submission'
         AND p.key1 = rs_meta.id
         AND p.key2 = ''
        LEFT JOIN sm.reddit_comment rc_meta
          ON p.platform = 'reddit_comment'
         AND p.key1 = rc_meta.id
         AND p.key2 = ''
        LEFT JOIN sm.reddit_submission rc_sub
          ON p.platform = 'reddit_comment'
         AND rc_sub.id = rc_meta.link_id
        LEFT JOIN sm.telegram_post tp_meta
          ON p.platform = 'telegram_post'
         AND p.key1 = tp_meta.channel_id::text
         AND p.key2 = tp_meta.message_id::text
        LEFT JOIN scrape.telegram_channel_state tcs
          ON p.platform = 'telegram_post'
         AND tp_meta.channel_id = tcs.channel_id
        LEFT JOIN youtube.video yv_meta
          ON p.platform = 'youtube_video'
         AND p.key1 = yv_meta.video_id
         AND p.key2 = ''
        LEFT JOIN youtube.video yc_video
          ON p.platform = 'youtube_comment'
         AND p.key1 = yc_video.video_id
        LEFT JOIN podcasts.episodes pe_meta
          ON p.platform = 'podcast_episode'
         AND p.key1 = pe_meta.id
         AND p.key2 = ''
        LEFT JOIN podcasts.shows ps_meta
          ON p.platform = 'podcast_episode'
         AND pe_meta.podcast_id = ps_meta.id
    """


def _context_select_sql() -> str:
    return """
            rs_meta.title AS reddit_submission_title,
            rc_sub.title AS reddit_comment_submission_title,
            COALESCE(rs_meta.subreddit, rc_meta.subreddit) AS reddit_subreddit,
            tp_meta.channel_id::text AS telegram_channel,
            CASE
                WHEN NULLIF(btrim(tcs.username), '') IS NOT NULL
                    THEN NULLIF(btrim(regexp_replace(tcs.username, '^@', '')), '')
                WHEN tp_meta.link ~ '^https?://t\\.me/c/' THEN NULL
                ELSE NULLIF(substring(tp_meta.link from '^https?://t\\.me/([^/]+)/'), '')
            END AS telegram_channel_name,
            COALESCE(yv_meta.title, yc_video.title) AS youtube_video_title,
            COALESCE(yv_meta.channel_title, yc_video.channel_title) AS youtube_channel_title,
            ps_meta.title AS podcast_name,
            pe_meta.title AS podcast_episode_title
    """


def _sql_fetch_posts_for_ids() -> str:
    return f"""
        SELECT
            p.post_id,
            p.platform,
            p.key1,
            p.key2,
            p.date_entered,
            p.created_at_ts,
            p.text,
            p.tsv_en,
            p.is_en,
            p.primary_metric,
            p.url,
            {_context_select_sql()}
        FROM sm.posts_all p
        {_post_context_joins()}
        WHERE p.post_id = ANY(%s)
        ORDER BY p.post_id
    """


def _sql_fetch_post_context_for_ids() -> str:
    """Same context columns as the post fetch, without body or search hits."""
    return f"""
        SELECT
            p.post_id,
            p.platform,
            {_context_select_sql()}
        FROM sm.posts_all p
        {_post_context_joins()}
        WHERE p.post_id = ANY(%s)
    """


def _row_to_post(row: tuple[Any, ...]) -> dict[str, Any]:
    width = len(_POST_COLUMNS) + len(CONTEXT_FIELDS)
    if len(row) != width:
        raise ValueError(f"post row has {len(row)} columns, expected {width}")
    post: dict[str, Any] = {}
    for key, val in zip(_POST_COLUMNS, row[: len(_POST_COLUMNS)]):
        if key in ("date_entered", "created_at_ts"):
            post[key] = _json_val(_ensure_utc(val))
        elif key == "tsv_en":
            post[key] = str(val) if val is not None else None
        else:
            post[key] = val
    for key, val in zip(CONTEXT_FIELDS, row[len(_POST_COLUMNS) :]):
        post[key] = val
    post["hits"] = []
    return post


def _sql_fetch_hits_for_ids() -> str:
    return """
        WITH term_ids AS (
            SELECT id, name
            FROM taxonomy.vaccine_term
            WHERE name = ANY(%s)
        )
        SELECT
            ph.post_id,
            ph.term_id,
            t.name AS term_name,
            ph.match_start,
            ph.match_end
        FROM matches.post_term_hit ph
        JOIN term_ids t
          ON t.id = ph.term_id
        WHERE ph.post_id = ANY(%s)
        ORDER BY ph.post_id, ph.match_start, ph.match_end, ph.term_id
    """


def _matching_post_ids_cur(
    cur: Any,
    *,
    terms: Sequence[str],
    since: datetime | None,
    until: datetime | None,
) -> list[int]:
    cur.execute(_sql_hit_post_ids(), (list(terms),))
    hit_ids = sorted(int(r[0]) for r in cur.fetchall() if r and r[0] is not None)
    date_params = build_date_params(since, until)
    keep: list[int] = []
    step = max(1, int(_ID_FILTER_CHUNK))
    for i in range(0, len(hit_ids), step):
        cur.execute(_sql_filter_post_ids(), (hit_ids[i : i + step], *date_params))
        keep.extend(int(r[0]) for r in cur.fetchall() if r and r[0] is not None)
    keep.sort()
    return keep


def find_matching_post_ids(
    terms: Sequence[str],
    *,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[int]:
    """Sorted ids of posts in ``posts_all`` that hit ``terms`` within [since, until).

    Requires an initialized pool.
    """
    if not terms:
        return []
    with getcursor() as cur:
        return _matching_post_ids_cur(
            cur,
            terms=terms,
            since=_ensure_utc(since),
            until=_ensure_utc(until),
        )


def matching_post_ids(
    terms: Sequence[str],
    *,
    use_prod: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[int]:
    """Like :func:`find_matching_post_ids`, opening and closing the DB pool."""
    init_pool(prefix="prod" if use_prod else "dev")
    try:
        return find_matching_post_ids(terms, since=since, until=until)
    finally:
        close_pool()


def count_posts_with_hits(
    terms: Sequence[str],
    *,
    since: datetime | None = None,
    until: datetime | None = None,
) -> int:
    return len(find_matching_post_ids(terms, since=since, until=until))


def _post_chars(post: dict[str, Any]) -> int:
    text = post.get("text") or ""
    tsv = post.get("tsv_en") or ""
    return len(text) + len(tsv)


def _split_budget(
    posts: list[dict[str, Any]],
    budget: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split off a head that stays within ``budget`` chars. Head is never empty."""
    if not posts:
        return [], []
    head = [posts[0]]
    total = _post_chars(posts[0])
    idx = 1
    while idx < len(posts):
        nxt = _post_chars(posts[idx])
        if total + nxt > budget:
            break
        head.append(posts[idx])
        total += nxt
        idx += 1
    return head, posts[idx:]


def _hydrate_posts(
    cur_posts: Any,
    cur_hits: Any,
    *,
    terms: Sequence[str],
    post_ids: Sequence[int],
) -> list[dict[str, Any]]:
    """Load post bodies and term hits for ``post_ids``. Drops ids with no hits."""
    if not post_ids:
        return []
    cur_posts.execute(_sql_fetch_posts_for_ids(), (list(post_ids),))
    posts_by_id: dict[int, dict[str, Any]] = {}
    for row in cur_posts.fetchall():
        post = _row_to_post(tuple(row))
        posts_by_id[int(post["post_id"])] = post

    cur_hits.execute(_sql_fetch_hits_for_ids(), (list(terms), list(post_ids)))
    for row in cur_hits.fetchall():
        post_id, term_id, term_name, match_start, match_end = row
        post = posts_by_id.get(int(post_id))
        if post is None:
            continue
        post["hits"].append(
            {
                "term_id": term_id,
                "term_name": term_name,
                "match_start": match_start,
                "match_end": match_end,
            }
        )

    posts: list[dict[str, Any]] = []
    for post_id in post_ids:
        post = posts_by_id.get(int(post_id))
        if post and post["hits"]:
            posts.append(post)
    return posts


def iter_post_batches(
    terms: Sequence[str],
    *,
    use_prod: bool = False,
    row_fetch_size: int = DEFAULT_ROW_FETCH_SIZE,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
    post_ids: Sequence[int] | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Yield hydrated post slices that hit any of ``terms``.

    ``until`` is exclusive. ``limit`` caps yielded posts (``None`` = no cap).
    ``post_ids`` skips the id lookup when the caller already ran
    :func:`matching_post_ids` (e.g. to show a total).
    Database cursors are closed before each yield, so the caller can spend
    a long time on a slice (punctuation, claim extraction) without holding
    connections or the rest of the result set.
    """
    if not terms:
        return
    since = _ensure_utc(since)
    until = _ensure_utc(until)
    if since is not None and until is not None and since >= until:
        raise ValueError(f"since ({since.isoformat()}) must be before until ({until.isoformat()})")

    init_pool(prefix="prod" if use_prod else "dev")
    try:
        if post_ids is None:
            ids = find_matching_post_ids(terms, since=since, until=until)
        else:
            ids = sorted(int(i) for i in post_ids)
        if limit is not None:
            ids = ids[: max(0, int(limit))]

        step = max(1, int(row_fetch_size))
        prefix = "prod" if use_prod else "dev"
        for i in range(0, len(ids), step):
            slice_ids = ids[i : i + step]
            buffered = None
            for attempt in range(1, _HYDRATE_ATTEMPTS + 1):
                try:
                    with getcursor() as cur_posts, getcursor() as cur_hits:
                        buffered = _hydrate_posts(
                            cur_posts,
                            cur_hits,
                            terms=terms,
                            post_ids=slice_ids,
                        )
                    break
                except psycopg2.OperationalError as err:
                    if attempt == _HYDRATE_ATTEMPTS:
                        raise
                    wait_s = _HYDRATE_RETRY_S[min(attempt - 1, len(_HYDRATE_RETRY_S) - 1)]
                    print(
                        f"[fetch] database connection dropped ({err}); "
                        f"retrying in {wait_s}s ({attempt + 1}/{_HYDRATE_ATTEMPTS})",
                        flush=True,
                    )
                    time.sleep(wait_s)
                    try:
                        close_pool()
                    except Exception:
                        pass
                    init_pool(prefix=prefix)
            assert buffered is not None
            while buffered:
                head, buffered = _split_budget(buffered, max(1, int(_PAGE_CHAR_BUDGET)))
                yield head
    finally:
        close_pool()


def iter_posts_for_terms(
    terms: Sequence[str],
    *,
    use_prod: bool = False,
    row_fetch_size: int = DEFAULT_ROW_FETCH_SIZE,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield post dicts that hit any of ``terms``, optionally within [since, until).

    ``until`` is exclusive. Timestamps use COALESCE(created_at_ts, date_entered).
    ``limit`` caps yielded posts (None = no cap).

    Pages are fetched underneath. ``list(...)`` on this iterator still holds the
    full result; use :func:`iter_post_batches` or :func:`fetch_and_write` for
    large ranges.
    """
    for batch in iter_post_batches(
        terms,
        use_prod=use_prod,
        row_fetch_size=row_fetch_size,
        since=since,
        until=until,
        limit=limit,
    ):
        yield from batch


def build_posts_payload(
    posts: list[dict[str, Any]],
    *,
    terms: Sequence[str],
    since: datetime | None = None,
    until: datetime | None = None,
    matched_post_count: int | None = None,
) -> dict[str, Any]:
    """Build the ``{posts: [...], ...}`` envelope used by extract / claim pipelines."""
    since_u = _ensure_utc(since)
    until_u = _ensure_utc(until)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "terms": list(terms),
        "since": since_u.isoformat() if since_u else None,
        "until": until_u.isoformat() if until_u else None,
        "matched_post_count": matched_post_count if matched_post_count is not None else len(posts),
        "post_count": len(posts),
        "posts": posts,
    }


def write_posts_json(
    out_path: Path,
    posts: list[dict[str, Any]],
    *,
    terms: Sequence[str],
    since: datetime | None = None,
    until: datetime | None = None,
    matched_post_count: int | None = None,
) -> dict[str, Any]:
    payload = build_posts_payload(
        posts,
        terms=terms,
        since=since,
        until=until,
        matched_post_count=matched_post_count,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(out_path)
    return payload


class JsonPostsStreamWriter:
    """Write a ``{..., "posts": [...], "post_count": N}`` file one post at a time."""

    def __init__(self, final_path: Path, meta: dict[str, Any]) -> None:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        self.final_path = final_path
        self.tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
        self._f = self.tmp_path.open("w", encoding="utf-8")
        self._first_post = True
        self._closed = False
        self._finalized = False
        self.written_posts = 0

        self._f.write("{\n")
        for key, value in meta.items():
            if key == "posts":
                continue
            self._f.write(f"  {json.dumps(key)}: {json.dumps(value, ensure_ascii=False)},\n")
        self._f.write('  "posts": [\n')

    def write_post(self, post: dict[str, Any]) -> None:
        if not self._first_post:
            self._f.write(",\n")
        self._first_post = False
        blob = json.dumps(post, ensure_ascii=False, indent=2)
        self._f.write("\n".join("    " + line for line in blob.splitlines()))
        self.written_posts += 1
        self._f.flush()

    def finalize(self, extra: dict[str, Any] | None = None) -> None:
        if self._finalized:
            return
        tail: dict[str, Any] = {"post_count": self.written_posts}
        if extra:
            for key, value in extra.items():
                if key not in tail:
                    tail[key] = value
        self._f.write("\n  ]")
        for key, value in tail.items():
            self._f.write(",\n")
            self._f.write(f"  {json.dumps(key)}: {json.dumps(value, ensure_ascii=False)}")
        self._f.write("\n}\n")
        self._f.flush()
        self._close_file()
        self.tmp_path.replace(self.final_path)
        self._finalized = True

    def abort(self) -> None:
        if self._finalized:
            return
        self._close_file()
        self.tmp_path.unlink(missing_ok=True)

    def _close_file(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._f.close()


def fetch_and_write(
    *,
    terms: Sequence[str],
    out_path: Path,
    use_prod: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
    row_fetch_size: int = DEFAULT_ROW_FETCH_SIZE,
    count_first: bool = False,
) -> dict[str, Any]:
    """Fetch matching posts and write ``out_path``. Returns summary metadata.

    Posts are streamed to disk one database slice at a time.
    """
    terms_list = [t.strip() for t in terms if str(t).strip()]
    if not terms_list:
        raise ValueError("At least one search term is required")

    since_u = _ensure_utc(since)
    until_u = _ensure_utc(until)
    matched: int | None = None
    if count_first:
        init_pool(prefix="prod" if use_prod else "dev")
        try:
            matched = count_posts_with_hits(terms_list, since=since_u, until=until_u)
        finally:
            close_pool()

    writer = JsonPostsStreamWriter(
        out_path,
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "terms": terms_list,
            "since": since_u.isoformat() if since_u else None,
            "until": until_u.isoformat() if until_u else None,
        },
    )
    finalized = False
    try:
        total = 0
        page_n = 0
        for batch in iter_post_batches(
            terms_list,
            use_prod=use_prod,
            row_fetch_size=row_fetch_size,
            since=since_u,
            until=until_u,
            limit=limit,
        ):
            page_n += 1
            for post in batch:
                writer.write_post(post)
            total += len(batch)
            print(f"[fetch] page {page_n}: {len(batch)} posts ({total} total)", flush=True)
        matched_out = matched if matched is not None else writer.written_posts
        writer.finalize(extra={"matched_post_count": matched_out})
        finalized = True
    finally:
        if not finalized:
            writer.abort()

    return {
        "out": str(out_path),
        "post_count": writer.written_posts,
        "matched_post_count": matched if matched is not None else writer.written_posts,
        "terms": terms_list,
        "since": since_u.isoformat() if since_u else None,
        "until": until_u.isoformat() if until_u else None,
        "use_prod": use_prod,
    }


def _load_terms_file(path: Path) -> list[str]:
    terms: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if s and not s.startswith("#"):
            terms.append(s)
    return terms


def count_only(
    *,
    terms: Sequence[str],
    use_prod: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
) -> dict[str, Any]:
    """COUNT matching posts; no fetch/write. Opens and closes the DB pool."""
    terms_list = [t.strip() for t in terms if str(t).strip()]
    if not terms_list:
        raise ValueError("At least one search term is required")
    since = _ensure_utc(since)
    until = _ensure_utc(until)
    if since is not None and until is not None and since >= until:
        raise ValueError(f"since ({since.isoformat()}) must be before until ({until.isoformat()})")

    init_pool(prefix="prod" if use_prod else "dev")
    try:
        matched = count_posts_with_hits(terms_list, since=since, until=until)
    finally:
        close_pool()
    return {
        "matched_post_count": matched,
        "terms": terms_list,
        "since": since.isoformat() if since else None,
        "until": until.isoformat() if until else None,
        "use_prod": use_prod,
    }


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(description="Fetch posts for vaccine search terms")
    ap.add_argument("--terms", nargs="*", default=[], help="Search term names (taxonomy.vaccine_term.name)")
    ap.add_argument("--terms-file", type=Path, default=None, help="One term per line")
    ap.add_argument("--since", type=str, default=None, help="Inclusive lower bound (UTC ISO / YYYY-MM-DD)")
    ap.add_argument("--until", type=str, default=None, help="Exclusive upper bound (UTC ISO / YYYY-MM-DD)")
    ap.add_argument("--out", type=Path, default=None, help="Output JSON (required unless --count-only)")
    ap.add_argument("--prod", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--count-first", action="store_true", help="Run COUNT(*) before streaming")
    ap.add_argument(
        "--count-only",
        action="store_true",
        help="Print matched post count and exit (no fetch, no --out)",
    )
    ap.add_argument(
        "--upload",
        action="store_true",
        help="PUT --out to nitwitch WebDAV (NITWITCH_UPLOAD_URL/USER/PASSWORD in .env)",
    )
    ap.add_argument(
        "--upload-as",
        type=str,
        default=None,
        help="Remote filename for --upload (default: basename of --out)",
    )
    args = ap.parse_args(argv)

    terms = list(args.terms)
    if args.terms_file is not None:
        terms.extend(_load_terms_file(args.terms_file))
    # dedupe preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for t in terms:
        if t not in seen:
            seen.add(t)
            uniq.append(t)

    if args.count_only:
        if args.upload:
            ap.error("--upload is incompatible with --count-only")
        summary = count_only(
            terms=uniq,
            use_prod=bool(args.prod),
            since=parse_utc_datetime(args.since),
            until=parse_utc_datetime(args.until),
        )
        print(json.dumps(summary, ensure_ascii=False), flush=True)
        return 0

    if args.out is None:
        ap.error("--out is required unless --count-only")

    summary = fetch_and_write(
        terms=uniq,
        out_path=args.out,
        use_prod=bool(args.prod),
        since=parse_utc_datetime(args.since),
        until=parse_utc_datetime(args.until),
        limit=args.limit,
        count_first=bool(args.count_first),
    )
    if args.upload:
        from storage.nitwitch_upload import upload_file

        summary["uploaded_url"] = upload_file(
            Path(args.out),
            remote_name=args.upload_as,
        )
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
