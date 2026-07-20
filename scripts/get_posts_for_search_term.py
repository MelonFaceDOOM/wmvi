"""Fetch posts that matched one or more vaccine search terms.

Already supports a list of terms. Date range filters on
``COALESCE(created_at_ts, date_entered)`` (post time, falling back to ingest time).

For fetch + punct/trim + claim extract + upload in one command, use
``python -m scripts.get_posts_extract_upload``.

To dump taxonomy term names for ``--terms-file``::

    python -m scripts.list_vaccine_terms --prod --out my_terms.txt
    python -m scripts.list_vaccine_terms --prod --subset core_search_terms

Library::

    from scripts.get_posts_for_search_term import iter_posts_for_terms, write_posts_json

    for post in iter_posts_for_terms(["measles", "mmr"], since=..., until=...):
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

from dotenv import load_dotenv

from db.db import close_pool, getcursor, init_pool


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


def _sql_fetch_post_id_page() -> str:
    date_sql, _ = _date_filter_sql("p")
    return f"""
        WITH term_ids AS (
            SELECT id
            FROM taxonomy.vaccine_term
            WHERE name = ANY(%s)
        )
        SELECT DISTINCT ph.post_id
        FROM matches.post_term_hit ph
        JOIN term_ids t
          ON t.id = ph.term_id
        JOIN sm.posts_all p
          ON p.post_id = ph.post_id
        WHERE ph.post_id > %s
        {date_sql}
        ORDER BY ph.post_id
        LIMIT %s
    """


def _sql_fetch_posts_for_ids() -> str:
    return """
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
            rs_meta.title AS reddit_submission_title,
            rc_sub.title AS reddit_comment_submission_title,
            tp_meta.channel_id::text AS telegram_channel,
            yv_meta.title AS youtube_video_title,
            ps_meta.title AS podcast_name
        FROM sm.posts_all p
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
         AND rc_sub.id = regexp_replace(rc_meta.link_id, '^t3_', '')
        LEFT JOIN sm.telegram_post tp_meta
          ON p.platform = 'telegram_post'
         AND p.key1 = tp_meta.channel_id::text
         AND p.key2 = tp_meta.message_id::text
        LEFT JOIN youtube.video yv_meta
          ON p.platform = 'youtube_video'
         AND p.key1 = yv_meta.video_id
         AND p.key2 = ''
        LEFT JOIN podcasts.episodes pe_meta
          ON p.platform = 'podcast_episode'
         AND p.key1 = pe_meta.id
         AND p.key2 = ''
        LEFT JOIN podcasts.shows ps_meta
          ON p.platform = 'podcast_episode'
         AND pe_meta.podcast_id = ps_meta.id
        WHERE p.post_id = ANY(%s)
        ORDER BY p.post_id
    """


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


def count_posts_with_hits(
    terms: Sequence[str],
    *,
    since: datetime | None = None,
    until: datetime | None = None,
) -> int:
    if not terms:
        return 0
    date_sql, _ = _date_filter_sql("p")
    sql = f"""
        WITH term_ids AS (
            SELECT id
            FROM taxonomy.vaccine_term
            WHERE name = ANY(%s)
        )
        SELECT count(DISTINCT ph.post_id)
        FROM matches.post_term_hit ph
        JOIN sm.posts_all p
          ON p.post_id = ph.post_id
        WHERE ph.term_id IN (SELECT id FROM term_ids)
        {date_sql}
    """
    with getcursor() as cur:
        cur.execute(sql, (list(terms), *build_date_params(since, until)))
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def iter_posts_for_terms(
    terms: Sequence[str],
    *,
    use_prod: bool = False,
    row_fetch_size: int = 2000,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield post dicts that hit any of ``terms``, optionally within [since, until).

    ``until`` is exclusive. Timestamps use COALESCE(created_at_ts, date_entered).
    ``limit`` caps yielded posts (None = no cap).
    """
    if not terms:
        return
    since = _ensure_utc(since)
    until = _ensure_utc(until)
    if since is not None and until is not None and since >= until:
        raise ValueError(f"since ({since.isoformat()}) must be before until ({until.isoformat()})")

    init_pool(prefix="prod" if use_prod else "dev")
    yielded = 0
    try:
        sql_post_id_page = _sql_fetch_post_id_page()
        sql_posts_for_ids = _sql_fetch_posts_for_ids()
        sql_hits_for_ids = _sql_fetch_hits_for_ids()
        last_post_id = 0
        date_params = build_date_params(since, until)

        with getcursor() as cur_ids, getcursor() as cur_posts, getcursor() as cur_hits:
            while True:
                page_limit = max(1, int(row_fetch_size))
                if limit is not None:
                    remaining = int(limit) - yielded
                    if remaining <= 0:
                        break
                    page_limit = min(page_limit, remaining)

                cur_ids.execute(
                    sql_post_id_page,
                    (list(terms), last_post_id, *date_params, page_limit),
                )
                id_rows = cur_ids.fetchall()
                if not id_rows:
                    break
                post_ids = [int(r[0]) for r in id_rows if r and r[0] is not None]
                if not post_ids:
                    break
                last_post_id = post_ids[-1]

                cur_posts.execute(sql_posts_for_ids, (post_ids,))
                posts_by_id: dict[int, dict[str, Any]] = {}
                for row in cur_posts.fetchall():
                    (
                        post_id,
                        platform,
                        key1,
                        key2,
                        date_entered,
                        created_at_ts,
                        text,
                        tsv_en,
                        is_en,
                        primary_metric,
                        url,
                        reddit_submission_title,
                        reddit_comment_submission_title,
                        telegram_channel,
                        youtube_video_title,
                        podcast_name,
                    ) = row
                    posts_by_id[int(post_id)] = {
                        "post_id": post_id,
                        "platform": platform,
                        "key1": key1,
                        "key2": key2,
                        "date_entered": _json_val(_ensure_utc(date_entered)),
                        "created_at_ts": _json_val(_ensure_utc(created_at_ts)),
                        "text": text,
                        "tsv_en": str(tsv_en) if tsv_en is not None else None,
                        "is_en": is_en,
                        "primary_metric": primary_metric,
                        "url": url,
                        "reddit_submission_title": reddit_submission_title,
                        "reddit_comment_submission_title": reddit_comment_submission_title,
                        "telegram_channel": telegram_channel,
                        "youtube_video_title": youtube_video_title,
                        "podcast_name": podcast_name,
                        "hits": [],
                    }

                cur_hits.execute(sql_hits_for_ids, (list(terms), post_ids))
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

                for post_id in post_ids:
                    post = posts_by_id.get(int(post_id))
                    if post and post["hits"]:
                        yield post
                        yielded += 1
                        if limit is not None and yielded >= int(limit):
                            return
    finally:
        close_pool()


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


def fetch_and_write(
    *,
    terms: Sequence[str],
    out_path: Path,
    use_prod: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
    row_fetch_size: int = 2000,
    count_first: bool = False,
) -> dict[str, Any]:
    """Fetch matching posts and write ``out_path``. Returns summary metadata."""
    terms_list = [t.strip() for t in terms if str(t).strip()]
    if not terms_list:
        raise ValueError("At least one search term is required")

    matched: int | None = None
    if count_first:
        init_pool(prefix="prod" if use_prod else "dev")
        try:
            matched = count_posts_with_hits(terms_list, since=since, until=until)
        finally:
            close_pool()

    posts = list(
        iter_posts_for_terms(
            terms_list,
            use_prod=use_prod,
            row_fetch_size=row_fetch_size,
            since=since,
            until=until,
            limit=limit,
        )
    )
    write_posts_json(
        out_path,
        posts,
        terms=terms_list,
        since=since,
        until=until,
        matched_post_count=matched if matched is not None else len(posts),
    )
    return {
        "out": str(out_path),
        "post_count": len(posts),
        "matched_post_count": matched if matched is not None else len(posts),
        "terms": terms_list,
        "since": since.isoformat() if since else None,
        "until": until.isoformat() if until else None,
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
