from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

from db.db import close_pool, getcursor, init_pool


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _json_val(v: Any) -> Any:
    if isinstance(v, datetime):
        dt = _ensure_utc(v)
        return dt.isoformat() if dt is not None else None
    return v


def _sql_fetch_post_id_page() -> str:
    return """
        WITH term_ids AS (
            SELECT id
            FROM taxonomy.vaccine_term
            WHERE name = ANY(%s)
        )
        SELECT DISTINCT ph.post_id
        FROM matches.post_term_hit ph
        JOIN term_ids t
          ON t.id = ph.term_id
        WHERE ph.post_id > %s
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


def count_posts_with_hits(terms: Sequence[str]) -> int:
    if not terms:
        return 0
    sql = """
        WITH term_ids AS (
            SELECT id
            FROM taxonomy.vaccine_term
            WHERE name = ANY(%s)
        )
        SELECT count(DISTINCT ph.post_id)
        FROM matches.post_term_hit ph
        WHERE ph.term_id IN (SELECT id FROM term_ids)
    """
    with getcursor() as cur:
        cur.execute(sql, (list(terms),))
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def iter_posts_for_terms(
    terms: Sequence[str],
    *,
    use_prod: bool = False,
    row_fetch_size: int = 2000,
) -> Iterator[dict[str, Any]]:
    if not terms:
        return

    init_pool(prefix="prod" if use_prod else "dev")
    try:
        sql_post_id_page = _sql_fetch_post_id_page()
        sql_posts_for_ids = _sql_fetch_posts_for_ids()
        sql_hits_for_ids = _sql_fetch_hits_for_ids()
        last_post_id = 0

        with getcursor() as cur_ids, getcursor() as cur_posts, getcursor() as cur_hits:
            while True:
                cur_ids.execute(sql_post_id_page, (list(terms), last_post_id, max(1, int(row_fetch_size))))
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
    finally:
        close_pool()
