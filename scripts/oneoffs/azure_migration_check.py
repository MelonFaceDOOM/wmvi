from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from psycopg2.extras import RealDictCursor

from db.db import init_pool, close_pool, getcursor


SAMPLE_LIMIT = 3
STALE_HOURS = 24


@dataclass(frozen=True)
class SourceTable:
    table: str
    label: str
    platform: str
    key1_sql: str
    key2_sql: str
    source_join_null_sql: str
    created_col: str
    date_entered_col: str
    compare_select: tuple[tuple[str, str], ...]


SOURCE_TABLES = [
    SourceTable(
        table="sm.reddit_submission",
        label="reddit_submission",
        platform="reddit_submission",
        key1_sql="t.id",
        key2_sql="''",
        source_join_null_sql="t.id IS NULL",
        created_col="t.created_at_ts",
        date_entered_col="t.date_entered",
        compare_select=(
            ("id", "t.id"),
            ("created_at_ts", "t.created_at_ts"),
            ("date_entered", "t.date_entered"),
            ("url_hash", "t.url_hash"),
            ("title_md5", "md5(coalesce(t.title, ''))"),
            ("title_len", "length(coalesce(t.title, ''))"),
            ("filtered_text_md5", "md5(coalesce(t.filtered_text, ''))"),
            ("filtered_text_len", "length(coalesce(t.filtered_text, ''))"),
            ("selftext_md5", "md5(coalesce(t.selftext, ''))"),
            ("selftext_len", "length(coalesce(t.selftext, ''))"),
            ("score", "t.score"),
            ("num_comments", "t.num_comments"),
            ("is_en", "t.is_en"),
        ),
    ),
    SourceTable(
        table="sm.reddit_comment",
        label="reddit_comment",
        platform="reddit_comment",
        key1_sql="t.id",
        key2_sql="''",
        source_join_null_sql="t.id IS NULL",
        created_col="t.created_at_ts",
        date_entered_col="t.date_entered",
        compare_select=(
            ("id", "t.id"),
            ("created_at_ts", "t.created_at_ts"),
            ("date_entered", "t.date_entered"),
            ("body_md5", "md5(coalesce(t.body, ''))"),
            ("body_len", "length(coalesce(t.body, ''))"),
            ("filtered_text_md5", "md5(coalesce(t.filtered_text, ''))"),
            ("filtered_text_len", "length(coalesce(t.filtered_text, ''))"),
            ("score", "t.score"),
            ("parent_comment_id", "t.parent_comment_id"),
            ("is_en", "t.is_en"),
        ),
    ),
    SourceTable(
        table="sm.telegram_post",
        label="telegram_post",
        platform="telegram_post",
        key1_sql="t.channel_id::text",
        key2_sql="t.message_id::text",
        source_join_null_sql="t.channel_id IS NULL",
        created_col="t.created_at_ts",
        date_entered_col="t.date_entered",
        compare_select=(
            ("channel_id", "t.channel_id"),
            ("message_id", "t.message_id"),
            ("created_at_ts", "t.created_at_ts"),
            ("date_entered", "t.date_entered"),
            ("link_hash", "t.link_hash"),
            ("text_md5", "md5(coalesce(t.text, ''))"),
            ("text_len", "length(coalesce(t.text, ''))"),
            ("filtered_text_md5", "md5(coalesce(t.filtered_text, ''))"),
            ("filtered_text_len", "length(coalesce(t.filtered_text, ''))"),
            ("views", "t.views"),
            ("is_en", "t.is_en"),
        ),
    ),
    SourceTable(
        table="sm.tweet",
        label="tweet",
        platform="tweet",
        key1_sql="t.id::text",
        key2_sql="''",
        source_join_null_sql="t.id IS NULL",
        created_col="t.created_at_ts",
        date_entered_col="t.date_entered",
        compare_select=(
            ("id", "t.id"),
            ("created_at_ts", "t.created_at_ts"),
            ("date_entered", "t.date_entered"),
            ("tweet_text_md5", "md5(coalesce(t.tweet_text, ''))"),
            ("tweet_text_len", "length(coalesce(t.tweet_text, ''))"),
            ("filtered_text_md5", "md5(coalesce(t.filtered_text, ''))"),
            ("filtered_text_len", "length(coalesce(t.filtered_text, ''))"),
            ("like_count", "t.like_count"),
            ("reply_count", "t.reply_count"),
            ("is_en", "t.is_en"),
        ),
    ),
    SourceTable(
        table="youtube.video",
        label="youtube_video",
        platform="youtube_video",
        key1_sql="t.video_id",
        key2_sql="''",
        source_join_null_sql="t.video_id IS NULL",
        created_col="t.created_at_ts",
        date_entered_col="t.date_entered",
        compare_select=(
            ("video_id", "t.video_id"),
            ("created_at_ts", "t.created_at_ts"),
            ("date_entered", "t.date_entered"),
            ("url_hash", "t.url_hash"),
            ("title_md5", "md5(coalesce(t.title, ''))"),
            ("title_len", "length(coalesce(t.title, ''))"),
            ("description_md5", "md5(coalesce(t.description, ''))"),
            ("description_len", "length(coalesce(t.description, ''))"),
            ("duration_seconds", "t.duration_seconds"),
            ("view_count", "t.view_count"),
            ("transcript_md5", "md5(coalesce(t.transcript, ''))"),
            ("transcript_len", "length(coalesce(t.transcript, ''))"),
            ("transcript_updated_at", "t.transcript_updated_at"),
            ("is_en", "t.is_en"),
        ),
    ),
    SourceTable(
        table="youtube.comment",
        label="youtube_comment",
        platform="youtube_comment",
        key1_sql="t.video_id",
        key2_sql="t.comment_id",
        source_join_null_sql="t.video_id IS NULL",
        created_col="t.created_at_ts",
        date_entered_col="t.date_entered",
        compare_select=(
            ("video_id", "t.video_id"),
            ("comment_id", "t.comment_id"),
            ("created_at_ts", "t.created_at_ts"),
            ("date_entered", "t.date_entered"),
            ("comment_url_hash", "t.comment_url_hash"),
            ("text_md5", "md5(coalesce(t.text, ''))"),
            ("text_len", "length(coalesce(t.text, ''))"),
            ("filtered_text_md5", "md5(coalesce(t.filtered_text, ''))"),
            ("filtered_text_len", "length(coalesce(t.filtered_text, ''))"),
            ("like_count", "t.like_count"),
            ("parent_comment_id", "t.parent_comment_id"),
            ("reply_count", "t.reply_count"),
            ("is_en", "t.is_en"),
        ),
    ),
    SourceTable(
        table="podcasts.episodes",
        label="podcast_episode",
        platform="podcast_episode",
        key1_sql="t.id",
        key2_sql="''",
        source_join_null_sql="t.id IS NULL",
        created_col="t.created_at_ts",
        date_entered_col="t.date_entered",
        compare_select=(
            ("id", "t.id"),
            ("created_at_ts", "t.created_at_ts"),
            ("date_entered", "t.date_entered"),
            ("podcast_id", "t.podcast_id"),
            ("title_md5", "md5(coalesce(t.title, ''))"),
            ("title_len", "length(coalesce(t.title, ''))"),
            ("description_md5", "md5(coalesce(t.description, ''))"),
            ("description_len", "length(coalesce(t.description, ''))"),
            ("download_url_md5", "md5(coalesce(t.download_url, ''))"),
            ("transcript_md5", "md5(coalesce(t.transcript, ''))"),
            ("transcript_len", "length(coalesce(t.transcript, ''))"),
            ("transcript_updated_at", "t.transcript_updated_at"),
            ("is_en", "t.is_en"),
        ),
    ),
    SourceTable(
        table="news.article",
        label="news_article",
        platform="news_article",
        key1_sql="t.id::text",
        key2_sql="''",
        source_join_null_sql="t.id IS NULL",
        created_col="t.created_at_ts",
        date_entered_col="t.date_entered",
        compare_select=(
            ("id", "t.id"),
            ("created_at_ts", "t.created_at_ts"),
            ("date_entered", "t.date_entered"),
            ("url_hash", "t.url_hash"),
            ("title_md5", "md5(coalesce(t.title, ''))"),
            ("title_len", "length(coalesce(t.title, ''))"),
            ("text_md5", "md5(coalesce(t.text, ''))"),
            ("text_len", "length(coalesce(t.text, ''))"),
            ("is_en", "t.is_en"),
        ),
    ),
]


def one(sql: str, params: tuple = ()) -> Any:
    with getcursor(commit=False) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return None if row is None else row[0]


def row(sql: str, params: tuple = ()) -> dict[str, Any]:
    with getcursor(commit=False, cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return dict(cur.fetchone())


def rows(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with getcursor(commit=False, cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def fmt(v: Any) -> str:
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def compare_select_sql(meta: SourceTable) -> str:
    return ", ".join(f"{expr} AS {alias}" for alias, expr in meta.compare_select)


def source_summary(meta: SourceTable) -> dict[str, Any]:
    sql = f"""
    SELECT
        count(*)::bigint AS total_rows,
        count(*) FILTER (WHERE t.is_en IS TRUE)::bigint AS is_en_true,
        count(*) FILTER (WHERE t.is_en IS FALSE)::bigint AS is_en_false,
        count(*) FILTER (WHERE t.is_en IS NULL)::bigint AS is_en_null,
        count(*) FILTER (WHERE {meta.created_col} IS NULL)::bigint AS null_created_at,
        count(pr.id)::bigint AS matched_registry_rows,
        count(*) FILTER (WHERE pr.id IS NULL)::bigint AS source_missing_registry,
        min({meta.created_col}) AS min_created_at,
        max({meta.created_col}) AS max_created_at,
        min({meta.date_entered_col}) AS min_date_entered,
        max({meta.date_entered_col}) AS max_date_entered
    FROM {meta.table} t
    LEFT JOIN sm.post_registry pr
      ON pr.platform = %s
     AND pr.key1 = {meta.key1_sql}
     AND pr.key2 = {meta.key2_sql}
    """
    out = row(sql, (meta.platform,))
    out["platform_registry_rows"] = one(
        "SELECT count(*)::bigint FROM sm.post_registry WHERE platform = %s",
        (meta.platform,),
    )

    orphan_sql = f"""
    SELECT count(*)::bigint
    FROM sm.post_registry pr
    LEFT JOIN {meta.table} t
      ON pr.key1 = {meta.key1_sql}
     AND pr.key2 = {meta.key2_sql}
    WHERE pr.platform = %s
      AND {meta.source_join_null_sql}
    """
    out["registry_orphans"] = one(orphan_sql, (meta.platform,))
    return out


def transcript_summary(
    *,
    parent_table: str,
    parent_key: str,
    transcript_col: str,
    started_col: str,
    updated_col: str,
    segments_table: str,
    segments_fk: str,
) -> dict[str, Any]:
    parent_sql = f"""
    SELECT
        count(*)::bigint AS parent_rows,
        count(*) FILTER (WHERE {transcript_col} IS NOT NULL AND {transcript_col} <> '')::bigint AS with_transcript,
        count(*) FILTER (WHERE ({transcript_col} IS NULL OR {transcript_col} = '') AND {started_col} IS NOT NULL)::bigint AS started_but_no_transcript,
        count(*) FILTER (WHERE {updated_col} IS NOT NULL AND ({transcript_col} IS NULL OR {transcript_col} = ''))::bigint AS updated_but_no_transcript,
        count(*) FILTER (WHERE ({transcript_col} IS NOT NULL AND {transcript_col} <> '') AND {updated_col} IS NULL)::bigint AS transcript_but_no_updated_at,
        count(*) FILTER (
            WHERE {started_col} IS NOT NULL
              AND ({transcript_col} IS NULL OR {transcript_col} = '')
              AND {started_col} < now() - interval '{STALE_HOURS} hours'
        )::bigint AS stale_started_no_transcript,
        count(*) FILTER (
            WHERE ({transcript_col} IS NOT NULL AND {transcript_col} <> '')
              AND NOT EXISTS (
                  SELECT 1
                  FROM {segments_table} s
                  WHERE s.{segments_fk} = p.{parent_key}
              )
        )::bigint AS transcript_without_segments
    FROM {parent_table} p
    """
    seg_sql = f"""
    SELECT
        count(*)::bigint AS segment_rows,
        count(DISTINCT {segments_fk})::bigint AS segment_parents
    FROM {segments_table}
    """
    out = row(parent_sql)
    out.update(row(seg_sql))
    return out


def sample_rows(meta: SourceTable, direction: str) -> list[dict[str, Any]]:
    assert direction in ("ASC", "DESC")
    sql = f"""
    SELECT {compare_select_sql(meta)}
    FROM {meta.table} t
    ORDER BY {meta.created_col} {direction} NULLS LAST
    LIMIT {SAMPLE_LIMIT}
    """
    return rows(sql)


def print_header(title: str) -> None:
    print()
    print("=" * 110)
    print(title)
    print("=" * 110)


def print_source_checks() -> list[str]:
    issues: list[str] = []

    print_header("SOURCE TABLES / REGISTRY COVERAGE")

    for meta in SOURCE_TABLES:
        s = source_summary(meta)
        print(f"\n[{meta.label}]")
        for field in [
            "total_rows",
            "platform_registry_rows",
            "matched_registry_rows",
            "source_missing_registry",
            "registry_orphans",
            "is_en_true",
            "is_en_false",
            "is_en_null",
            "null_created_at",
        ]:
            print(f"  {field:24} {fmt(s[field]):>14}")

        print(f"  {'min_created_at':24} {s['min_created_at']}")
        print(f"  {'max_created_at':24} {s['max_created_at']}")
        print(f"  {'min_date_entered':24} {s['min_date_entered']}")
        print(f"  {'max_date_entered':24} {s['max_date_entered']}")

        if s["source_missing_registry"] > 0:
            issues.append(f"{meta.label}: {s['source_missing_registry']} source rows missing post_registry entry")
        if s["registry_orphans"] > 0:
            issues.append(f"{meta.label}: {s['registry_orphans']} post_registry rows are orphaned")
        if s["matched_registry_rows"] != s["total_rows"]:
            issues.append(f"{meta.label}: matched_registry_rows != total_rows")
        if s["platform_registry_rows"] != s["total_rows"]:
            issues.append(f"{meta.label}: platform_registry_rows != total_rows")

    return issues


def print_posts_all_checks() -> list[str]:
    issues: list[str] = []

    print_header("POSTS_ALL VIEW SANITY")

    registry_rows = one("SELECT count(*)::bigint FROM sm.post_registry")
    posts_all_rows = one("SELECT count(*)::bigint FROM sm.posts_all")
    posts_all_en_rows = one("SELECT count(*)::bigint FROM sm.post_search_en")

    print(f"{'sm.post_registry rows':30} {fmt(registry_rows):>14}")
    print(f"{'sm.posts_all rows':30} {fmt(posts_all_rows):>14}")
    print(f"{'sm.post_search_en rows':30} {fmt(posts_all_en_rows):>14}")

    if posts_all_rows != registry_rows:
        issues.append(f"sm.posts_all row count != sm.post_registry row count ({posts_all_rows} vs {registry_rows})")

    return issues


def print_transcript_checks() -> list[str]:
    issues: list[str] = []

    print_header("TRANSCRIPT / SEGMENT COMPLETION")

    configs = {
        "youtube_video": dict(
            parent_table="youtube.video",
            parent_key="video_id",
            transcript_col="transcript",
            started_col="transcription_started_at",
            updated_col="transcript_updated_at",
            segments_table="youtube.transcript_segments",
            segments_fk="video_id",
        ),
        "podcast_episode": dict(
            parent_table="podcasts.episodes",
            parent_key="id",
            transcript_col="transcript",
            started_col="transcription_started_at",
            updated_col="transcript_updated_at",
            segments_table="podcasts.transcript_segments",
            segments_fk="episode_id",
        ),
    }

    for label, cfg in configs.items():
        s = transcript_summary(**cfg)
        print(f"\n[{label}]")
        for field in [
            "parent_rows",
            "with_transcript",
            "started_but_no_transcript",
            "updated_but_no_transcript",
            "transcript_but_no_updated_at",
            "stale_started_no_transcript",
            "transcript_without_segments",
            "segment_rows",
            "segment_parents",
        ]:
            print(f"  {field:28} {fmt(s[field]):>14}")

        if s["updated_but_no_transcript"] > 0:
            issues.append(f"{label}: rows with transcript_updated_at but no transcript")
        if s["transcript_but_no_updated_at"] > 0:
            issues.append(f"{label}: rows with transcript but no transcript_updated_at")
        if s["stale_started_no_transcript"] > 0:
            issues.append(f"{label}: stale rows started > {STALE_HOURS}h ago but still no transcript")
        if s["transcript_without_segments"] > 0:
            issues.append(f"{label}: rows with transcript but no transcript segments")

    return issues


def print_match_checks() -> list[str]:
    issues: list[str] = []

    print_header("MATCH / SCRAPE LINK TABLES")

    post_term_hit = one("SELECT count(*)::bigint FROM matches.post_term_hit")
    matched_posts = one("SELECT count(DISTINCT post_id)::bigint FROM matches.post_term_hit")
    post_scrape = one("SELECT count(*)::bigint FROM scrape.post_scrape")
    scrape_jobs = one("SELECT count(*)::bigint FROM scrape.job")

    print(f"{'matches.post_term_hit':30} {fmt(post_term_hit):>14}")
    print(f"{'distinct matched posts':30} {fmt(matched_posts):>14}")
    print(f"{'scrape.post_scrape':30} {fmt(post_scrape):>14}")
    print(f"{'scrape.job':30} {fmt(scrape_jobs):>14}")

    return issues


def print_samples() -> None:
    print_header("OLDEST / NEWEST SAMPLE ROWS")

    for meta in SOURCE_TABLES:
        print(f"\n[{meta.label}]")

        oldest = sample_rows(meta, "ASC")
        newest = sample_rows(meta, "DESC")

        print("  oldest:")
        for r in oldest:
            print(f"    {r}")

        print("  newest:")
        for r in newest:
            print(f"    {r}")


def print_top_transcript_lengths() -> None:
    print_header("LONGEST TRANSCRIPTS / TEXTS (manual sanity spot-check)")

    queries = {
        "youtube.video": """
            SELECT video_id, created_at_ts, length(coalesce(transcript, '')) AS transcript_len, left(title, 100) AS title
            FROM youtube.video
            ORDER BY length(coalesce(transcript, '')) DESC NULLS LAST
            LIMIT 5
        """,
        "podcasts.episodes": """
            SELECT id, created_at_ts, length(coalesce(transcript, '')) AS transcript_len, left(title, 100) AS title
            FROM podcasts.episodes
            ORDER BY length(coalesce(transcript, '')) DESC NULLS LAST
            LIMIT 5
        """,
        "news.article": """
            SELECT id, created_at_ts, length(coalesce(text, '')) AS text_len, left(title, 100) AS title
            FROM news.article
            ORDER BY length(coalesce(text, '')) DESC NULLS LAST
            LIMIT 5
        """,
    }

    for label, sql in queries.items():
        print(f"\n[{label}]")
        for r in rows(sql):
            print(f"  {r}")


def main() -> None:
    init_pool(prefix="prod")

    try:
        issues: list[str] = []
        issues.extend(print_source_checks())
        issues.extend(print_posts_all_checks())
        issues.extend(print_transcript_checks())
        issues.extend(print_match_checks())
        print_samples()
        print_top_transcript_lengths()

        print_header("RESULT")
        if issues:
            print(f"Found {len(issues)} issue(s):")
            for issue in issues:
                print(f"  - {issue}")
            raise SystemExit(1)
        else:
            print("No internal data-completion issues found in the checked tables.")
            raise SystemExit(0)
    finally:
        close_pool()


if __name__ == "__main__":
    main()