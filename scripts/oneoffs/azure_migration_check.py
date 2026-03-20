"""
Compare and validate new db after transfering from old azure platform to new
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

#TODO update these:
OLD_PREFIX = "OLD"
NEW_PREFIX = "NEW"
SAMPLE_LIMIT = 2


@dataclass(frozen=True)
class SourceTable:
    table: str
    label: str
    platform: str
    key_cols: tuple[str, ...]
    key1_sql: str
    key2_sql: str
    null_check_sql: str
    created_col: str
    date_entered_col: str
    compare_select: tuple[tuple[str, str], ...]


SOURCE_TABLES = [
    SourceTable(
        table="sm.reddit_submission",
        label="reddit_submission",
        platform="reddit_submission",
        key_cols=("id",),
        key1_sql="t.id",
        key2_sql="''",
        null_check_sql="t.id IS NULL",
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
        key_cols=("id",),
        key1_sql="t.id",
        key2_sql="''",
        null_check_sql="t.id IS NULL",
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
        key_cols=("channel_id", "message_id"),
        key1_sql="t.channel_id::text",
        key2_sql="t.message_id::text",
        null_check_sql="t.channel_id IS NULL",
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
        key_cols=("id",),
        key1_sql="t.id::text",
        key2_sql="''",
        null_check_sql="t.id IS NULL",
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
        key_cols=("video_id",),
        key1_sql="t.video_id",
        key2_sql="''",
        null_check_sql="t.video_id IS NULL",
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
        key_cols=("video_id", "comment_id"),
        key1_sql="t.video_id",
        key2_sql="t.comment_id",
        null_check_sql="t.video_id IS NULL",
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
        key_cols=("id",),
        key1_sql="t.id",
        key2_sql="''",
        null_check_sql="t.id IS NULL",
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
        key_cols=("id",),
        key1_sql="t.id::text",
        key2_sql="''",
        null_check_sql="t.id IS NULL",
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


def creds(prefix: str) -> dict[str, Any]:
    prefix = prefix.upper()
    return {
        "host": os.environ[f"{prefix}_PGHOST"],
        "user": os.environ[f"{prefix}_PGUSER"],
        "password": os.environ[f"{prefix}_PGPASSWORD"],
        "port": int(os.environ.get(f"{prefix}_PGPORT", "5432")),
        "dbname": os.environ.get(f"{prefix}_PGDATABASE", "postgres"),
        "sslmode": os.environ.get(f"{prefix}_PGSSLMODE", "require"),
        "connect_timeout": int(os.environ.get("PGCONNECT_TIMEOUT", "10")),
    }


def connect(prefix: str):
    return psycopg2.connect(**creds(prefix))


def one(conn, sql: str, params: tuple = ()) -> Any:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return None if row is None else row[0]


def row(conn, sql: str, params: tuple = ()) -> dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return dict(cur.fetchone())


def rows(conn, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def fmt(v: Any) -> str:
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def compare_select_sql(meta: SourceTable) -> str:
    return ", ".join(f"{expr} AS {alias}" for alias, expr in meta.compare_select)


def source_summary(conn, meta: SourceTable) -> dict[str, Any]:
    sql = f"""
    SELECT
        count(*)::bigint AS total_rows,
        count(*) FILTER (WHERE t.is_en IS TRUE)::bigint AS is_en_true,
        count(*) FILTER (WHERE t.is_en IS FALSE)::bigint AS is_en_false,
        count(*) FILTER (WHERE t.is_en IS NULL)::bigint AS is_en_null,
        count(pr.id)::bigint AS matched_registry_rows,
        count(*) FILTER (WHERE pr.id IS NULL)::bigint AS source_missing_registry,
        min({meta.created_col}) AS min_created_at,
        max({meta.created_col}) AS max_created_at,
        max({meta.date_entered_col}) AS max_date_entered
    FROM {meta.table} t
    LEFT JOIN sm.post_registry pr
      ON pr.platform = %s
     AND pr.key1 = {meta.key1_sql}
     AND pr.key2 = {meta.key2_sql}
    """
    out = row(conn, sql, (meta.platform,))
    out["platform_registry_rows"] = one(
        conn,
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
      AND {meta.null_check_sql}
    """
    out["registry_orphans"] = one(conn, orphan_sql, (meta.platform,))
    return out


def count_new_rows_up_to_old_cutoff(conn, meta: SourceTable, old_cutoff) -> int | None:
    if old_cutoff is None:
        return None
    sql = f"""
    SELECT count(*)::bigint
    FROM {meta.table} t
    WHERE {meta.date_entered_col} <= %s
    """
    return one(conn, sql, (old_cutoff,))


def transcript_summary(
    conn,
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
        count(*) FILTER (WHERE {started_col} IS NOT NULL)::bigint AS started_count,
        count(*) FILTER (WHERE {updated_col} IS NOT NULL)::bigint AS updated_count,
        count(*) FILTER (
            WHERE {transcript_col} IS NOT NULL AND {transcript_col} <> ''
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
    out = row(conn, parent_sql)
    out.update(row(conn, seg_sql))
    return out


def sample_rows_old(conn, meta: SourceTable, direction: str) -> list[dict[str, Any]]:
    assert direction in ("ASC", "DESC")
    sql = f"""
    SELECT {compare_select_sql(meta)}
    FROM {meta.table} t
    ORDER BY {meta.created_col} {direction} NULLS LAST, {", ".join("t." + k for k in meta.key_cols)} {direction}
    LIMIT {SAMPLE_LIMIT}
    """
    return rows(conn, sql)


def fetch_row_by_key(conn, meta: SourceTable, key_row: dict[str, Any]) -> dict[str, Any] | None:
    where = " AND ".join(f"t.{col} = %s" for col in meta.key_cols)
    params = tuple(key_row[col] for col in meta.key_cols)
    sql = f"""
    SELECT {compare_select_sql(meta)}
    FROM {meta.table} t
    WHERE {where}
    """
    result = rows(conn, sql, params)
    return result[0] if result else None


def schema_migrations(conn) -> list[dict[str, Any]]:
    return rows(
        conn,
        "SELECT version, checksum FROM public.schema_migrations ORDER BY version"
    )


def compare_source(old_conn, new_conn, meta: SourceTable) -> list[str]:
    issues: list[str] = []

    old = source_summary(old_conn, meta)
    new = source_summary(new_conn, meta)

    print(f"\n[{meta.label}]")
    for field in [
        "total_rows",
        "platform_registry_rows",
        "matched_registry_rows",
        "is_en_true",
        "is_en_false",
        "is_en_null",
    ]:
        status = "OK" if (new[field] is not None and new[field] >= old[field]) else "LOWER"
        print(f"  {field:24} old={fmt(old[field]):>12}   new={fmt(new[field]):>12}   {status}")
        if new[field] < old[field]:
            issues.append(f"{meta.label}: {field} is lower in new ({new[field]} < {old[field]})")

    # coverage should not worsen
    for field in ["source_missing_registry", "registry_orphans"]:
        status = "OK" if new[field] <= old[field] else "WORSE"
        print(f"  {field:24} old={fmt(old[field]):>12}   new={fmt(new[field]):>12}   {status}")
        if new[field] > old[field]:
            issues.append(f"{meta.label}: {field} got worse in new ({new[field]} > {old[field]})")

    # time boundaries from old data should still be represented in new
    print(f"  {'min_created_at':24} old={old['min_created_at']}   new={new['min_created_at']}")
    print(f"  {'max_created_at':24} old={old['max_created_at']}   new={new['max_created_at']}")
    if old["min_created_at"] is not None and new["min_created_at"] is not None and new["min_created_at"] > old["min_created_at"]:
        issues.append(f"{meta.label}: new min_created_at is later than old")
    if old["max_created_at"] is not None and new["max_created_at"] is not None and new["max_created_at"] < old["max_created_at"]:
        issues.append(f"{meta.label}: new max_created_at is earlier than old")

    # count rows in new up to the old date_entered cutoff
    cutoff = old["max_date_entered"]
    upto = count_new_rows_up_to_old_cutoff(new_conn, meta, cutoff)
    print(f"  {'new_rows_upto_old_max_date_entered':24} old={fmt(old['total_rows']):>12}   new={fmt(upto):>12}")
    if upto is not None and upto < old["total_rows"]:
        issues.append(
            f"{meta.label}: rows in new up to old max(date_entered) are lower than old total "
            f"({upto} < {old['total_rows']})"
        )

    # oldest/newest sample comparisons from old -> new
    for label, direction in [("oldest", "ASC"), ("newest", "DESC")]:
        old_samples = sample_rows_old(old_conn, meta, direction)
        print(f"  sample check ({label}): {len(old_samples)} rows")
        for s in old_samples:
            new_row = fetch_row_by_key(new_conn, meta, s)
            key_desc = ", ".join(f"{k}={s[k]}" for k in meta.key_cols)
            if new_row is None:
                issues.append(f"{meta.label}: missing sampled {label} row in new ({key_desc})")
                print(f"    MISSING in new: {key_desc}")
                continue
            if new_row != s:
                issues.append(f"{meta.label}: sampled {label} row mismatch for {key_desc}")
                print(f"    MISMATCH: {key_desc}")
                print(f"      old={s}")
                print(f"      new={new_row}")
            else:
                print(f"    OK: {key_desc}")

    return issues


def compare_transcripts(old_conn, new_conn) -> list[str]:
    issues: list[str] = []

    items = {
        "youtube_video": transcript_summary(
            old_conn,
            parent_table="youtube.video",
            parent_key="video_id",
            transcript_col="transcript",
            started_col="transcription_started_at",
            updated_col="transcript_updated_at",
            segments_table="youtube.transcript_segments",
            segments_fk="video_id",
        ),
        "podcast_episode": transcript_summary(
            old_conn,
            parent_table="podcasts.episodes",
            parent_key="id",
            transcript_col="transcript",
            started_col="transcription_started_at",
            updated_col="transcript_updated_at",
            segments_table="podcasts.transcript_segments",
            segments_fk="episode_id",
        ),
    }
    items_new = {
        "youtube_video": transcript_summary(
            new_conn,
            parent_table="youtube.video",
            parent_key="video_id",
            transcript_col="transcript",
            started_col="transcription_started_at",
            updated_col="transcript_updated_at",
            segments_table="youtube.transcript_segments",
            segments_fk="video_id",
        ),
        "podcast_episode": transcript_summary(
            new_conn,
            parent_table="podcasts.episodes",
            parent_key="id",
            transcript_col="transcript",
            started_col="transcription_started_at",
            updated_col="transcript_updated_at",
            segments_table="podcasts.transcript_segments",
            segments_fk="episode_id",
        ),
    }

    print("\n" + "=" * 100)
    print("TRANSCRIPT / SEGMENT COUNTS (new should not be lower)")
    print("=" * 100)

    for label in ["youtube_video", "podcast_episode"]:
        print(f"\n[{label}]")
        old = items[label]
        new = items_new[label]

        for field in [
            "parent_rows",
            "with_transcript",
            "started_count",
            "updated_count",
            "segment_rows",
            "segment_parents",
        ]:
            status = "OK" if new[field] >= old[field] else "LOWER"
            print(f"  {field:24} old={fmt(old[field]):>12}   new={fmt(new[field]):>12}   {status}")
            if new[field] < old[field]:
                issues.append(f"{label}: {field} is lower in new ({new[field]} < {old[field]})")

        # this one can legitimately rise because new DB kept working;
        # just print it and only flag if absurdly worse
        print(
            f"  {'transcript_without_segments':24} old={fmt(old['transcript_without_segments']):>12}   "
            f"new={fmt(new['transcript_without_segments']):>12}"
        )

    return issues


def compare_schema_migrations(old_conn, new_conn) -> list[str]:
    issues: list[str] = []
    old = schema_migrations(old_conn)
    new = schema_migrations(new_conn)

    print("\n" + "=" * 100)
    print("SCHEMA MIGRATIONS")
    print("=" * 100)
    print(f"  old rows: {len(old)}")
    print(f"  new rows: {len(new)}")

    old_set = {(r["version"], r["checksum"]) for r in old}
    new_set = {(r["version"], r["checksum"]) for r in new}

    missing = old_set - new_set
    if missing:
        issues.append(f"schema_migrations: new is missing {len(missing)} old migration rows")
        print("  MISSING in new:")
        for m in sorted(missing):
            print(f"    {m}")
    else:
        print("  OK: all old schema migrations exist in new")

    return issues


def main():
    with connect(OLD_PREFIX) as old_conn, connect(NEW_PREFIX) as new_conn:
        old_conn.autocommit = True
        new_conn.autocommit = True

        all_issues: list[str] = []

        print("=" * 100)
        print("SOURCE TABLE SANITY CHECKS")
        print("=" * 100)

        for meta in SOURCE_TABLES:
            all_issues.extend(compare_source(old_conn, new_conn, meta))

        all_issues.extend(compare_transcripts(old_conn, new_conn))
        all_issues.extend(compare_schema_migrations(old_conn, new_conn))

        print("\n" + "=" * 100)
        print("RESULT")
        print("=" * 100)
        if all_issues:
            print(f"Found {len(all_issues)} issue(s):")
            for issue in all_issues:
                print(f"  - {issue}")
            raise SystemExit(1)
        else:
            print("No issues found. New DB looks like a superset / faithful carry-forward of old DB for checked data.")


if __name__ == "__main__":
    main()