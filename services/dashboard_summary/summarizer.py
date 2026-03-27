from __future__ import annotations

import json
from decimal import Decimal
import datetime as dt
import logging
import math
import os
from typing import Any

from dotenv import load_dotenv

from db.db import init_pool, getcursor, close_pool
from services.storage import StorageBackend, LocalFileStorage, AzureBlobStorage


load_dotenv()


# ----------------------------------------------------------------------
# logging
# ----------------------------------------------------------------------

def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def get_storage_backend() -> StorageBackend:
    kind = os.environ.get("SUMMARY_STORAGE_KIND", "local").strip().lower()

    if kind == "local":
        base_dir = os.environ.get("SUMMARY_LOCAL_DIR", "./tmp_summary_exports")
        return LocalFileStorage(base_dir)

    if kind == "azure":
        return AzureBlobStorage.from_env()

    raise ValueError(f"Unsupported SUMMARY_STORAGE_KIND: {kind!r}")


# ----------------------------------------------------------------------
# query helpers
# ----------------------------------------------------------------------

def run_query(sql: str) -> list[dict[str, Any]]:
    with getcursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in rows]


def _json_default(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def to_json(data: Any) -> str:
    return json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        default=_json_default,
    )


def timed_query(name: str, sql: str) -> list[dict[str, Any]]:
    q0 = dt.datetime.now(dt.timezone.utc)
    rows = run_query(sql)
    q1 = dt.datetime.now(dt.timezone.utc)
    logging.info(
        "Query done: %s (%d rows, %.3fs)",
        name,
        len(rows),
        (q1 - q0).total_seconds(),
    )
    return rows


# ----------------------------------------------------------------------
# SQL
# ----------------------------------------------------------------------

SQL_INGESTION_DAILY = """
WITH src AS (
    SELECT 'reddit_submission'::text AS platform, date_entered FROM sm.reddit_submission
    UNION ALL
    SELECT 'reddit_comment'::text AS platform, date_entered FROM sm.reddit_comment
    UNION ALL
    SELECT 'telegram_post'::text AS platform, date_entered FROM sm.telegram_post
    UNION ALL
    SELECT 'youtube_video'::text AS platform, date_entered FROM youtube.video
    UNION ALL
    SELECT 'youtube_comment'::text AS platform, date_entered FROM youtube.comment
    UNION ALL
    SELECT 'podcast_episode'::text AS platform, date_entered FROM podcasts.episodes
    UNION ALL
    SELECT 'news_article'::text AS platform, date_entered FROM news.article
)
SELECT
    date_trunc('day', date_entered)::date AS day,
    platform,
    COUNT(*)::bigint AS n
FROM src
WHERE date_entered >= now() - interval '30 days'
GROUP BY 1, 2
ORDER BY 1 ASC, 2 ASC;
"""

SQL_IS_EN_COUNTS_7D = """
SELECT
    date_trunc('day', date_entered)::date AS day,
    platform,
    COUNT(*) FILTER (WHERE is_en IS TRUE)::bigint  AS is_en_true,
    COUNT(*) FILTER (WHERE is_en IS FALSE)::bigint AS is_en_false,
    COUNT(*) FILTER (WHERE is_en IS NULL)::bigint  AS is_en_null
FROM sm.posts_all
WHERE date_entered >= now() - interval '7 days'
GROUP BY 1, 2
ORDER BY 1 ASC, 2 ASC;
"""

SQL_TERM_MATCHES_PER_DAY = """
SELECT
    date_trunc('day', matched_at)::date AS day,
    COUNT(*)::bigint AS matches
FROM matches.post_term_hit
WHERE matched_at >= now() - interval '30 days'
GROUP BY 1
ORDER BY 1 ASC;
"""

SQL_PODCAST_TRANSCRIPTIONS_PER_DAY = """
SELECT
    date_trunc('day', transcript_updated_at)::date AS day,
    COUNT(*)::bigint AS transcriptions
FROM podcasts.episodes
WHERE transcript IS NOT NULL
  AND transcript_updated_at >= now() - interval '30 days'
GROUP BY 1
ORDER BY 1 ASC;
"""

SQL_PODCAST_TRANSCRIPTION_SUMMARY = """
SELECT
    COUNT(*)::bigint AS total,
    COUNT(*) FILTER (WHERE transcript IS NOT NULL)::bigint AS completed,
    COUNT(*) FILTER (
        WHERE transcription_started_at IS NOT NULL
          AND transcript IS NULL
    )::bigint AS in_progress
FROM podcasts.episodes;
"""

SQL_YOUTUBE_TRANSCRIPTIONS_PER_DAY = """
SELECT
    date_trunc('day', transcript_updated_at)::date AS day,
    COUNT(*)::bigint AS transcriptions
FROM youtube.video
WHERE transcript IS NOT NULL
  AND transcript_updated_at >= now() - interval '30 days'
GROUP BY 1
ORDER BY 1 ASC;
"""

SQL_YOUTUBE_TRANSCRIPTION_SUMMARY = """
SELECT
    COUNT(*)::bigint AS total,
    COUNT(*) FILTER (WHERE transcript IS NOT NULL)::bigint AS completed,
    COUNT(*) FILTER (
        WHERE transcription_started_at IS NOT NULL
          AND transcript IS NULL
    )::bigint AS in_progress
FROM youtube.video;
"""


# ----------------------------------------------------------------------
# postprocessing
# ----------------------------------------------------------------------

def add_transcription_forecast(
    summary_rows: list[dict[str, Any]],
    daily_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not summary_rows:
        return summary_rows

    row = dict(summary_rows[0])

    total = int(row.get("total") or 0)
    completed = int(row.get("completed") or 0)
    in_progress = int(row.get("in_progress") or 0)
    remaining = max(total - completed, 0)

    pct = round((completed / total) * 100.0, 2) if total > 0 else None

    daily_total = 0
    for drow in daily_rows:
        n = int(drow.get("transcriptions") or 0)
        daily_total += n

    # Use 30-day average across all days, not only active days.
    avg_per_day_30 = daily_total / 30.0 if daily_total > 0 else 0.0

    est_finish_date = None
    est_days_remaining = None
    if remaining == 0:
        est_days_remaining = 0
        est_finish_date = dt.date.today().isoformat()
    elif avg_per_day_30 > 0:
        est_days_remaining = math.ceil(remaining / avg_per_day_30)
        est_finish_date = (
            dt.date.today() + dt.timedelta(days=est_days_remaining)
        ).isoformat()

    return [
        {
            "total": total,
            "completed": completed,
            "in_progress": in_progress,
            "percent_completion": pct,
            "avg_completed_per_day_30d": round(avg_per_day_30, 4) if avg_per_day_30 > 0 else 0.0,
            "estimated_days_remaining": est_days_remaining,
            "estimated_finish_date": est_finish_date,
        }
    ]


# ----------------------------------------------------------------------
# export helpers
# ----------------------------------------------------------------------

def build_dashboard_summary() -> dict[str, Any]:
    logging.info("Running summary queries...")
    t0 = dt.datetime.now(dt.timezone.utc)

    ingestion_daily = timed_query("ingestion_daily", SQL_INGESTION_DAILY)
    is_en_counts_7d = timed_query("is_en_counts_7d", SQL_IS_EN_COUNTS_7D)
    term_matches_per_day = timed_query("term_matches_per_day", SQL_TERM_MATCHES_PER_DAY)

    podcast_daily = timed_query(
        "podcast_transcriptions_per_day",
        SQL_PODCAST_TRANSCRIPTIONS_PER_DAY,
    )
    podcast_summary_raw = timed_query(
        "podcast_transcription_summary",
        SQL_PODCAST_TRANSCRIPTION_SUMMARY,
    )
    podcast_summary = add_transcription_forecast(podcast_summary_raw, podcast_daily)

    youtube_daily = timed_query(
        "youtube_transcriptions_per_day",
        SQL_YOUTUBE_TRANSCRIPTIONS_PER_DAY,
    )
    youtube_summary_raw = timed_query(
        "youtube_transcription_summary",
        SQL_YOUTUBE_TRANSCRIPTION_SUMMARY,
    )
    youtube_summary = add_transcription_forecast(youtube_summary_raw, youtube_daily)

    t1 = dt.datetime.now(dt.timezone.utc)
    logging.info("All queries complete (%.3fs total)", (t1 - t0).total_seconds())

    return {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "ingestion_daily": ingestion_daily,
        "is_en_counts_7d": is_en_counts_7d,
        "term_matches_per_day": term_matches_per_day,
        "podcast_transcriptions_per_day": podcast_daily,
        "podcast_transcription_summary": podcast_summary,
        "youtube_transcriptions_per_day": youtube_daily,
        "youtube_transcription_summary": youtube_summary,
    }


def build_and_export_all(storage: StorageBackend) -> None:
    t0 = dt.datetime.now(dt.timezone.utc)

    dashboard_summary = build_dashboard_summary()

    logging.info("Starting export...")
    w0 = dt.datetime.now(dt.timezone.utc)
    storage.write_text("dashboard_summary.json", to_json(dashboard_summary))
    w1 = dt.datetime.now(dt.timezone.utc)

    logging.info("Wrote dashboard_summary.json (%.3fs)", (w1 - w0).total_seconds())
    logging.info("build_and_export_all complete (%.3fs total)", (w1 - t0).total_seconds())


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------

def main(prod: bool = False) -> None:
    _setup_logging()

    if prod:
        init_pool(prefix="prod")
        logging.info("Initialized DB pool with PROD prefix.")
    else:
        init_pool(prefix="dev")
        logging.info("Initialized DB pool with DEV prefix.")

    storage = get_storage_backend()
    ok, reason = storage.is_accessible()
    if not ok:
        raise RuntimeError(f"Storage not accessible: {reason}")

    logging.info("Storage backend accessible.")

    try:
        build_and_export_all(storage)
        logging.info("Summaries exported successfully.")
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received, shutting down summarizer...")
        raise
    finally:
        close_pool()
        logging.info("DB pool closed; exiting.")


if __name__ == "__main__":
    main(prod=False)