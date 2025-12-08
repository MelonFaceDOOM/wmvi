from __future__ import annotations
import os
import json
import csv
from pathlib import Path
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from db.db import init_pool, getcursor, close_pool
from db.migrations_runner import run_migrations
from typing import Optional, Iterable
from datetime import datetime, timezone
from filtering.anonymization import redact_pii
from ingestion.ingestion import ensure_scrape_job
from ingestion.reddit_submission import flush_reddit_submission_batch
from ingestion.telegram_post import flush_telegram_batch
from ingestion.youtube_video import flush_youtube_video_batch
from ingestion.youtube_comment import flush_youtube_comment_batch
from ingestion.reddit_comment import (
    flush_reddit_comment_batch,
    parse_link_id,
    parse_comment_id
)
from ingestion.podcast import (
    flush_podcast_shows_batch,
    flush_podcast_episodes_batch,
    flush_podcast_transcript_segments_batch,
)


load_dotenv()

TARGET_DB = os.environ.get("DEV_PGDATABASE")


def main():
    # Optionally: inspect the old DB
    # print_old_db_summary()

    # --- Rebuild DEV DB and run migrations ---
    
    # don't run this by accident lmao
    # # # # # reset_dev_db()
    
    
    ensure_database(TARGET_DB, admin_prefix="DEV_", admin_db="postgres")
    init_pool(prefix="DEV", minconn=1, maxconn=4, force_tunnel=False)

    try:
        applied = run_migrations(migrations_dir="db/migrations")
        print("Applied migrations:", applied)

        # -----------------------------
        # Legacy DB -> new schema
        # -----------------------------
        legacy_ins, legacy_skip = transfer_historical_reddit_submissions()
        print(
            f"Legacy reddit submissions (DB) - inserted: {legacy_ins}, skipped: {legacy_skip}"
        )

        shows_ins, shows_skip = transfer_podcast_shows()
        print(
            f"Podcast shows (DB) - inserted: {shows_ins}, skipped: {shows_skip}"
        )

        eps_ins, eps_skip = transfer_podcast_episodes()
        print(
            f"Episodes (DB) - inserted: {eps_ins}, skipped: {eps_skip}"
        )

        seg_ins, seg_skip = transfer_transcript_segments()
        print(
            f"Transcript segments (DB) - inserted: {seg_ins}, skipped: {seg_skip}"
        )

        # -----------------------------
        # Disk files -> new schema
        # -----------------------------
        tg_ins, tg_skip = import_telegram_jsonl(
            path="data/telegram.jsonl",
            batch_commit=5000,
        )
        print(
            f"Telegram posts (disk) - inserted: {tg_ins}, skipped: {tg_skip}"
        )

        yt_vid_ins, yt_vid_skip = import_yt_videos_jsonl(
            path="data/yt_videos.jsonl",
            batch_commit=5000,
        )
        print(
            f"YouTube videos (disk) - inserted: {yt_vid_ins}, skipped: {yt_vid_skip}"
        )

        # Ensure videos are present before comments (FK)
        yt_cmt_ins, yt_cmt_skip = import_yt_comments_jsonl(
            path="data/yt_comments.jsonl",
            batch_commit=5000,
        )
        print(
            f"YouTube comments (disk) - inserted: {yt_cmt_ins}, skipped: {yt_cmt_skip}"
        )

        rs_ins, rs_skip = import_reddit_submissions_csv(
            path="data/reddit_submissions.csv",
            batch_commit=5000,
        )
        print(
            f"Reddit submissions (disk CSV) - inserted: {rs_ins}, skipped: {rs_skip}"
        )

        rc_ins, rc_skip = import_reddit_comments_csv(
            path="data/reddit_comments.csv",
            batch_commit=5000,
        )
        print(
            f"Reddit comments (disk CSV) - inserted: {rc_ins}, skipped: {rc_skip}"
        )

    finally:
        close_pool()

 
 
def print_old_db_summary():
    with _old_conn() as c:
        with c.cursor() as cur:
            cur.execute("SELECT count(*) FROM tweet")
            print("OLD.tweet count:", cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM reddit_submission")
            print("OLD.reddit_submission count:", cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM reddit_comment")
            print("OLD.reddit_comment count:", cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM podcasts")
            print("OLD.podcasts count:", cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM episodes")
            print("OLD.episodes count:", cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM transcript_segments")
            print("OLD.transcript_segments count:", cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM transcript_words")
            print("OLD.transcript_words count:", cur.fetchone()[0])
            

# ----------------------------------------------
# -------------- GENERAL DB STUFF --------------
# ----------------------------------------------

def reset_dev_db() -> None:
    """
    Drop and recreate the dev database from scratch.
    """
    # connect to postgres, drop wmvi
    db_to_drop = "wmvi"
    dsn = db_creds_from_env("DEV_", db_override="postgres")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (db_to_drop,),
            )
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_to_drop)))
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_to_drop)))
    finally:
        conn.close()

def ensure_database(dbname: str, admin_prefix: str = "TEST_", admin_db: str = "postgres") -> None:
    """
    connect to postgres, but ensure that TEST_PGDATABASE exists
    """
    dsn = (
        f"host={os.environ[f'{admin_prefix}PGHOST']} "
        f"port={os.environ.get(f'{admin_prefix}PGPORT','5432')} "
        f"dbname={admin_db} user={os.environ[f'{admin_prefix}PGUSER']} "
        f"password={os.environ[f'{admin_prefix}PGPASSWORD']} sslmode=require"
    )
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            if not cur.fetchone():
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    finally:
        conn.close()
   
def db_creds_from_env(prefix: str, db_override: str | None = None) -> str:
    host = os.environ[f"{prefix}PGHOST"]
    port = os.environ.get(f"{prefix}PGPORT", "5432")
    user = os.environ[f"{prefix}PGUSER"]
    pwd  = os.environ[f"{prefix}PGPASSWORD"]
    db   = db_override or os.environ.get(f"{prefix}PGDATABASE", "postgres")
    ssl  = os.environ.get(f"{prefix}PGSSLMODE", "require")
    return f"host={host} port={port} dbname={db} user={user} password={pwd} sslmode={ssl}"

def _old_conn():
    """Direct connection to OLD_ database (no pool)."""
    return psycopg2.connect(db_creds_from_env("OLD_"))
    

# -------------------------------------------------
# ---------- TRANSFER REDDIT SUBMISSIONS ----------
# -------------------------------------------------

OLD_REDDIT_SUB_COLS = [
    "id",
    "url",
    "domain",
    "title",
    "permalink",
    "created_utc",
    "url_overridden_by_dest",
    "subreddit_id",
    "subreddit",
    "upvote_ratio",
    "score",
    "gilded",
    "num_comments",
    "num_crossposts",
    "pinned",
    "stickied",
    "over_18",
    "is_created_from_ads_ui",
    "is_self",
    "is_video",
    "media",
    "gildings",
    "all_awardings",
    "is_en",
]

def ensure_scrape_job_reddit_submissions() -> int:
    return ensure_scrape_job(
        name="historical reddit submissions",
        description="Backfill from legacy reddit_submission table",
        platforms=["reddit_submission"],
    )


def _iter_old_reddit_submissions(n: int = 0):
    """
    Yields dict rows from OLD_.reddit_submission in created_utc order.
    Keys match OLD_REDDIT_SUB_COLS.
    """
    sql_base = f"""
        SELECT
        {",\n".join(OLD_REDDIT_SUB_COLS)}
        FROM reddit_submission
        ORDER BY created_utc
    """
    with _old_conn() as conn:
        with conn.cursor(
            name="old_reddit_submissions_stream",
            cursor_factory=RealDictCursor
        ) as cur:
            if n > 0:
                cur.execute(sql_base + " LIMIT %s", (n,))
            else:
                cur.execute(sql_base)
            for row in cur:
                yield row


def transfer_historical_reddit_submissions(
    n: int = 0, batch_commit: int = 2000
) -> tuple[int, int]:
    """
    Copy N reddit submissions from OLD_.reddit_submission into sm.reddit_submission.
    - n=0 means copy all.
    - Creates/gets the 'historical reddit submissions' scrape job.
    - For each inserted submission, relies on triggers to populate sm.post_registry,
      then populates scrape.post_scrape.
    Returns number of submissions inserted and skipped
    """
    job_id = ensure_scrape_job_reddit_submissions()
    pending: list[dict] = []
    inserted = 0
    skipped = 0

    for row in _iter_old_reddit_submissions(n=n):
        row["created_at_ts"] = datetime.fromtimestamp(row["created_utc"], tz=timezone.utc)
        row["filtered_text"] = redact_pii(row["title"])
        row["id"] = parse_link_id(row["id"])
        del row["created_utc"] 
        
        pending.append(row)

        if len(pending) >= batch_commit:
            batch_inserted, batch_skipped = flush_reddit_submission_batch(pending, job_id)
            inserted += batch_inserted
            skipped += batch_skipped
            pending.clear()

    if pending:
        batch_inserted, batch_skipped = flush_reddit_submission_batch(pending, job_id)
        inserted += batch_inserted
        skipped += batch_skipped
        pending.clear()

    return inserted, skipped
    
    
# ----------------------------------
# ---------- TRANSFER PODCAST SHOWS
# ----------------------------------
# OLD.podcasts -> podcasts.shows

OLD_PODCAST_SHOWS_COLS = [
    "id",
    "date_entered",
    "title",
    "rss_url",
]


def _iter_old_podcast_shows(n: int = 0):
    """
    Yields dict rows from OLD_.podcasts ordered by id.
    Keys: id, date_entered, title, rss_url.
    """
    sql_base = f"""
        SELECT
            {", ".join(OLD_PODCAST_SHOWS_COLS)}
        FROM podcasts
        ORDER BY id
    """
    with _old_conn() as conn:
        with conn.cursor(
            name="old_podcast_shows_stream",
            cursor_factory=RealDictCursor,
        ) as cur:
            if n > 0:
                cur.execute(sql_base + " LIMIT %s", (n,))
            else:
                cur.execute(sql_base)
            for row in cur:
                yield row


def transfer_podcast_shows(
    n: int = 0,
    batch_commit: int = 2000,
) -> tuple[int, int]:
    """
    Copy N podcast shows from OLD_.podcasts -> podcasts.shows via ingestion layer.
    - n=0 means copy all.

    Returns:
        (inserted, skipped)
    """
    pending: list[dict] = []
    inserted = 0
    skipped = 0

    for row in _iter_old_podcast_shows(n=n):
        # Row already matches new-schema keys for podcasts.shows
        d = {
            "id": row["id"],
            "date_entered": row["date_entered"],
            "title": row["title"],
            "rss_url": row["rss_url"],
        }
        pending.append(d)

        if len(pending) >= batch_commit:
            batch_inserted, batch_skipped = flush_podcast_shows_batch(pending)
            inserted += batch_inserted
            skipped += batch_skipped
            pending.clear()

    if pending:
        batch_inserted, batch_skipped = flush_podcast_shows_batch(pending)
        inserted += batch_inserted
        skipped += batch_skipped
        pending.clear()

    return inserted, skipped


# -----------------------------
# ---------- TRANSFER EPISODES
# -----------------------------
# OLD.episodes -> podcasts.episodes

OLD_EP_COLS = [
    "id",
    "date_entered",
    "audio_path",
    "guid",
    "title",
    "description",
    "pub_date",
    "download_url",
    "podcast_id",
]


def _iter_old_episodes(n: int = 0):
    """
    Yields dict rows from OLD_.episodes ordered by id.
    Keys match OLD_EP_COLS.
    """
    sql_base = f"""
        SELECT
            {", ".join(OLD_EP_COLS)}
        FROM episodes
        ORDER BY id
    """
    with _old_conn() as conn:
        with conn.cursor(
            name="old_episodes_stream",
            cursor_factory=RealDictCursor,
        ) as cur:
            if n > 0:
                cur.execute(sql_base + " LIMIT %s", (n,))
            else:
                cur.execute(sql_base)
            for row in cur:
                yield row


def transfer_podcast_episodes(
    n: int = 0,
    batch_commit: int = 2000,
) -> tuple[int, int]:
    """
    Copy N episodes from OLD_.episodes -> podcasts.episodes via ingestion layer.
    - n=0 means copy all.

    Converts:
        pub_date (legacy) -> created_at_ts (new schema)

    Returns:
        (inserted, skipped)
    """
    pending: list[dict] = []
    inserted = 0
    skipped = 0

    for row in _iter_old_episodes(n=n):
        if row["pub_date"] is not None:
            created_at_ts = row["pub_date"].replace(tzinfo=timezone.utc)
        else:
            created_at_ts = None

        d = {
            "id": row["id"],
            "date_entered": row["date_entered"],
            "audio_path": row["audio_path"],
            "guid": row["guid"],
            "title": row["title"],
            "description": row["description"],
            "created_at_ts": created_at_ts,
            "download_url": row["download_url"],
            "podcast_id": row["podcast_id"],
        }

        pending.append(d)

        if len(pending) >= batch_commit:
            batch_inserted, batch_skipped = flush_podcast_episodes_batch(pending)
            inserted += batch_inserted
            skipped += batch_skipped
            pending.clear()

    if pending:
        batch_inserted, batch_skipped = flush_podcast_episodes_batch(pending)
        inserted += batch_inserted
        skipped += batch_skipped
        pending.clear()

    return inserted, skipped


# --------------------------------------------------
# ---------- TRANSFER TRANSCRIPT SEGMENTS ----------
# --------------------------------------------------

OLD_SEGMENT_COLS = [
    "id",
    "episode_id",
    "seg_idx",
    "start_s",
    "end_s",
    "text",
]


def _iter_old_transcript_segments(n: int = 0):
    """
    Yields dict rows from OLD_.transcript_segments ordered by episode_id, seg_idx.
    Keys: id, episode_id, seg_idx, start_s, end_s, text.
    """
    sql_base = f"""
        SELECT
            {", ".join(OLD_SEGMENT_COLS)}
        FROM transcript_segments
        ORDER BY episode_id, seg_idx
    """
    with _old_conn() as conn:
        with conn.cursor(
            name="old_transcript_segments_stream",
            cursor_factory=RealDictCursor,
        ) as cur:
            if n > 0:
                cur.execute(sql_base + " LIMIT %s", (n,))
            else:
                cur.execute(sql_base)
            for row in cur:
                yield row


def transfer_transcript_segments(
    n: int = 0,
    batch_commit: int = 2000,
) -> tuple[int, int]:
    """
    Copy N transcript segments from OLD_.transcript_segments -> podcasts.transcript_segments
    via ingestion layer.
    - n=0 means copy all.
    - Assumes matching episodes already exist in podcasts.episodes.

    Adds:
        filtered_text = text
        not using redact for podcasts

    Returns:
        (inserted, skipped)
    """
    pending: list[dict] = []
    inserted = 0
    skipped = 0

    for row in _iter_old_transcript_segments(n=n):
        # filtered = redact_pii(row.get("text") or "") # removing from podcasts

        d = {
            "id": row["id"],
            "episode_id": row["episode_id"],
            "seg_idx": row["seg_idx"],
            "start_s": row["start_s"],
            "end_s": row["end_s"],
            "text": row["text"],
            "filtered_text": row["text"],
        }

        pending.append(d)

        if len(pending) >= batch_commit:
            batch_inserted, batch_skipped = flush_podcast_transcript_segments_batch(pending)
            inserted += batch_inserted
            skipped += batch_skipped
            pending.clear()

    if pending:
        batch_inserted, batch_skipped = flush_podcast_transcript_segments_batch(pending)
        inserted += batch_inserted
        skipped += batch_skipped
        pending.clear()

    return inserted, skipped


# -------------------------------------------------------
# ---------- TRANSFER TELEGRAM POSTS FROM FILE ----------
# -------------------------------------------------------

def import_telegram_jsonl(
    path: str = "data/telegram.jsonl",
    batch_commit: int = 2000,
) -> tuple[int, int]:
    """
    Import Telegram posts from a JSONL file into sm.telegram_post.

    Expects each line to look like:
      {
        "channel_id": ...,
        "message_id": ...,
        "link": "...",
        "date": "2025-08-20T18:08:05+00:00",
        "text": "...",
        "views": ...,
        "forwards": ...,
        "replies": ...,
        "reactions_total": ...,
        "is_pinned": false,
        "has_media": true,
        "raw_type": "Message",
        ...
      }

    Uses ingestion.telegram_post.flush_telegram_batch and a scrape.job
    named 'disk telegram import'.
    
    Returns number of rows inserted and skipped
    """
    job_id = ensure_scrape_job(
        name="disk telegram import",
        description="Import from data/telegram.jsonl",
        platforms=["telegram_post"],
    )

    p = Path(path)
    if not p.exists():
        print(f"[telegram] File not found: {path}")
        return 0

    pending: list[dict] = []
    
    inserted = 0
    skipped = 0 

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)

            # Map to telegram schema columns
            dt = datetime.fromisoformat(rec["date"])
            d: dict = {
                "channel_id": rec["channel_id"],
                "message_id": rec["message_id"],
                "link": rec["link"],
                "created_at_ts": dt,
                "text": rec.get("text") or "",
                "filtered_text": redact_pii(rec.get("text") or ""),
                "views": rec.get("views"),
                "forwards": rec.get("forwards"),
                "replies": rec.get("replies"),
                "reactions_total": rec.get("reactions_total"),
                "is_pinned": rec.get("is_pinned", False),
                "has_media": rec.get("has_media", False),
                "raw_type": rec.get("raw_type"),
                "is_en": None,
            }

            pending.append(d)

            if len(pending) >= batch_commit:
                batch_inserted, batch_skipped = flush_telegram_batch(pending, job_id)
                inserted += batch_inserted
                skipped += batch_skipped
                pending.clear()

    if pending:
        batch_inserted, batch_skipped = flush_telegram_batch(pending, job_id)
        inserted += batch_inserted
        skipped += batch_skipped
        pending.clear()

    return inserted, skipped


# --------------------------------------------------
# ---------- TRANSFER YT VIDEOS FROM FILE ----------
# --------------------------------------------------

def import_yt_videos_jsonl(
    path: str = "data/yt_videos.jsonl",
    batch_commit: int = 2000,
) -> tuple[int, int]:
    """
    Import YouTube videos from JSONL into sm.youtube_video.

    Expects each line roughly like:
      {
        "video_id": "...",
        "url": "...",
        "title": "...",
        "description": "...",
        "published_at": "2025-08-18T21:11:14Z",
        "channel_id": "...",
        "channel_title": "...",
        "duration": "PT1M33S",
        "view_count": 1,
        "like_count": 0,
        "comment_count": 0,
        ...
      }

    Uses ingestion.yt_vid.flush_youtube_video_batch and a scrape.job
    named 'disk youtube videos import'.
    
    Returns number of rows inserted and skipped
    """
    job_id = ensure_scrape_job(
        name="disk youtube videos import",
        description="Import from data/yt_videos.jsonl",
        platforms=["youtube_video"],
    )

    p = Path(path)
    if not p.exists():
        print(f"[yt_videos] File not found: {path}")
        return 0

    inserted = 0
    skipped = 0
    
    pending: list[dict] = []

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)

            published = rec["published_at"].replace("Z", "+00:00")
            dt = datetime.fromisoformat(published)

            d: dict = {
                "video_id": rec["video_id"],
                "url": rec["url"],
                "title": rec["title"],
                "filtered_text": redact_pii(rec.get("title") or ""),
                "description": rec.get("description"),
                "created_at_ts": dt,
                "channel_id": rec["channel_id"],
                "channel_title": rec.get("channel_title"),
                "duration_iso": rec.get("duration"),
                "view_count": rec.get("view_count"),
                "like_count": rec.get("like_count"),
                "comment_count": rec.get("comment_count"),
                "is_en": None,
            }

            pending.append(d)

            if len(pending) >= batch_commit:
                batch_inserted, batch_skipped = flush_youtube_video_batch(pending, job_id)
                inserted += batch_inserted
                skipped += batch_skipped
                pending.clear()

    if pending:
        batch_inserted, batch_skipped = flush_youtube_video_batch(pending, job_id)
        inserted += batch_inserted
        skipped += batch_skipped
        pending.clear()

    return inserted, skipped


# ----------------------------------------------------
# ---------- TRANSFER YT COMMENTS FROM FILE ----------
# ----------------------------------------------------

def import_yt_comments_jsonl(
    path: str = "data/yt_comments.jsonl",
    batch_commit: int = 2000,
) -> tuple[int, int]:
    """
    Import YouTube comments from JSONL into sm.youtube_comment.

    IMPORTANT: Corresponding videos must already exist in sm.youtube_video
    to satisfy the FK.

    Each line roughly:
      {
        "video_id": "...",
        "comment_id": "...",
        "comment_url": "...",
        "text": "...",
        "published_at": "2025-08-18T17:09:49Z",
        "like_count": 1,
        "raw": {...}
      }
      
    Returns number of rows inserted and skipped
    """
    job_id = ensure_scrape_job(
        name="disk youtube comments import",
        description="Import from data/yt_comments.jsonl",
        platforms=["youtube_comment"],
    )

    p = Path(path)
    if not p.exists():
        print(f"[yt_comments] File not found: {path}")
        return 0

    inserted = 0
    skipped = 0
    pending: list[dict] = []

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)

            published = rec["published_at"].replace("Z", "+00:00")
            dt = datetime.fromisoformat(published)

            text = rec.get("text") or ""

            d: dict = {
                "video_id": rec["video_id"],
                "comment_id": rec["comment_id"],
                "comment_url": rec["comment_url"],
                "text": text,
                "filtered_text": redact_pii(text),
                "created_at_ts": dt,
                "like_count": rec.get("like_count"),
                "raw": rec.get("raw") or {},
                "is_en": None,
            }

            pending.append(d)

            if len(pending) >= batch_commit:
                batch_inserted, batch_skipped = flush_youtube_comment_batch(pending, job_id)
                inserted += batch_inserted
                skipped += batch_skipped
                pending.clear()

    if pending:
        batch_inserted, batch_skipped = flush_youtube_comment_batch(pending, job_id)
        inserted += batch_inserted
        skipped += batch_skipped
        pending.clear()

    return inserted, skipped


# -----------------------------------------------------------
# ---------- TRANSFER REDDIT SUBMISSIONS FROM FILE ----------
# -----------------------------------------------------------

def import_reddit_submissions_csv(
    path: str = "data/reddit_submissions.csv",
    batch_commit: int = 2000,
) -> tuple[int, int]:
    """
    Import 'lite' reddit submissions from CSV into sm.reddit_submission.

    CSV columns:
      id,title,subreddit,created_utc,cgpt_response,score,num_comments

    Many fields in sm.reddit_submission are synthesized with reasonable defaults
    (e.g., url/domain/permalink, upvote_ratio=1.0, gilded=0, etc.).
    
    Return number of rows inserted and skipped
    """
    job_id = ensure_scrape_job(
        name="disk reddit submissions import",
        description="Import from data/reddit_submissions.csv",
        platforms=["reddit_submission"],
    )

    p = Path(path)
    if not p.exists():
        print(f"[reddit_submissions] File not found: {path}")
        return 0, 0

    inserted = 0
    skipped = 0
    pending: list[dict] = []

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for rec in reader:
            sid = rec["id"]
            subreddit = rec["subreddit"]
            created_utc = float(rec["created_utc"])
            created_ts = datetime.fromtimestamp(created_utc, tz=timezone.utc)

            score = int(rec["score"]) if rec.get("score") not in (None, "",) else 0
            num_comments = (
                int(rec["num_comments"]) if rec.get("num_comments") not in (None, "",) else 0
            )

            title = rec.get("title") or ""
            filtered = redact_pii(title)

            url = f"https://www.reddit.com/comments/{sid}"
            permalink = url

            d: dict = {
                "id": parse_link_id(sid),
                "url": url,
                "domain": "reddit.com",
                "title": title,
                "permalink": permalink,
                "created_at_ts": created_ts,
                "filtered_text": filtered,
                "url_overridden_by_dest": None,
                # We don't have subreddit_id; use empty string as placeholder.
                "subreddit_id": "",
                "subreddit": subreddit,
                "upvote_ratio": 1.0,
                "score": score,
                "gilded": 0,
                "num_comments": num_comments,
                "num_crossposts": 0,
                "pinned": False,
                "stickied": False,
                "over_18": False,
                "is_created_from_ads_ui": False,
                "is_self": False,
                "is_video": False,
                "media": None,
                "gildings": None,
                "all_awardings": None,
                "is_en": None,
            }

            pending.append(d)

            if len(pending) >= batch_commit:
                batch_inserted, batch_skipped = flush_reddit_submission_batch(pending, job_id)
                inserted += batch_inserted
                skipped += batch_skipped
                pending.clear()

    if pending:
        batch_inserted, batch_skipped = flush_reddit_submission_batch(pending, job_id)
        inserted += batch_inserted
        skipped += batch_skipped
        pending.clear()

    return inserted, skipped


# --------------------------------------------------------
# ---------- TRANSFER REDDIT COMMENTS FROM FILE ----------
# --------------------------------------------------------

def _parse_bool_str(val: str | None) -> bool:
    if val is None:
        return False
    v = str(val).strip().lower()
    return v in ("true", "t", "1", "yes", "y")

def import_reddit_comments_csv(
    path: str = "data/reddit_comments.csv",
    batch_commit: int = 2000,
) -> tuple[int, int]:
    """
    Import reddit comments from CSV into sm.reddit_comment.

    CSV columns:
      id,parent_id,link_id,body,permalink,created_utc,subreddit_id,
      subreddit_type,total_awards_received,subreddit,score,gilded,
      stickied,is_submitter,gildings,all_awardings,is_en
      
    Returns number of rows inserted and skipped
    """
    job_id = ensure_scrape_job(
        name="disk reddit comments import",
        description="Import from data/reddit_comments.csv",
        platforms=["reddit_comment"],
    )

    p = Path(path)
    if not p.exists():
        print(f"[reddit_comments] File not found: {path}")
        return 0

    inserted = 0
    skipped = 0
    pending: list[dict] = []

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for rec in reader:
            created_utc = float(rec["created_utc"])
            created_ts = datetime.fromtimestamp(created_utc, tz=timezone.utc)

            body = rec.get("body") or ""
            filtered = redact_pii(body)

            # Parse JSON-like columns
            gildings_raw = rec.get("gildings")
            all_awardings_raw = rec.get("all_awardings")
            try:
                gildings = json.loads(gildings_raw) if gildings_raw not in (None, "",) else None
            except json.JSONDecodeError:
                gildings = None
            try:
                all_awardings = (
                    json.loads(all_awardings_raw) if all_awardings_raw not in (None, "",) else None
                )
            except json.JSONDecodeError:
                all_awardings = None

            d: dict = {
                "id": parse_comment_id(rec["id"]),
                "parent_comment_id": parse_comment_id(rec.get("parent_id")),
                "link_id": parse_link_id(rec["link_id"]),
                "body": body,
                "permalink": rec["permalink"],
                "created_at_ts": created_ts,
                "filtered_text": filtered,
                "subreddit_id": rec.get("subreddit_id") or "",
                "subreddit_type": rec.get("subreddit_type"),
                "total_awards_received": int(rec["total_awards_received"] or 0),
                "subreddit": rec["subreddit"],
                "score": int(rec["score"] or 0),
                "gilded": int(rec["gilded"] or 0),
                "stickied": _parse_bool_str(rec.get("stickied")),
                "is_submitter": _parse_bool_str(rec.get("is_submitter")),
                "gildings": gildings,
                "all_awardings": all_awardings,
                "is_en": None if rec.get("is_en") in (None, "",) else _parse_bool_str(rec["is_en"]),
            }

            pending.append(d)

            if len(pending) >= batch_commit:
                batch_inserted, batch_skipped = flush_reddit_comment_batch(pending, job_id)
                inserted += batch_inserted
                skipped += batch_skipped
                pending.clear()

    if pending:
        batch_inserted, batch_skipped = flush_reddit_comment_batch(pending, job_id)
        inserted += batch_inserted
        skipped += batch_skipped
        pending.clear()

    return inserted, skipped


if __name__ == "__main__":
    main()
