"""
some cols were changed for reddit submissions (mainly selftext added)
so we have to go over all existing reddit submissions and re-pull their data
"""


from __future__ import annotations

import argparse
import logging
from typing import List, Tuple, Optional

from dotenv import load_dotenv
import prawcore
from psycopg2.extras import execute_values

from db.db import init_pool, close_pool, getcursor

# Reuse your existing Reddit + mapping logic
from services.reddit_monitor.scrape_runner import (
    make_reddit_api_interface,
    submission_id_bare,
    backoff_api_call,
    _submission_to_row,  # yes, it's "private", but this is a one-off backfill script
)

load_dotenv()


RESUME_FILE = "reddit_submission_backfill.tmp"


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _read_resume_cursor(path: str) -> tuple[Optional[str], Optional[str]]:
    """
    File format: "<created_at_ts_iso>\t<id>\n"
    Example: "2026-02-10 12:34:56.123456+00\tt3_abc123\n"
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            line = f.read().strip()
        if not line:
            return None, None
        parts = line.split("\t")
        if len(parts) != 2:
            logging.warning("Resume file %s is malformed; ignoring.", path)
            return None, None
        last_created_at_ts, last_id = parts[0].strip(), parts[1].strip()
        return (last_created_at_ts or None, last_id or None)
    except FileNotFoundError:
        return None, None


def _write_resume_cursor(path: str, last_created_at_ts: str, last_id: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(f"{last_created_at_ts}\t{last_id}\n")
    # atomic-ish replace on POSIX
    import os
    os.replace(tmp, path)


def _fetch_batch_keyset(
    *,
    batch_size: int,
    last_created_at_ts: Optional[str],
    last_id: Optional[str],
) -> List[Tuple[str, str]]:
    """
    Keyset pagination over sm.reddit_submission.

    Returns list of (submission_link_id, created_at_ts_iso_str)
    where submission_link_id is expected to be like 't3_abc123'.
    """
    with getcursor() as cur:
        if last_created_at_ts is None:
            cur.execute(
                """
                SELECT id, created_at_ts::text
                FROM sm.reddit_submission
                WHERE selftext IS NULL
                    AND shared_url IS NULL
                ORDER BY created_at_ts DESC, id DESC
                LIMIT %s
                """,
                (batch_size,),
            )
        else:
            # (created_at_ts, id) < (last_created_at_ts, last_id)
            cur.execute(
                """
                SELECT id, created_at_ts::text
                FROM sm.reddit_submission
                WHERE (created_at_ts, id) < (%s::timestamptz, %s)
                    AND selftext IS NULL
                    AND shared_url IS NULL
                ORDER BY created_at_ts DESC, id DESC
                LIMIT %s
                """,
                (last_created_at_ts, last_id, batch_size),
            )

        rows = cur.fetchall()
        return [(str(r[0]), str(r[1])) for r in rows]


def _update_rows(rows: List[dict]) -> int:
    """
    Batch UPDATE for existing submissions.
    Updates: url, permalink, shared_url, selftext, filtered_text, subreddit, subreddit_id
    (and nothing else).
    """
    if not rows:
        return 0

    values = [
        (
            r["id"],
            r.get("url"),
            r.get("permalink"),
            r.get("shared_url"),
            r.get("selftext"),
            r.get("filtered_text"),
            r.get("subreddit"),
            r.get("subreddit_id"),
        )
        for r in rows
    ]

    sql = """
        UPDATE sm.reddit_submission AS s
        SET
            url = v.url,
            permalink = v.permalink,
            shared_url = v.shared_url,
            selftext = v.selftext,
            filtered_text = v.filtered_text,
            subreddit = v.subreddit,
            subreddit_id = v.subreddit_id
        FROM (VALUES %s) AS v(
            id,
            url,
            permalink,
            shared_url,
            selftext,
            filtered_text,
            subreddit,
            subreddit_id
        )
        WHERE s.id = v.id
    """

    with getcursor(commit=True) as cur:
        execute_values(cur, sql, values, page_size=1000)
        # cursor.rowcount is not reliable for UPDATE ... FROM (VALUES ...) across all drivers,
        # so we return len(rows) as "updated attempted".
        return len(rows)


def main() -> int:
    _setup_logging()

    ap = argparse.ArgumentParser(
        description="Backfill reddit_submission fields using live Reddit API data."
    )
    ap.add_argument("--prod", action="store_true",
                    help="Use prod DB pool prefix.")
    ap.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="How many DB rows to process per batch.",
    )
    ap.add_argument(
        "--max-submissions",
        type=int,
        default=0,
        help="Optional cap on how many submissions to process (0 = no cap).",
    )

    args = ap.parse_args()

    prefix = "prod" if args.prod else "dev"
    init_pool(prefix=prefix)

    try:
        reddit = make_reddit_api_interface()

        total_seen = 0
        total_updated = 0
        total_skipped = 0
        total_failed = 0

        last_created_at_ts: Optional[str] = None
        last_id: Optional[str] = None

        last_created_at_ts, last_id = _read_resume_cursor(RESUME_FILE)
        if last_created_at_ts and last_id:
            logging.info(
                "Resuming from cursor: created_at_ts=%s id=%s (file=%s)",
                last_created_at_ts,
                last_id,
                RESUME_FILE,
            )

        while True:
            if args.max_submissions and total_seen >= args.max_submissions:
                break

            batch = _fetch_batch_keyset(
                batch_size=args.batch_size,
                last_created_at_ts=last_created_at_ts,
                last_id=last_id,
            )
            if not batch:
                logging.info("No more submissions found. Done.")
                break

            logging.info("Fetched %d submissions for backfill.", len(batch))

            update_rows: List[dict] = []

            # Track last successfully *checked* item in this batch for resume.
            last_checked_id: Optional[str] = None
            last_checked_ts: Optional[str] = None

            for link_id, created_at_ts in batch:
                if args.max_submissions and total_seen >= args.max_submissions:
                    break

                total_seen += 1
                last_checked_id = link_id
                last_checked_ts = created_at_ts

                bare_id = submission_id_bare(link_id)

                try:
                    submission = backoff_api_call(
                        lambda: reddit.submission(id=bare_id))
                    # Force fetch early so 404/403 happens here
                    backoff_api_call(lambda: getattr(submission, "id"))

                    row = _submission_to_row(submission)
                    if row is None:
                        total_failed += 1
                        continue

                    # Only carry fields we actually update
                    update_rows.append(
                        {
                            "id": row["id"],  # should be 't3_<id>'
                            "url": row.get("url"),
                            "permalink": row.get("permalink"),
                            "shared_url": row.get("shared_url"),
                            "selftext": row.get("selftext"),
                            "filtered_text": row.get("filtered_text"),
                            "subreddit": row.get("subreddit"),
                            "subreddit_id": row.get("subreddit_id"),
                        }
                    )

                except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden) as e:
                    total_skipped += 1
                    msg = (
                        f"Skipping {link_id} (bare_id={bare_id}): "
                        f"{type(e).__name__} ({e})"
                    )
                    logging.info(msg)
                    continue
                except Exception as e:
                    total_failed += 1
                    logging.exception("Failed to backfill %s: %s", link_id, e)
                    continue

            updated = _update_rows(update_rows)
            total_updated += updated

            if last_checked_ts and last_checked_id:
                _write_resume_cursor(
                    args.resume_file, last_checked_ts, last_checked_id)

            last_id, last_created_at_ts = batch[-1][0], batch[-1][1]

            logging.info(
                "Progress: seen=%d updated_total=%d skipped_total=%d failed_total=%d (last_batch_updated=%d)",
                total_seen,
                total_updated,
                total_skipped,
                total_failed,
                updated,
            )

        logging.info(
            "Backfill complete. seen=%d updated=%d skipped=%d failed=%d",
            total_seen,
            total_updated,
            total_skipped,
            total_failed,
        )
        return 0

    finally:
        close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
