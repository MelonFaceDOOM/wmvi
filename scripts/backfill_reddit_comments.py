from __future__ import annotations

import argparse
import logging
from typing import List, Tuple

from dotenv import load_dotenv

from db.db import init_pool, close_pool, getcursor
from ingestion.ingestion import ensure_scrape_job

# Import your existing functions from wherever they live:
# adjust the import path to your actual module name/file
from services.reddit_monitor.scrape_runner import (
    make_reddit_api_interface,
    fetch_comment_rows_for_submission,
)

load_dotenv()


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _fetch_candidates(limit: int, offset: int = 0) -> List[Tuple[str, int]]:
    """
    Returns list of (submission_link_id, num_comments) where:
      - num_comments > 0
      - no rows exist in sm.reddit_comment for that link_id
    """
    with getcursor() as cur:
        cur.execute(
            """
            SELECT s.id, s.num_comments
            FROM sm.reddit_submission s
            WHERE s.num_comments > 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM sm.reddit_comment c
                  WHERE c.link_id = s.id
              )
            ORDER BY s.created_at_ts DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        return [(row[0], int(row[1] or 0)) for row in cur.fetchall()]


def main() -> int:
    _setup_logging()

    ap = argparse.ArgumentParser(
        description="Backfill Reddit comments for submissions with num_comments>0 but no saved comments.")
    ap.add_argument("--prod", action="store_true",
                    help="Use prod DB pool prefix.")
    ap.add_argument("--batch-size", type=int, default=200,
                    help="How many submissions to select per DB batch.")
    ap.add_argument("--max-submissions", type=int, default=0,
                    help="Optional cap on how many submissions to process (0 = no cap).")
    ap.add_argument("--max-comments", type=int, default=500)
    ap.add_argument("--replace-more-limit", type=int, default=8)
    ap.add_argument("--replace-more-threshold", type=int, default=10)
    args = ap.parse_args()

    prefix = "prod" if args.prod else "dev"
    init_pool(prefix=prefix)

    try:
        reddit = make_reddit_api_interface()

        job_id = ensure_scrape_job(
            name="reddit comment backfill",
            description="Backfill: fetch comments for submissions with num_comments>0 and no saved comments",
            platforms=["reddit_comment"],
        )

        total_processed = 0
        total_inserted = 0
        offset = 0

        while True:
            if args.max_submissions and total_processed >= args.max_submissions:
                break

            batch = _fetch_candidates(limit=args.batch_size, offset=offset)
            if not batch:
                logging.info("No more candidates found. Done.")
                break

            logging.info(
                "Fetched %d candidate submissions (offset=%d).", len(batch), offset)

            for link_id, reported_num_comments in batch:
                if args.max_submissions and total_processed >= args.max_submissions:
                    break

                inserted = fetch_comment_rows_for_submission(
                    reddit=reddit,
                    submission_id_any=link_id,
                    job_id=job_id,
                    top_level_only=False,
                    max_comments=args.max_comments,
                    replace_more_limit=args.replace_more_limit,
                    replace_more_threshold=args.replace_more_threshold,
                )

                total_processed += 1
                total_inserted += int(inserted or 0)

                logging.info(
                    "Backfill progress: processed=%d inserted_total=%d (last=%s inserted=%d reported_num_comments=%d)",
                    total_processed,
                    total_inserted,
                    link_id,
                    inserted,
                    reported_num_comments,
                )

            offset += args.batch_size

        logging.info("Backfill complete. processed=%d inserted_total=%d job_id=%s",
                     total_processed, total_inserted, job_id)
        return 0

    finally:
        close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
