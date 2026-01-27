from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Set

import praw
import prawcore
from psycopg2.extensions import cursor as PGCursor

from db.db import getcursor

from ingestion.ingestion import ensure_scrape_job
from ingestion.reddit_submission import flush_reddit_submission_batch
from filtering.anonymization import redact_pii
from ingestion.reddit_comment import parse_link_id 


# -------------------------
# Reddit API setup
# -------------------------

def make_reddit_api_interface() -> praw.Reddit:
    """
    Initialize a PRAW Reddit client using env vars:

        REDDIT_ID, REDDIT_SECRET, REDDIT_UA (optional)
    """
    try:
        logging.info("Initializing Reddit API interface")
        return praw.Reddit(
            client_id=os.environ["REDDIT_ID"],
            client_secret=os.environ["REDDIT_SECRET"],
            user_agent=os.getenv("REDDIT_UA", "wmvi-reddit-scraper/0.1"),
            ratelimit_seconds=60,
        )
    except KeyError as k:
        raise SystemExit(f"Missing env var: {k}. Check .env file.") from k


def backoff_api_call(api_call_func, *args, max_sleep: int = 300, **kwargs):
    """
    Retry a PRAW API call with exponential backoff on transient errors.
    """
    delay = 2
    while True:
        try:
            return api_call_func(*args, **kwargs)
        except StopIteration:
            raise
        except praw.exceptions.RedditAPIException as e:
            for item in e.items:
                if item.error_type == "RATELIMIT":
                    logging.warning("Rate limit hit: %s", item.message)
                    # crude parse for "X minutes"
                    import re
                    match = re.search(r"(\d+)\s+minute", item.message)
                    wait_minutes = int(match.group(1)) if match else 1
                    wait_seconds = wait_minutes * 60
                    logging.info("Waiting %s seconds before retrying...", wait_seconds)
                    time.sleep(wait_seconds)
                    break
            else:
                # no RATELIMIT item -> re-raise
                raise
        except prawcore.exceptions.RequestException as e:
            logging.warning("Request exception: %s. Retrying...", e)
            time.sleep(5)
        except Exception as e:
            logging.error("Unexpected exception in backoff_api_call: %s", e)
            raise
        time.sleep(delay)
        delay = min(delay * 2, max_sleep)


# -------------------------
# Existing-ID lookup
# -------------------------

def _get_existing_submission_ids_for_term(
    cur: PGCursor,
    term: str,
    limit: int = 1000,
) -> Set[str]:
    """
    Find up to `limit` existing reddit_submission IDs whose filtered_text
    matches the term (ILIKE), ordered by recency.

    Used for early stopping when scraping a term: once we see one of these IDs,
    we can break.
    """
    pattern = f"%{term}%"
    cur.execute(
        """
        SELECT id
        FROM sm.reddit_submission
        WHERE filtered_text ILIKE %s
        ORDER BY created_at_ts DESC
        LIMIT %s
        """,
        (pattern, limit),
    )
    return {row[0] for row in cur.fetchall()}

# -------------------------
# Core scrape primitives
# -------------------------

def get_submissions_until_duplicate(
    reddit: praw.Reddit,
    query_str: str,
    existing_submission_ids: Set[str] | None = None,
):
    """
    Generator that yields new PRAW submissions for a query.

    - Searches r/all for `query_str`, sorted by 'new'.
    - Stops when it encounters a submission whose ID is already in
      `existing_submission_ids`.
    """
    logging.info("Starting submission scrape for query %r", query_str)

    if existing_submission_ids is None:
        existing_submission_ids = set()

    gen = reddit.subreddit("all").search(query_str, sort="new", limit=None)

    for submission in gen:
        # submission = backoff_api_call(lambda: submission)
        if submission.id in existing_submission_ids:
            logging.info(
                "Stopping early for %r: submission ID %s already exists.",
                query_str,
                submission.id,
            )
            break

        yield submission
        logging.debug("Yielded submission ID: %s", submission.id)


# -------------------------
# Mapping PRAW → ingestion rows
# -------------------------

def _submission_to_row(submission) -> dict:
    """
    Map a PRAW Submission -> dict keyed by REDDIT_SUB_COLS.
    """
    # created_utc is seconds since epoch (float)
    created_ts = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)

    title = submission.title or ""
    filtered = redact_pii(title)

    # id format: we store without the "t3_" prefix (same as CSV importer).
    internal_id = parse_link_id(submission.id)

    # Some attributes may not be present on all submissions; use getattr with defaults.
    subreddit_obj = getattr(submission, "subreddit", None)
    if subreddit_obj is not None and hasattr(subreddit_obj, "display_name"):
        subreddit_name = subreddit_obj.display_name
    else:
        subreddit_name = getattr(submission, "subreddit", "") or ""

    subreddit_id = getattr(subreddit_obj, "id", "") if subreddit_obj is not None else ""
    try:
        row = {
            "id": internal_id,
            "url": submission.url or f"https://www.reddit.com/comments/{submission.id}",
            "domain": submission.domain or "reddit.com",
            "title": title,
            "permalink": f"https://www.reddit.com{submission.permalink}"
            if getattr(submission, "permalink", None)
            else f"https://www.reddit.com/comments/{submission.id}",
            "created_at_ts": created_ts,
            "filtered_text": filtered,
            # "url_overridden_by_dest": getattr(submission, "url_overridden_by_dest", None), JUST REPLACING CUS IT SEEMS TO CAUSE A WEB REQUEST TO ACCESS IT
            "url_overridden_by_dest": None,
            "subreddit_id": subreddit_id or "",
            "subreddit": subreddit_name,
            "upvote_ratio": float(getattr(submission, "upvote_ratio", 1.0) or 1.0),
            "score": int(getattr(submission, "score", 0) or 0),
            "gilded": int(getattr(submission, "gilded", 0) or 0),
            "num_comments": int(getattr(submission, "num_comments", 0) or 0),
            "num_crossposts": int(getattr(submission, "num_crossposts", 0) or 0),
            "pinned": bool(getattr(submission, "pinned", False)),
            "stickied": bool(getattr(submission, "stickied", False)),
            "over_18": bool(getattr(submission, "over_18", False)),
            "is_created_from_ads_ui": bool(
                getattr(submission, "is_created_from_ads_ui", False)
            ),
            "is_self": bool(getattr(submission, "is_self", False)),
            "is_video": bool(getattr(submission, "is_video", False)),
            # These three are JSON in the DB; insert_batch(json_cols=...) will serialize.
            "media": getattr(submission, "media", None),
            "gildings": getattr(submission, "gildings", None),
            "all_awardings": getattr(submission, "all_awardings", None),
            # Language detection can be a later pass.
            "is_en": None,
        }
        return row
    except Exception as e:
        logging.exception("Failed to map submission %s: %s", submission.id, e)
        return None

def _submissions_to_rows(submissions) -> List[dict]:
    rows: List[Dict] = []
    for s in submissions:
        row = _submission_to_row(s)
        if row is not None:
            rows.append(row)
    return rows


# -------------------------
# High-level entrypoint called by scheduler
# -------------------------

def scrape_term_once(term: str) -> None:
    """
    High-level "do one scrape cycle for a term" entrypoint.

    Responsibilities:
      - Get existing submission IDs for this term (for early stopping).
      - Stream fresh submissions from Reddit.
      - Map PRAW submissions -> sm.reddit_submission rows.
      - Insert via ingestion.reddit_submission.flush_reddit_submission_batch,
        and link posts to a scrape.job via ensure_scrape_job.
    """
    logging.info("Scrape runner: starting scrape for term %r", term)

    reddit = make_reddit_api_interface()

    # 1) Find existing submission IDs (for early stop)
    with getcursor() as cur:
        existing_ids = _get_existing_submission_ids_for_term(cur, term)

    # 2) Fetch new submissions from Reddit
    new_submissions: List[object] = []
    for submission in get_submissions_until_duplicate(reddit, term, existing_ids):
        new_submissions.append(submission)

    if not new_submissions:
        logging.info("No new submissions found for %r", term)
        return

    logging.info("Found %d new submissions for term %r", len(new_submissions), term)

    # 3) Map to ingestion rows
    rows = _submissions_to_rows(new_submissions)

    # 4) Ensure scrape job & bulk insert + link
    job_id = ensure_scrape_job(
        name=f"reddit monitor: {term}",
        description=f"Continuous monitor scrape for term {term!r}",
        platforms=["reddit_submission"],
    )

    inserted, skipped = flush_reddit_submission_batch(rows, job_id)

    logging.info(
        "Inserted %d reddit submissions for term %r (skipped %d existing)",
        inserted,
        term,
        skipped,
    )
    
    return inserted
