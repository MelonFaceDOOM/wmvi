from __future__ import annotations


import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Iterable, Dict, Optional, Tuple
import random

import re
import praw
import prawcore
from psycopg2.extensions import cursor as PGCursor

from db.db import getcursor

from ingestion.ingestion import ensure_scrape_job
from ingestion.reddit_submission import flush_reddit_submission_batch
from ingestion.reddit_comment import flush_reddit_comment_batch
from filtering.anonymization import redact_pii
from ingestion.reddit_comment import parse_link_id, parse_comment_id

import threading

# a lock for 1 expensive reddit api endpoint
_replace_more_lock = threading.Lock()
# -------------------------
# HIGH LEVEL ENTRYPOINTS
# -------------------------


def scrape_term_once(term: str) -> int:
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

    with getcursor() as cur:
        term_id = _get_term_id(cur, term)
        last_found_ts, last_found_id = _get_reddit_search_status(cur, term_id)

    new_submissions: List[object] = []
    for submission in get_new_submissions_since_status(
        reddit,
        term,
        last_found_ts=last_found_ts,
        last_found_id=last_found_id,
    ):
        new_submissions.append(submission)

    if not new_submissions:
        logging.info("No new submissions found for %r", term)
        return 0

    logging.info("Found %d new submissions for term %r",
                 len(new_submissions), term)

    # Map to format that can be ingested to db
    rows = _submissions_to_rows(new_submissions)
    logging.info("Mapped %d/%d submissions to rows for term %r",
                 len(rows), len(new_submissions), term)

    try:
        logging.info("Ensuring scrape job for term %r ...", term)
        job_id = ensure_scrape_job(
            name=f"reddit monitor: {term}",
            description=f"Continuous monitor scrape for term {term!r}",
            platforms=["reddit_submission", "reddit_comment"],
        )
        inserted, skipped = flush_reddit_submission_batch(rows, job_id)
        logging.info(
            "Flush complete for term %r: inserted=%d skipped=%d", term, inserted, skipped)

    except Exception:
        logging.exception(
            "Insert pipeline failed for term %r (rows=%d)", term, len(rows))
        raise

    # -------------------------
    # Fetch + save comments for newly seen submissions
    # -------------------------
    for s in new_submissions:
        # PRAW gives num_comments in the listing response, usually without extra requests.
        reported = int(getattr(s, "num_comments", 0) or 0)
        if reported <= 0:
            continue

        link_id = parse_link_id(s.id)  # 't3_<id>'

        if not _should_fetch_comments_for_submission(link_id, reported, max_comments=500):
            continue
        _ = fetch_comment_rows_for_submission(
            reddit=reddit,
            submission_id_any=link_id,
            job_id=job_id
        )
    # record new scrape status info for this term
    max_ts = last_found_ts
    max_id = last_found_id

    for s in new_submissions:
        ts = datetime.fromtimestamp(float(s.created_utc), tz=timezone.utc)
        sid = parse_link_id(s.id)
        if ts > max_ts or (ts == max_ts and sid > max_id):
            max_ts, max_id = ts, sid

    # Only update if we advanced
    if max_ts > last_found_ts or (max_ts == last_found_ts and max_id != last_found_id):
        with getcursor() as cur:
            _upsert_reddit_search_status(cur, term_id, max_ts, max_id)

    return inserted


def submission_id_bare(raw: str) -> str:
    """
    Convert submission id to bare base36 for PRAW calls.
    Accepts bare or 't3_'.
    """
    link_id = parse_link_id(raw)  # ensures 't3_<id>'
    return link_id.removeprefix("t3_")


def fetch_comment_rows_for_submission(
    reddit: praw.Reddit,
    submission_id_any: str,
    *,
    job_id: int | None = None,
    top_level_only: bool = False,
    max_comments: int = 500,
    replace_more_limit: int = 8,
    replace_more_threshold: int = 10,
) -> int:
    """Get comments for a given submission.
       Does not attempt to 'only insert new comments'
       So it should be called with a strategy in mind to target submissions
       that don't already have comments saved

       return number inserted
       """
    link_id = parse_link_id(submission_id_any)
    bare_id = submission_id_bare(submission_id_any)

    try:
        submission = backoff_api_call(lambda: reddit.submission(id=bare_id))

        # Force fetch early so 404/403 happens here
        backoff_api_call(lambda: getattr(submission, "id"))
        with _replace_more_lock:
            backoff_api_call(
                lambda: submission.comments.replace_more(
                    limit=replace_more_limit,
                    threshold=replace_more_threshold,
                )
            )

    except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden) as e:
        logging.info(
            "Skipping %r: Reddit returned %s when fetching submission (bare_id=%r) (%s)",
            link_id,
            type(e).__name__,
            bare_id,
            e,
        )
        return 0

    gen = _iter_top_level_comments(
        submission) if top_level_only else _iter_all_comments(submission)

    rows: List[dict] = []
    for c in gen:
        if len(rows) >= max_comments:
            break
        row = _comment_to_row(c, link_id=link_id)  # store link_id as 't3_<id>'
        if row is not None:
            rows.append(row)

    if not rows:
        logging.info("No rows found for submission id %s", link_id)
        return 0

    # Ensure scrape job & bulk insert + link
    if job_id is None:
        job_id = ensure_scrape_job(
            name=f"reddit comments scrape",
            description=f"scrape for a given submission_id",
            platforms=["reddit_comment"],
        )

    inserted, skipped = flush_reddit_comment_batch(rows, job_id=job_id)

    logging.info(
        "Inserted %d reddit comments skipped %d.",
        inserted,
        skipped,
    )
    return inserted

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


# ---------------------------------------
# Common Utils for submission + comments
# ---------------------------------------


_RATELIMIT_RE = re.compile(r"(\d+)\s*(second|minute)", re.IGNORECASE)


def _parse_ratelimit_seconds(message: str) -> int | None:
    """
    Try to parse Reddit's human message into seconds.
    Returns None if it can't parse.
    """
    m = _RATELIMIT_RE.search(message or "")
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2).lower()
    return n if unit.startswith("second") else n * 60


def backoff_api_call(
    api_call_func,
    *args,
    max_sleep: int = 300,
    max_attempts: int | None = 3,
    **kwargs,
):
    """
    Call `api_call_func(*args, **kwargs)` with retry/backoff on transient Reddit/PRAW failures.

    - Retries on:
        - praw.exceptions.RedditAPIException with RATELIMIT item(s)
        - prawcore.exceptions.TooManyRequests (uses Retry-After when available)
        - prawcore.exceptions.RequestException (network issues)
        - prawcore.exceptions.ResponseException / ServerError / BadGateway / GatewayTimeout (transient HTTP issues)

    - Does NOT catch StopIteration (so wrapping next(generator) works correctly).
    - If max_attempts is set, re-raises the *last retryable exception* once attempts are exceeded.
      (Attempts count only calls to api_call_func, not internal sleeps.)
    """
    delay = 2.0
    attempts = 0
    last_retryable: Optional[BaseException] = None

    def _attempt_str() -> str:
        return f"{attempts}/{max_attempts}" if max_attempts is not None else str(attempts)

    while True:
        if max_attempts is not None and attempts >= max_attempts:
            # Re-raise the last retryable exception for better tracebacks/debuggability.
            if last_retryable is not None:
                raise last_retryable
            raise RuntimeError(
                f"backoff_api_call exceeded max_attempts={max_attempts}")

        try:
            attempts += 1
            return api_call_func(*args, **kwargs)

        # ---------- PRAW "logical" API errors (rate limit in body) ----------
        except praw.exceptions.RedditAPIException as e:
            wait_seconds = None
            for item in getattr(e, "items", []):
                if getattr(item, "error_type", None) == "RATELIMIT":
                    msg = getattr(item, "message", "") or ""
                    wait_seconds = _parse_ratelimit_seconds(msg) or 60
                    logging.warning(
                        "Rate limit hit. Waiting %ds before retry (attempt %s)...",
                        wait_seconds,
                        _attempt_str(),
                    )
                    time.sleep(wait_seconds)
                    delay = 2.0
                    break

            if wait_seconds is None:
                # Not a rate-limit; don't retry blindly.
                raise

            last_retryable = e
            continue

        except (prawcore.exceptions.Forbidden, prawcore.exceptions.NotFound) as e:
            raise  # do not retry

        # ---------- 429 / Too Many Requests ----------
        except prawcore.exceptions.TooManyRequests as e:
            last_retryable = e

            retry_after = getattr(e, "response", None)
            retry_after = getattr(retry_after, "headers", {}).get(
                "retry-after") if retry_after else None

            if retry_after is not None:
                try:
                    sleep_for = max(int(retry_after), 1)
                except ValueError:
                    sleep_for = delay
            else:
                sleep_for = delay

            sleep_for = min(float(sleep_for), float(max_sleep))
            sleep_for *= (0.8 + random.random() * 0.4)

            logging.warning(
                "TooManyRequests: %s. Sleeping %.1fs (attempt %s)...",
                e,
                sleep_for,
                _attempt_str(),
            )
            time.sleep(sleep_for)
            delay = min(delay * 2.0, float(max_sleep))
            continue

        # ---------- Transient HTTP / server issues ----------
        except (
            prawcore.exceptions.RequestException,
            prawcore.exceptions.ResponseException,
            prawcore.exceptions.ServerError,
        ) as e:
            last_retryable = e

            sleep_for = min(delay, float(max_sleep))
            sleep_for *= (0.8 + random.random() * 0.4)

            logging.warning(
                "%s: %s. Retrying in %.1fs (attempt %s)...",
                type(e).__name__,
                e,
                sleep_for,
                _attempt_str(),
            )
            time.sleep(sleep_for)
            delay = min(delay * 2.0, float(max_sleep))
            continue

# -------------------------
# SUBMISSIONS STUFF
# -------------------------


def _get_term_id(cur: PGCursor, term: str) -> int:
    cur.execute("SELECT id FROM taxonomy.vaccine_term WHERE name = %s", (term,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"term {term!r} not found in taxonomy.vaccine_term")
    return int(row[0])


def _get_reddit_search_status(cur: PGCursor, term_id: int) -> Tuple[datetime, str]:
    """
    Returns (last_found_ts, last_found_id). If missing, returns a sentinel old value.
    last_found_id is expected to be a fullname like 't3_abcd12'.
    """
    cur.execute(
        """
        SELECT last_found_ts, last_found_id
        FROM sm.reddit_submission_search_status
        WHERE term_id = %s
        """,
        (term_id,),
    )
    row = cur.fetchone()
    if not row:
        # Sentinel: very old time + empty id boundary
        return (datetime(1970, 1, 1, tzinfo=timezone.utc), "")
    return (row[0], row[1])


def _upsert_reddit_search_status(cur: PGCursor, term_id: int, last_found_ts: datetime, last_found_id: str) -> None:
    """
    Upsert the per-term high-water mark.
    """
    cur.execute(
        """
        INSERT INTO sm.reddit_submission_search_status (term_id, last_found_ts, last_found_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (term_id) DO UPDATE
        SET last_found_ts = EXCLUDED.last_found_ts,
            last_found_id = EXCLUDED.last_found_id,
            last_updated = now()
        """,
        (term_id, last_found_ts, last_found_id),
    )


def get_new_submissions_since_status(
    reddit: praw.Reddit,
    query_str: str,
    *,
    last_found_ts: datetime,
    last_found_id: str,
):
    """
    Yield submissions from Reddit search sorted by 'new' until we hit the boundary
    defined by (last_found_ts, last_found_id).

    Boundary stop:
      - stop when created_at_ts < last_found_ts
      - stop when created_at_ts == last_found_ts AND link_id <= last_found_id
        (lex compare on fullnames; good enough as a tie-breaker when timestamps collide)
    """
    logging.info(
        "Starting submission scrape for query %r (boundary ts=%s id=%r)",
        query_str,
        last_found_ts.isoformat(),
        last_found_id,
    )

    submission_generator = reddit.subreddit(
        "all").search(query_str, sort="new", limit=None)

    while True:
        try:
            submission = backoff_api_call(lambda: next(submission_generator))
        except StopIteration:
            break

        created_ts = datetime.fromtimestamp(
            float(submission.created_utc), tz=timezone.utc)
        link_id = parse_link_id(submission.id)  # 't3_<id>'

        # Stop once we're at/behind the last processed boundary.
        if created_ts < last_found_ts:
            break
        if created_ts == last_found_ts and last_found_id and link_id <= last_found_id:
            break

        yield submission


def _submission_to_row(submission) -> dict | None:
    """
    Map a PRAW Submission -> dict keyed by REDDIT_SUB_COLS.
    """
    # created_utc is seconds since epoch (float)
    created_ts = datetime.fromtimestamp(
        submission.created_utc, tz=timezone.utc)

    title = submission.title or ""
    is_self = bool(getattr(submission, "is_self", False))

    # Pull selftext only when relevant (and avoid attribute surprises)
    selftext = getattr(submission, "selftext", "") or ""
    selftext_norm = selftext.strip().lower()

    # Treat these as "no usable selftext"
    unusable_selftext = (
        selftext.strip() == ""
        or selftext_norm in {"[removed]", "[deleted]"}
        or selftext_norm == "[redacted]"
    )

    if not is_self:
        raw_for_filter = title
    else:
        if unusable_selftext:
            raw_for_filter = title
        else:
            raw_for_filter = f"{title}\n{selftext}"

    filtered = redact_pii(raw_for_filter)

    internal_id = parse_link_id(submission.id)

    # Some attributes may not be present on all submissions; use getattr with defaults.
    subreddit_obj = getattr(submission, "subreddit", None)

    subreddit_name = getattr(
        subreddit_obj, "display_name", "") if subreddit_obj is not None else ""
    if not isinstance(subreddit_name, str):
        subreddit_name = ""

    subreddit_id = getattr(
        subreddit_obj, "id", "") if subreddit_obj is not None else ""
    if not isinstance(subreddit_id, str):
        subreddit_id = ""

    if getattr(submission, "permalink", None):
        reddit_url = f"https://www.reddit.com{submission.permalink}"
    else:
        reddit_url = f"https://www.reddit.com/comments/{submission.id}"

    # Outbound/shared URL
    shared_url = getattr(submission, "url", None) or None
    if shared_url == reddit_url:
        shared_url = None

    logging.info("===ATTRIBUTES GATHERED, FINAL FORMATTING===")
    try:
        row = {
            "id": internal_id,
            "url": reddit_url,
            "domain": submission.domain or "reddit.com",
            "title": title,
            "selftext": selftext if is_self else "",
            "permalink": reddit_url,
            "shared_url": shared_url,
            "created_at_ts": created_ts,
            "filtered_text": filtered,
            # "url_overridden_by_dest": getattr(submission, "url_overridden_by_dest", None), REMOVING CUS IT SEEMS TO CAUSE A WEB REQUEST TO ACCESS IT
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
            "is_created_from_ads_ui": bool(getattr(submission, "is_created_from_ads_ui", False)),
            "is_self": is_self,
            "is_video": bool(getattr(submission, "is_video", False)),
            # These three are JSON in the DB; insert_batch(json_cols=...) will serialize.
            "media": getattr(submission, "media", None),
            "gildings": getattr(submission, "gildings", None),
            "all_awardings": getattr(submission, "all_awardings", None),
            # Language detection is done in a standardized later pass.
            "is_en": None,
        }
        return row
    except Exception as e:
        logging.exception("Failed to map submission %s: %s", submission.id, e)
        return None


def _submissions_to_rows(submissions) -> List[dict]:
    rows: List[Dict] = []
    t0 = time.time()
    n = len(submissions)

    for i, s in enumerate(submissions, start=1):
        if i == 1 or i % 25 == 0:
            logging.info("Mapping progress %d/%d", i, n)

        row = _submission_to_row(s)
        if row is not None:
            rows.append(row)

    logging.info("Mapping complete: %d/%d in %.2fs",
                 len(rows), n, time.time() - t0)
    return rows

# -------------------------
# COMMENTS STUFF
# -------------------------


def _should_fetch_comments_for_submission(link_id: str, reported_num_comments: int, *, max_comments: int) -> bool:
    """
    Decide if we should hit Reddit for comments.
    - If Reddit says 0 comments: skip.
    - If we already have >= min(reported, max_comments) stored: skip.
    """
    if reported_num_comments <= 0:
        return False

    already = count_comments_for_link_id(link_id)
    target = min(int(reported_num_comments), int(max_comments))
    return already < target


def get_submission_exists_and_num_comments(submission_id_any: str) -> tuple[bool, int]:
    """
    sm.reddit_submission.id is stored as 't3_<id>'.
    Returns (exists, num_comments). If not exists, num_comments=0.
    """
    link_id = parse_link_id(submission_id_any)  # normalize to 't3_<id>'

    with getcursor() as cur:
        cur.execute(
            """
            SELECT num_comments
            FROM sm.reddit_submission
            WHERE id = %s
            """,
            (link_id,),
        )
        row = cur.fetchone()

    if not row:
        logging.info("DB precheck miss: raw=%r normalized=%r (no row in sm.reddit_submission)",
                     submission_id_any, link_id)
        return (False, 0)

    return (True, int(row[0] or 0))


def count_comments_for_link_id(link_id_t3: str) -> int:
    with getcursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM sm.reddit_comment
            WHERE link_id = %s
            """,
            (link_id_t3,),
        )
        return int(cur.fetchone()[0] or 0)


def _iter_top_level_comments(submission) -> Iterable[object]:
    for c in submission.comments:
        yield c


def _iter_all_comments(submission) -> Iterable[object]:
    for c in submission.comments.list():
        yield c


def _comment_to_row(comment, *, link_id: str) -> dict | None:
    """
    Returns dict keyed exactly by REDDIT_COMMENT_COLS.
    - id and parent_comment_id are 't1_<id>' (or None for parent)
    - link_id is 't3_<id>'
    """
    try:
        created_ts = datetime.fromtimestamp(
            float(comment.created_utc), tz=timezone.utc)

        body = comment.body or ""
        filtered = redact_pii(body)

        comment_id = parse_comment_id(getattr(comment, "id", None))
        if comment_id is None:
            return None

        parent_comment_id = parse_comment_id(
            getattr(comment, "parent_id", None))

        subreddit_obj = getattr(comment, "subreddit", None)
        subreddit_name = getattr(
            subreddit_obj, "display_name", "") if subreddit_obj else ""

        permalink = getattr(comment, "permalink", None)
        if permalink:
            permalink = f"https://www.reddit.com{permalink}"
        else:
            permalink = (
                f"https://www.reddit.com/comments/"
                f"{link_id.removeprefix('t3_')}"
            )

        row = {
            "id": comment_id,
            "parent_comment_id": parent_comment_id,
            "link_id": link_id,
            "body": body,
            "permalink": permalink,
            "created_at_ts": created_ts,
            "filtered_text": filtered,
            "subreddit_id": getattr(comment, "subreddit_id", "") or "",
            "subreddit_type": getattr(comment, "subreddit_type", None),
            "total_awards_received": int(getattr(comment, "total_awards_received", 0) or 0),
            "subreddit": subreddit_name or "",
            "score": int(getattr(comment, "score", 0) or 0),
            "gilded": int(getattr(comment, "gilded", 0) or 0),
            "stickied": bool(getattr(comment, "stickied", False)),
            "is_submitter": bool(getattr(comment, "is_submitter", False)),
            "gildings": getattr(comment, "gildings", None),
            "all_awardings": getattr(comment, "all_awardings", None),
        }

        return row
    except Exception as e:
        logging.exception("Failed mapping comment -> row: %s", e)
        return None
