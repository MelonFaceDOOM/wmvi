"""
Scrape a long range of yt dates, starting with newest and working backwards
Highly coupled to db (search terms, status, results).

Responsibilities:
- load search terms (by list name)
- load per-term status (oldest_found_ts)
- periodically refresh both
- scrape yt via api client
- update status db table
- work through entire date range
- choose appropriate range chunks based on vid rate
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from db.db import getcursor, init_pool, close_pool

from services.youtube.quota_client import (
    BudgetTracker,
    YTBudgetExceeded,
    YTQuotaClient,
    YTQuotaExceeded,
)

from services.youtube.scraping import load_search_terms, scrape_window
from services.youtube.time import next_midnight_pacific, ensure_utc

# ----------------------------
# CONFIG
# ----------------------------

SEARCH_TERM_LIST_NAME = "core_search_terms"

# backfill target range (example: fill 2024)
BACKFILL_START_UTC = datetime(2024, 1, 1, tzinfo=timezone.utc)
BACKFILL_END_UTC = datetime.now(timezone.utc)  # default if none found in db

# adaptive windows
INITIAL_WINDOW = timedelta(days=30)
MIN_WINDOW = timedelta(days=7)
OVERLAP = timedelta(minutes=5)

MAX_PAGES = 10
MIN_NEW_RATIO = 0.1

TOTAL_BUDGET_UNITS_PER_DAY = 3_500

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ----------------------------
# DB boundary helper
# ----------------------------

def oldest_video_ts_for_term(term_id: int) -> Optional[datetime]:
    """
    Return the current per-term oldest boundary (UTC) from youtube.search_status.oldest_found_ts.
    This is the 'window_end' boundary for backfilling (we want to backfill earlier than it).

    Returns None if the term has no row in search_status or oldest_found_ts is NULL.
    """
    with getcursor() as cur:
        cur.execute(
            """
            SELECT oldest_found_ts
            FROM youtube.search_status
            WHERE term_id = %s
            """,
            (term_id,),
        )
        row = cur.fetchone()

    if not row or row[0] is None:
        return None

    return ensure_utc(row[0])


# ----------------------------
# Adaptive backfill per term
# ----------------------------

def backfill_term(qyt: YTQuotaClient, *, term_id: int, term_name: str) -> None:
    oldest_ts = oldest_video_ts_for_term(term_id)

    if oldest_ts is None:
        oldest_ts = BACKFILL_END_UTC
        logging.info("term=%r has no existing data; using backfill end=%s", term_name, oldest_ts.isoformat())

    published_before = oldest_ts
    window_size = INITIAL_WINDOW
    published_after = max(BACKFILL_START_UTC, published_before - window_size)

    while published_before > BACKFILL_START_UTC:
        if not qyt.can_afford("search.list"):
            raise YTBudgetExceeded("budget too low to continue")

        logging.info(
            "Backfill start term=%r window=[%s, %s] window_size=%s budget_used=%d remaining=%d",
            term_name,
            published_after.isoformat(),
            published_before.isoformat(),
            window_size,
            qyt.tracker.used_units_today(),
            qyt.tracker.remaining_units_today(),
        )

        out = scrape_window(
            qyt=qyt,
            term_name=term_name,
            published_after=published_after,
            published_before=published_before,
            max_pages=MAX_PAGES,
            new_ratio_threshold=MIN_NEW_RATIO
        )

        hit_max_pages = out.stops.get("max_pages", 0) > 0
        early_stop = out.stops.get("early_stop_low_new_ratio", 0) > 0

        logging.info(
            "Backfill window done term=%r window=[%s,%s] pages=%d found=%d inserted_v=%d skipped_v=%d new_for_comments=%d inserted_c=%d skipped_c=%d new_comments=%d hit_max_pages=%s early_stop=%s stops=%s",
            term_name,
            published_after.isoformat(),
            published_before.isoformat(),
            out.pages,
            out.found_v,
            out.ins_v,
            out.skip_v,
            len(out.new_vids),
            out.ins_c,
            out.skip_c,
            len(out.new_comments),
            hit_max_pages,
            early_stop,
            out.stops if out.stops else "{}",
        )

        # CHANGE WINDOW SIZE AND RETRY:
        # If we saturated (hit max pages) and the window is still bigger than MIN_WINDOW, shrink and retry.
        if hit_max_pages and window_size > MIN_WINDOW:
            window_size = max(MIN_WINDOW, window_size / 2)
            logging.info("Saturated; shrinking window_size to %s and retrying", window_size)
            continue

        # WINDOW WAS FINE:
        # Adjust window dates
        # We are moving BACKWARD in time, so the next window ends at (published_after + overlap),
        # keep a small overlap to reduce boundary misses.
        anchor = published_after
        next_published_before = anchor + OVERLAP
        if next_published_before >= published_before:
            break
        published_before = next_published_before
        published_after = max(BACKFILL_START_UTC, anchor - window_size)

        # Standard window rate calc
        if (not hit_max_pages) and (not early_stop):
            window_size = min(timedelta(days=90), window_size * 2)

        # TOO MANY RESULTS: 
        # If min window still saturates, accept loss and move on.
        if hit_max_pages and window_size <= MIN_WINDOW:
            logging.warning(
                "term=%r too dense even at MIN_WINDOW=%s; accepting incomplete coverage in this region",
                term_name,
                MIN_WINDOW,
            )
            # move on anyway (published_before already updated)
            continue


# ----------------------------
# Main loop
# ----------------------------
def run_backfill() -> None:
    """
    Run until all terms are fully backfilled, sleeping across quota/budget reset boundaries.
    - YTQuotaClient handles transient backoff internally.
    - We handle daily quota/budget exhaustion by sleeping until next midnight Pacific.
    """
    while True:
        tracker = BudgetTracker(budget_units_per_day=TOTAL_BUDGET_UNITS_PER_DAY)
        qyt = YTQuotaClient.from_api_key(tracker=tracker)

        terms = load_search_terms(SEARCH_TERM_LIST_NAME)

        all_done = True

        for term_id, term_name in terms:
            try:
                backfill_term(qyt, term_id=term_id, term_name=term_name)
            except (YTQuotaExceeded, YTBudgetExceeded) as e:
                # Quota exhausted (API) or local budget exhausted: wait for Pacific reset.
                now = datetime.now(timezone.utc)
                resume_at = next_midnight_pacific(now)
                sleep_s = max(0, (resume_at - now).total_seconds())

                logging.warning(
                    "%s exhausted (%s). Sleeping until %s (%.0fs)",
                    "YT quota" if isinstance(e, YTQuotaExceeded) else "Local budget",
                    str(e),
                    resume_at.isoformat(),
                    sleep_s,
                )

                # Actually sleep; keep service alive.
                __import__("time").sleep(sleep_s)

                # After sleeping, restart outer loop with a fresh client/tracker and refreshed term list.
                all_done = False
                break
            except Exception:
                # Unexpected / worth stopping for.
                logging.exception("Backfill crashed on term=%r (id=%s)", term_name, term_id)
                raise

        if all_done:
            logging.info("Backfill completed for all terms; exiting.")
            return


def main(prod: bool = False) -> None:
    if prod:
        init_pool(prefix="prod")
    else:
        init_pool(prefix="dev")
    try:
        run_backfill()
    finally:
        close_pool()


if __name__ == "__main__":
    main(prod=False)