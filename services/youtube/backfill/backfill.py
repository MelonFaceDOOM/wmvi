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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from db.db import getcursor, init_pool, close_pool

from services.youtube.quota_client import (
    BudgetTracker,
    YTBudgetExceeded,
    YTQuotaClient,
    YTQuotaExceeded,
)

from services.youtube.scraping import ScrapeWindowOutcome, load_search_terms, scrape_window
from services.youtube.time import next_midnight_pacific, ensure_utc

log = logging.getLogger(__name__)

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
# Logging / stats helpers
# ----------------------------

def _fmt_date(dt: datetime) -> str:
    return ensure_utc(dt).strftime("%Y-%m-%d")


def _fmt_window(published_after: datetime, published_before: datetime) -> str:
    return f"{_fmt_date(published_after)}..{_fmt_date(published_before)}"


@dataclass
class TermBackfillStats:
    windows: int = 0
    found_v: int = 0
    ins_v: int = 0
    skip_v: int = 0
    ins_c: int = 0
    skip_c: int = 0
    new_comments: int = 0
    saturated_windows: int = 0

    def add(self, out: ScrapeWindowOutcome, *, hit_max_pages: bool) -> None:
        self.windows += 1
        self.found_v += out.found_v
        self.ins_v += out.ins_v
        self.skip_v += out.skip_v
        self.ins_c += out.ins_c
        self.skip_c += out.skip_c
        self.new_comments += len(out.new_comments)
        if hit_max_pages:
            self.saturated_windows += 1


def _window_notable(out: ScrapeWindowOutcome, *, hit_max_pages: bool, early_stop: bool) -> bool:
    return (
        out.found_v > 0
        or out.ins_v > 0
        or out.ins_c > 0
        or hit_max_pages
        or early_stop
    )


def _log_window_done(
    *,
    term_name: str,
    published_after: datetime,
    published_before: datetime,
    out: ScrapeWindowOutcome,
    hit_max_pages: bool,
    early_stop: bool,
    oldest_saved: datetime | None,
) -> None:
    window = _fmt_window(published_after, published_before)
    msg = (
        f"Window done term={term_name!r}: found={out.found_v} inserted_v={out.ins_v} "
        f"skipped_v={out.skip_v} inserted_c={out.ins_c} skipped_c={out.skip_c} "
        f"pages={out.pages} window={window}"
    )
    if oldest_saved is not None:
        msg += f" oldest_saved={_fmt_date(oldest_saved)}"
    if hit_max_pages:
        msg += " saturated=1"
    if early_stop:
        msg += " early_stop=1"
    if out.stops:
        msg += f" stops={out.stops}"

    if _window_notable(out, hit_max_pages=hit_max_pages, early_stop=early_stop):
        log.info(msg)
    else:
        log.debug(msg)


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


def save_oldest_found_ts(term_id: int, oldest_ts: datetime) -> datetime | None:
    """
    Persist how far back backfill has reached for this term.
    Returns the stored oldest_found_ts, or None if no search_status row exists.
    """
    oldest_ts = ensure_utc(oldest_ts)
    with getcursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE youtube.search_status
               SET oldest_found_ts = LEAST(COALESCE(oldest_found_ts, %s), %s),
                   oldest_updated = now()
             WHERE term_id = %s
            RETURNING oldest_found_ts
            """,
            (oldest_ts, oldest_ts, term_id),
        )
        row = cur.fetchone()
    if not row:
        return None
    return ensure_utc(row[0])


# ----------------------------
# Adaptive backfill per term
# ----------------------------

def backfill_term(qyt: YTQuotaClient, *, term_id: int, term_name: str) -> None:
    oldest_ts = oldest_video_ts_for_term(term_id)

    if oldest_ts is None:
        oldest_ts = BACKFILL_END_UTC
        log.info(
            "Term backfill start term=%r: no prior boundary; starting_at=%s target=%s",
            term_name,
            _fmt_date(oldest_ts),
            _fmt_date(BACKFILL_START_UTC),
        )
    else:
        log.info(
            "Term backfill start term=%r: oldest_boundary=%s target=%s",
            term_name,
            _fmt_date(oldest_ts),
            _fmt_date(BACKFILL_START_UTC),
        )

    published_before = oldest_ts
    window_size = INITIAL_WINDOW
    published_after = max(BACKFILL_START_UTC, published_before - window_size)
    stats = TermBackfillStats()

    while published_before > BACKFILL_START_UTC:
        if not qyt.can_afford("search.list"):
            raise YTBudgetExceeded("budget too low to continue")

        log.debug(
            "Window start term=%r window=%s window_size=%s budget_used=%d remaining=%d",
            term_name,
            _fmt_window(published_after, published_before),
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
        stats.add(out, hit_max_pages=hit_max_pages)

        # CHANGE WINDOW SIZE AND RETRY:
        # If we saturated (hit max pages) and the window is still bigger than MIN_WINDOW, shrink and retry.
        if hit_max_pages and window_size > MIN_WINDOW:
            window_size = max(MIN_WINDOW, window_size / 2)
            log.info(
                "Window saturated term=%r: window=%s pages=%d found=%d inserted_v=%d; "
                "shrinking_to=%s and retrying",
                term_name,
                _fmt_window(published_after, published_before),
                out.pages,
                out.found_v,
                out.ins_v,
                window_size,
            )
            continue

        oldest_saved = save_oldest_found_ts(term_id, published_after)
        if oldest_saved is None:
            log.warning(
                "term=%r: no search_status row; window saved to DB but oldest boundary not persisted",
                term_name,
            )

        _log_window_done(
            term_name=term_name,
            published_after=published_after,
            published_before=published_before,
            out=out,
            hit_max_pages=hit_max_pages,
            early_stop=early_stop,
            oldest_saved=oldest_saved,
        )

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
            log.warning(
                "term=%r too dense even at MIN_WINDOW=%s window=%s; accepting incomplete coverage",
                term_name,
                MIN_WINDOW,
                _fmt_window(published_after, published_before),
            )
            continue

    log.info(
        "Term backfill done term=%r: windows=%d found=%d inserted_v=%d skipped_v=%d "
        "inserted_c=%d skipped_c=%d new_comments=%d saturated_windows=%d "
        "oldest_boundary=%s target=%s",
        term_name,
        stats.windows,
        stats.found_v,
        stats.ins_v,
        stats.skip_v,
        stats.ins_c,
        stats.skip_c,
        stats.new_comments,
        stats.saturated_windows,
        _fmt_date(published_before),
        _fmt_date(BACKFILL_START_UTC),
    )


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

                log.warning(
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
                log.exception("Backfill crashed on term=%r (id=%s)", term_name, term_id)
                raise

        if all_done:
            log.info("Backfill completed for all terms; exiting.")
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