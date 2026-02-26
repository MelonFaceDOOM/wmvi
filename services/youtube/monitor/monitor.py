"""
Long-running YouTube monitor.
Continually searches for new results
Highly coupled to db (search terms, status, results).

Responsibilities:
- load search terms (by list name)
- load per-term status (last seen published_at)
- periodically refresh both
- scrape yt via api client
- update status db table
- schedule next scrape for term based on new vid rate
"""

from __future__ import annotations

import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple
from dataclasses import dataclass, field
import heapq
import threading

from db.db import getcursor, init_pool, close_pool
from services.youtube.scraping import load_search_terms, scrape_window
from services.youtube.time import next_midnight_pacific, publication_span_seconds, ensure_utc, newest_published_dt

from services.youtube.quota_client import (
    BudgetTracker,
    YTBudgetExceeded,
    YTQuotaClient,
    YTQuotaExceeded,
)

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

SEARCH_TERM_LIST_NAME = "core_search_terms"

MAX_PAGES = 2                     # pages per scrape per term
DEFAULT_LOOKBACK_DAYS = 14

STATUS_REFRESH_EVERY_LOOPS = 1     # reload status + term list
SLEEP_BETWEEN_TERMS = 5.0          # seconds
SLEEP_BETWEEN_LOOPS = 30.0

# Rate smoothing
RATE_ALPHA = 0.3                  # EWMA smoothing factor
NUM_SCRAPE_WORKERS = 3
RESULTS_PER_PAGE = 50
SCHED_BUFFER = 2.0          # example: 2x scrapes vs "keep up"
MIN_INTERVAL_S = 60         # don't hammer
MAX_INTERVAL_S = 6 * 3600   # don't starve term (example: 6h)

TOTAL_BUDGET_UNITS_PER_DAY = 5_000

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------------------------------------------------------------------
# DB access
# ---------------------------------------------------------------------

def load_status_table() -> Dict[int, datetime]:
    """
    Load per-term most recent published_at.
    Returns: term_id -> datetime (UTC)
    """
    out: Dict[int, datetime] = {}
    with getcursor() as cur:
        cur.execute(
            """
            SELECT term_id, last_found_ts
            FROM youtube.search_status
            """
        )
        for term_id, ts in cur.fetchall():
            out[int(term_id)] = ensure_utc(ts)
    return out


def update_all_term_statuses(term_states: Dict[int, "TermState"]) -> None:
    """
    Should be called with a lock
    Persist updated last_found_ts for all term_ids in one statement.
    term_states: dict[term_id] -> TermState(term_name, last_seen)
    """
    if not term_states:
        return

    term_ids: list[int] = []
    last_seen_list: list[datetime] = []

    for term_id, st in term_states.items():
        dt = st.last_seen
        if dt is None:
            continue

        term_ids.append(int(term_id))
        last_seen_list.append(dt)

    if not term_ids:
        return

    with getcursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO youtube.search_status (term_id, last_found_ts)
            SELECT x.term_id, x.last_found_ts
            FROM unnest(%s::int[], %s::timestamptz[]) AS x(term_id, last_found_ts)
            ON CONFLICT (term_id)
            DO UPDATE SET
                last_found_ts = EXCLUDED.last_found_ts,
                last_updated = now()
            """,
            (term_ids, last_seen_list),
        )



# ---------------------------------------------------------------------
# Monitor state
# ---------------------------------------------------------------------

@dataclass
class TermState:
    name: str
    last_seen: datetime
    rate: float = 0.0  # items/sec
    next_run_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    last_run_at: datetime | None = None


@dataclass
class PauseState:
    # Used for global sleeps when quota limit has been reached
    # Represented by a timestamp in seconds past epoch
    # the purpose of this container is simply to make the
    # float value mutable so it can be passed between threads
    until_ts: float = 0.0  # epoch seconds


def build_heap(term_states: Dict[int, TermState]) -> list[Tuple[float, int]]:
    """Ordered list of (timestamp, term_id)"""
    heap: list[Tuple[float, int]] = []
    for tid, st in term_states.items():
        heapq.heappush(heap, (st.next_run_at.timestamp(), tid))
    return heap


def load_term_state(term_states):
    """
    Should be called with a lock
    Get search term list and state info from db
    Assumes term_states contains up-to-date RATE & SCHEDULE INFO,
    but that the DB has up-to-date TERM LIST INFO
    So keep db list of terms, but keep term_state rate and next run time, where available
    """
    new_terms = load_search_terms(SEARCH_TERM_LIST_NAME)
    updated_term_id_list = [i[0] for i in new_terms]
    status = load_status_table()

    now = datetime.now(timezone.utc)
    default_last_seen = now - timedelta(days=DEFAULT_LOOKBACK_DAYS)

    # 1) update last seen on existing terms
    stale_term_ids = []
    for term_id in list(term_states):
        if term_id not in updated_term_id_list:
            stale_term_ids.append(term_id)
        else:
            db_last_seen = status.get(term_id, default_last_seen)  # both possible values are utc
            term_states[term_id].last_seen = max(term_states[term_id].last_seen, db_last_seen)

    # 2) delete stale terms
    for stale_term_id in stale_term_ids:
        del term_states[stale_term_id]

    #3 schedule all brand new terms to be 1m
    # later than the latest-scheduled existing term
    # they will all end up 1 min apart at the back of queue
    latest_next = max((ts.next_run_at for ts in term_states.values()), default=now)
    for term_id, term_name in new_terms:
        if term_id not in term_states:
            last_seen = ensure_utc(status.get(term_id, default_last_seen))
            latest_next += timedelta(minutes=1)
            rate = 0.0
            scheduled_scrape_time = latest_next
            term_states[term_id] = TermState(
                name=term_name,
                last_seen=last_seen,
                rate=rate,
                next_run_at=scheduled_scrape_time
            )

# ---------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------

def compute_next_interval_s(
        term_state: TermState,
        new_vids: list[dict]
) -> float:
    """Should be called with a lock.
    Updates term_state.rate and returns time until next scrape"""
    if not new_vids:
        return MAX_INTERVAL_S

    new_count = len(new_vids)
    span_s = publication_span_seconds(new_vids)
    inst_rate = (new_count / span_s) if span_s and span_s > 0 else 0.0

    if term_state.rate == 0.0:
        term_state.rate = inst_rate
    else:
        term_state.rate = RATE_ALPHA * inst_rate + (1 - RATE_ALPHA) * term_state.rate

    newest_seen = newest_published_dt(new_vids)
    if newest_seen:
        term_state.last_seen = newest_seen

    capacity = MAX_PAGES * RESULTS_PER_PAGE
    if term_state.rate > 0.0:
        interval_s = capacity / (term_state.rate * SCHED_BUFFER)
        return max(float(MIN_INTERVAL_S), min(float(MAX_INTERVAL_S), interval_s))

    return MAX_INTERVAL_S


def schedule_term(
    *,
    cv: threading.Condition,
    heap: list[Tuple[float, int]],
    pause: PauseState,
    term_states: Dict[int, TermState],
    term_id: int,
    interval_s: float,
) -> None:
    now_ts = datetime.now(timezone.utc).timestamp()
    resume_ts = max(now_ts + float(interval_s), pause.until_ts)

    st = term_states.get(term_id)
    if st is None:
        return

    st.next_run_at = datetime.fromtimestamp(resume_ts, tz=timezone.utc)
    heapq.heappush(heap, (resume_ts, term_id))
    cv.notify()


def pause_until_next_midnight(
    *,
    cv: threading.Condition,
    pause: PauseState,
) -> None:
    now = datetime.now(timezone.utc)
    resume_at = next_midnight_pacific(now)
    resume_ts = resume_at.timestamp()

    logging.warning("Pausing all scrapes until %s", resume_at.isoformat())

    pause.until_ts = max(pause.until_ts, resume_ts)
    cv.notify_all()

# ---------------------------------------------------------------------
# Job Queue & Main Workers
# ---------------------------------------------------------------------

def pop_next_runnable(
    *,
    cv: threading.Condition,
    stop: threading.Event,
    heap: list[Tuple[float, int]],
    term_states: Dict[int, TermState],
    pause: PauseState,
) -> tuple[float, int] | None:
    """meant to be called with a lock"""
    while not stop.is_set():
        while not heap and not stop.is_set():
            cv.wait(timeout=1.0)
        if stop.is_set():
            return None

        next_ts, term_id = heap[0]
        effective_ts = max(next_ts, pause.until_ts)

        now_ts = datetime.now(timezone.utc).timestamp()
        sleep_s = effective_ts - now_ts
        if sleep_s > 0:
            cv.wait(timeout=min(sleep_s, 5.0))
            continue

        run_at_ts, term_id = heapq.heappop(heap)
        st = term_states.get(term_id)
        if st is None:
            continue
        if abs(st.next_run_at.timestamp() - run_at_ts) > 1e-6:
            continue

        return run_at_ts, term_id

    return None


def heap_worker(
    worker_id: int,
    term_states: Dict[int, TermState],
    cv: threading.Condition,
    stop: threading.Event,
    heap: list[Tuple[float, int]],
    pause: PauseState,
    tracker: BudgetTracker,
):
    qyt = YTQuotaClient.from_api_key(tracker=tracker)

    while not stop.is_set():
        with cv:
            popped = pop_next_runnable(
                cv=cv, stop=stop, heap=heap, term_states=term_states, pause=pause
            )
            if popped is None:
                return

            run_at_ts, term_id = popped
            term_state = term_states.get(term_id)
            if term_state is None:
                continue

            published_after = term_state.last_seen
            term_name = term_state.name

        # Budget preflight: avoid starting a scrape when we can't even afford search.list
        if not qyt.can_afford("search.list"):
            with cv:
                pause_until_next_midnight(cv=cv, pause=pause)
            continue

        published_before = datetime.now(timezone.utc)

        try:
            out = scrape_window(
                qyt=qyt,
                term_name=term_name,
                published_after=published_after,
                published_before=published_before,
                max_pages=MAX_PAGES,
            )

        except (YTQuotaExceeded, YTBudgetExceeded):
            with cv:
                pause_until_next_midnight(cv=cv, pause=pause)
            continue

        except Exception:
            logging.exception("Scrape/save FAILED term_id=%s term=%r", term_id, term_name)
            with cv:
                interval_s = min(15 * 60, MAX_INTERVAL_S)  # 15m backoff
                schedule_term(
                    cv=cv,
                    heap=heap,
                    pause=pause,
                    term_states=term_states,
                    term_id=term_id,
                    interval_s=interval_s,
                )
            continue

        # --- reschedule based on results ---
        with cv:
            st = term_states.get(term_id)
            if st is None:
                continue

            # compute_next_interval_s expects "new vids" list (normalized dicts)
            interval_s = compute_next_interval_s(st, out.new_vids)
            schedule_term(
                cv=cv,
                heap=heap,
                pause=pause,
                term_states=term_states,
                term_id=term_id,
                interval_s=interval_s,
            )

            # capture these while locked so the log below is consistent
            next_run_at = st.next_run_at
            rate = st.rate

        now_ts = datetime.now(timezone.utc).timestamp()
        logging.info(
            "Term done term=%r: found=%d inserted_v=%d skipped_v=%d inserted_c=%d skipped_c=%d rate=%.4f next_in=%.0fs",
            term_name,
            out.found_v,
            out.ins_v,
            out.skip_v,
            out.ins_c,
            out.skip_c,
            rate,
            (next_run_at.timestamp() - now_ts),
        )

def term_list_refresh_worker(
    term_states: Dict[int, TermState],
    cv: threading.Condition,
    stop: threading.Event,
    heap: list[Tuple[float, int]],
    interval_s: int = 3600
):
    """ update db with term states and sync term state list with db"""
    while not stop.is_set():
        # Sleep in small chunks so stop can interrupt promptly
        for _ in range(max(1, interval_s)):
            if stop.is_set():
                return
            time.sleep(1)

        try:
            with cv:
                load_term_state(term_states)
                # rebuild heap to remove any stale terms
                heap[:] = build_heap(term_states)
                update_all_term_statuses(term_states)
                cv.notify_all()

        except Exception:
            logging.exception("refresh worker error")


def monitor_loop():
    tracker = BudgetTracker(budget_units_per_day=TOTAL_BUDGET_UNITS_PER_DAY)

    term_states: Dict[int, TermState] = {}
    logging.info("=== MONITOR LOOP BEGINNING ===")

    while not term_states:
        load_term_state(term_states)
        if not term_states:
            logging.warning("No terms found for search term list: %s", SEARCH_TERM_LIST_NAME)
            time.sleep(600)

    heap = build_heap(term_states)

    lock = threading.Lock()
    cv = threading.Condition(lock)
    stop = threading.Event()
    pause = PauseState()

    scrape_threads: list[threading.Thread] = []
    for i in range(NUM_SCRAPE_WORKERS):
        t = threading.Thread(
            target=heap_worker,
            name=f"yt_scrape_worker_{i}",
            args=(i, term_states, cv, stop, heap, pause, tracker),
            daemon=True,
        )
        scrape_threads.append(t)
        t.start()

    t_refresh = threading.Thread(
        target=term_list_refresh_worker,
        name="term_list_refresh_worker",
        args=(term_states, cv, stop, heap, 3600),
        daemon=True,
    )
    t_refresh.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("Stopping workers...")
        stop.set()
        with cv:
            cv.notify_all()
        for t in scrape_threads:
            t.join(timeout=5)
        t_refresh.join(timeout=5)


def main(prod=False):
    logging.info("Refreshing search terms and status")
    if prod:    
        init_pool(prefix="prod")
    else:
        init_pool(prefix="dev")
    try:
        monitor_loop()
    finally:
        close_pool()


if __name__ == "__main__":
    main()
