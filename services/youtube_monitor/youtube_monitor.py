"""
yt_monitor.py

Long-running YouTube monitor.

Responsibilities:
- load search terms (by list name)
- load per-term status (last seen published_at)
- periodically refresh both
- scrape YouTube using yt_scrape.py
- handle quota exhaustion cleanly
- update status db table
"""

from __future__ import annotations

import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Iterable, Tuple, Any
from dataclasses import dataclass, field
import heapq
import threading
import pytz

from db.db import getcursor, init_pool, close_pool
from ingestion.ingestion import ensure_scrape_job
from ingestion.youtube_video import flush_youtube_video_batch
from ingestion.youtube_comment import flush_youtube_comment_batch

from .yt import youtube_client
from .yt_scrape import (
    iter_videos,
    fetch_comment_threads,
    YTQuotaExceeded,
)

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

SEARCH_TERM_LIST_NAME = "core_search_terms"

MAX_PAGES = 2                     # pages per scrape per term
MIN_COMMENTS_FOR_SCRAPE = 50       # only fetch comments if >= this many
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
# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------------------------------------------------------------------
# DB access (to be implemented)
# ---------------------------------------------------------------------


def load_search_terms(list_name: str) -> List[tuple[int, str]]:
    """
    Load (term_id, term_name) belonging to SEARCH_TERM_LIST_NAME.
    """
    with getcursor() as cur:
        cur.execute(
            """
            SELECT t.id, t.name
            FROM taxonomy.vaccine_term_subset s
            JOIN taxonomy.vaccine_term_subset_member m
              ON m.subset_id = s.id
            JOIN taxonomy.vaccine_term t
              ON t.id = m.term_id
            WHERE s.name = %s
            ORDER BY t.name
            """,
            (list_name,),
        )
        return [(int(row[0]), row[1]) for row in cur.fetchall()]


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
            out[int(term_id)] = utc_aware(ts)
    return out


def update_all_term_statuses(term_states: Dict[int, "TermState"]) -> None:
    """
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


def count_found_videos(pages: list[dict]) -> int:
    return sum(len(p.get("videos") or []) for p in pages)


def summarize_stop_reasons(pages: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for p in pages:
        r = p.get("stopped_reason")
        if r:
            out[r] = out.get(r, 0) + 1
    return out


def sample_video_debug(videos: list[dict], limit: int = 3) -> list[dict[str, Any]]:
    """
    Return a tiny sample of fields useful for debugging insert failures.
    """
    out: list[dict[str, Any]] = []
    for v in videos[:limit]:
        out.append({
            "video_id": v.get("video_id"),
            "created_at_ts": v.get("created_at_ts"),
            "channel_id": v.get("channel_id"),
            "title": (v.get("title")[:80] + "...") if isinstance(v.get("title"), str) and len(v["title"]) > 80 else v.get("title"),
            "keys": sorted(list(v.keys()))[:30],
        })
    return out


def save_videos(videos: List[dict], *, term_name: str) -> tuple[int, int, set[str]]:
    """
    Persist normalized videos via ingestion layer.
    Returns (inserted, skipped, inserted_video_ids).
    """
    if not videos:
        return 0, 0, set()

    job_id = ensure_scrape_job(
        name=f"youtube monitor: {term_name}",
        description=(
            f"Continuous YouTube monitor scrape for term "
            f"{term_name!r}"
        ),
        platforms=["youtube_video"],
    )

    try:
        inserted, skipped, inserted_ids = flush_youtube_video_batch(
            rows=videos,
            job_id=job_id,
        )
        logging.info(
            "save_videos term=%r: attempted=%d inserted=%d skipped=%d",
            term_name, len(videos), inserted, skipped,
        )
        return inserted, skipped, inserted_ids

    except Exception:
        # High-signal summary (don’t dump whole rows)
        logging.exception(
            "save_videos FAILED term=%r: attempted=%d sample=%s",
            term_name,
            len(videos),
            sample_video_debug(videos),
        )

        # Optional: try to isolate the failing row quickly.
        # This is extremely useful when a single malformed row breaks the batch.
        for idx, row in enumerate(videos[:10]):
            try:
                flush_youtube_video_batch(rows=[row], job_id=job_id)
            except Exception:
                logging.exception(
                    "save_videos single-row probe FAILED term=%r idx=%d video_id=%r keys=%s",
                    term_name,
                    idx,
                    row.get("video_id"),
                    sorted(list(row.keys())),
                )
                break

        # Propagate so heap_worker can reschedule safely and you see failure clearly.
        raise


def save_comments(comments: List[dict], *, term_name: str) -> tuple[int, int, set[tuple[str, str]]]:
    """
    Persist normalized comments via ingestion layer.
    Returns (inserted, skipped, inserted_comment_ids which is (video_id, comment_id)).
    """
    if not comments:
        return 0, 0, set()

    job_id = ensure_scrape_job(
        name=f"youtube comments monitor: {term_name}",
        description=f"YouTube comment scrape for term {term_name!r}",
        platforms=["youtube_comment"],
    )

    # If you updated comment flush to return inserted keys, you can ignore them here.
    ins, skipped, inserted_ids = flush_youtube_comment_batch(
        rows=comments, job_id=job_id)
    return ins, skipped, inserted_ids

# ---------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------


PACIFIC = pytz.timezone("US/Pacific")


def utc_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def next_midnight_pacific(now_utc: datetime) -> datetime:
    pt = now_utc.astimezone(PACIFIC)
    tomorrow = pt.date() + timedelta(days=1)
    midnight_pt = PACIFIC.localize(
        datetime.combine(tomorrow, datetime.min.time()))
    return midnight_pt.astimezone(timezone.utc)


# ---------------------------------------------------------------------
# Scraper interface
# ---------------------------------------------------------------------

def save_all_videos_on_pages(pages, term_name):
    """
    Loop over pages, save vids to DB, return:
      (newly_inserted_videos, inserted_count, skipped_count)
    """
    new_vids: list[dict] = []
    inserted_total = 0
    skipped_total = 0

    for page in pages:
        vids = page["videos"] or []
        if not vids:
            reason = page.get("stopped_reason")
            if reason:
                logging.info("Stopped reason for term %r: %s",
                             term_name, reason)
            else:
                logging.warning("No videos found on search page.")
            continue

        inserted, skipped, inserted_ids = save_videos(
            vids, term_name=term_name)
        inserted_total += inserted
        skipped_total += skipped

        if inserted_ids:
            new_vids.extend(
                [v for v in vids if v.get("video_id") in inserted_ids])
    return new_vids, inserted_total, skipped_total


def publication_span_seconds(videos: Iterable[dict]) -> float:
    dts: list[datetime] = []
    for v in videos:
        dt = v.get("created_at_ts")
        if isinstance(dt, datetime):
            dts.append(dt)
    if len(dts) < 2:
        return 0.0
    dts.sort()
    return (dts[-1] - dts[0]).total_seconds()


def newest_published_dt(videos: list[dict]) -> datetime | None:
    newest: datetime | None = None
    for v in videos:
        dt = v.get("created_at_ts")
        if not isinstance(dt, datetime):
            continue
        # dt should already be UTC-aware if produced by clean_created_at_ts
        if newest is None or dt > newest:
            newest = dt
    return newest


def save_comments_on_videos(yt, videos, term_name):
    """
    Scrape comments only for *newly inserted* videos meeting threshold.
    Returns (comments_inserted, inserted_comments, skipped_comments)
    """
    new_comments: list[dict] = []
    inserted_total = 0
    skipped_total = 0

    for v in videos:
        if (v.get("comment_count") or 0) < MIN_COMMENTS_FOR_SCRAPE:
            continue

        comments, _ = fetch_comment_threads(
            yt,
            video_id=v["video_id"],
            max_threads=100,
            order="relevance",
        )
        if not comments:
            continue

        ins, skip, inserted_ids = save_comments(comments, term_name=term_name)

        if inserted_ids:
            new_comments.extend(
                [c for c in comments if (c.get("video_id"), c.get("comment_id")) in inserted_ids])
        inserted_total += ins
        skipped_total += skip

    return new_comments, inserted_total, skipped_total


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
    Get search term list and state info from db
    Assumes term_states contains up-to-date RATE & SCHEDULE INFO,
    but that the DB has up-to-date TERM LIST INFO
    So keep db list of terms, but keep term_state rate and next run time, where available
    """
    old_term_states = term_states.copy()

    terms = load_search_terms(SEARCH_TERM_LIST_NAME)
    status = load_status_table()

    now = datetime.now(timezone.utc)
    default_last_seen = now - timedelta(days=DEFAULT_LOOKBACK_DAYS)

    # copy all existing rate data into new term_states
    for term_id, term_name in terms:
        last_seen = status.get(term_id, default_last_seen)

        if term_id in old_term_states:
            rate = old_term_states[term_id].rate
            scheduled_scrape_time = old_term_states[term_id].next_run_at

            term_states[term_id] = TermState(
                name=term_name,
                last_seen=last_seen,
                rate=rate,
                next_run_at=scheduled_scrape_time
            )

    # schedule all brand new terms to be 1m
    # later than the latest-scheduled existing term
    # they will all end up 1 min apart at the back of queue
    for term_id, term_name in terms:
        if term_id not in old_term_states:
            latest_next = max(
                (ts.next_run_at for ts in term_states.values()),
                default=now,
            )
            rate = 0.0
            scheduled_scrape_time = latest_next + timedelta(minutes=1)

            term_states[term_id] = TermState(
                name=term_name,
                last_seen=last_seen,
                rate=rate,
                next_run_at=scheduled_scrape_time
            )

    # remove any stale terms
    new_term_ids = [t[0] for t in terms]
    for term_id in list(term_states.keys()):
        if term_id not in new_term_ids:
            term_states.pop(term_id, None)

# ---------------------------------------------------------------------
# Main monitor
# ---------------------------------------------------------------------


def heap_worker(
    yt,
    term_states: Dict[int, TermState],
    cv: threading.Condition,
    stop: threading.Event,
    heap: list[Tuple[float, int]],
    pause: PauseState
):
    while not stop.is_set():
        with cv:
            while not heap and not stop.is_set():
                cv.wait(timeout=1.0)

            if stop.is_set():
                logging.info("Heap worker detected stop; quitting worker")
                return

            next_ts, term_id = heap[0]
            effective_ts = max(next_ts, pause.until_ts)

            now_ts = datetime.now(timezone.utc).timestamp()
            sleep_s = effective_ts - now_ts

            if sleep_s > 0:
                logging.debug("Heap worker waiting %.0fs", sleep_s)
                cv.wait(timeout=min(sleep_s, 5.0))
                continue

            run_at_ts, term_id = heapq.heappop(heap)
            ts = term_states.get(term_id)
            if ts is None:
                logging.debug(
                    "popped term_id=%s but term removed; skipping", term_id)
                continue
            if abs(ts.next_run_at.timestamp() - run_at_ts) > 1e-6:
                logging.debug(
                    "popped term_id=%s appears to be stale; skipping.", term_id)
                continue

        # Defaults so bookkeeping always works
        pages: list[dict] = []
        found_v = 0
        new_vids: list[dict] = []
        new_comments: list[dict] = []
        ins_v = skip_v = 0
        ins_c = skip_c = 0
        term_name = "<unknown>"
        term_state: TermState | None = None

        try:
            with cv:
                term_state = term_states.get(term_id)
                if term_state is None:
                    continue
                published_after = term_state.last_seen.isoformat()
                published_before = datetime.now(timezone.utc).isoformat()
                term_name = term_state.name

            logging.info(
                "Scrape start term=%r term_id=%s window=[%s, %s] heap_run_at=%s",
                term_name,
                term_id,
                published_after,
                published_before,
                datetime.fromtimestamp(run_at_ts, tz=timezone.utc).isoformat(),
            )

            pages = list(iter_videos(
                yt,
                term_name=term_name,
                region=None,
                published_after=published_after,
                published_before=published_before,
                max_pages=MAX_PAGES,
            ))

            found_v = count_found_videos(pages)
            stops = summarize_stop_reasons(pages)

            logging.info(
                "Scrape fetched term=%r: pages=%d found_videos=%d stops=%s",
                term_name,
                len(pages),
                found_v,
                stops if stops else "{}",
            )

            # Save videos (this now logs attempted/inserted/skipped and raises on failure)
            new_vids, ins_v, skip_v = save_all_videos_on_pages(
                pages, term_name)

            logging.info(
                "Video save summary term=%r: found=%d inserted=%d skipped=%d new_for_comments=%d",
                term_name,
                found_v,
                ins_v,
                skip_v,
                len(new_vids),
            )

            # Save comments on newly inserted videos
            new_comments, ins_c, skip_c = save_comments_on_videos(
                yt, new_vids, term_name)

            logging.info(
                "Comment save summary term=%r: candidate_videos=%d inserted=%d skipped=%d new=%d",
                term_name,
                len(new_vids),
                ins_c,
                skip_c,
                len(new_comments),
            )

        except YTQuotaExceeded:
            now = datetime.now(timezone.utc)
            resume_at = next_midnight_pacific(now)
            resume_ts = resume_at.timestamp()

            logging.warning(
                "Quota exceeded. Pausing all scrapes until %s",
                resume_at.isoformat(),
            )

            with cv:
                pause.until_ts = max(pause.until_ts, resume_ts)
                heapq.heappush(heap, (resume_ts, term_id))
                cv.notify_all()
            continue

        except Exception:
            # This captures video insert failures too (since save_videos re-raises)
            logging.exception(
                "Scrape/save FAILED term=%r term_id=%s", term_name, term_id)
            # Backoff this term to avoid tight failure loops
            now_ts = datetime.now(timezone.utc).timestamp()
            with cv:
                if term_id in term_states:
                    interval_s = min(15 * 60, MAX_INTERVAL_S)  # 15m backoff
                    resume_ts = max(now_ts + interval_s, pause.until_ts)
                    term_state = term_states[term_id]
                    term_state.next_run_at = datetime.fromtimestamp(
                        resume_ts, tz=timezone.utc)
                    heapq.heappush(heap, (resume_ts, term_id))
                    cv.notify()
            continue

        # --- Bookkeeping / reschedule ---
        now_ts = datetime.now(timezone.utc).timestamp()

        if term_state is None:
            continue

        if new_vids:
            new_count = len(new_vids)
            span_s = publication_span_seconds(new_vids)
            inst_rate = (new_count / span_s) if span_s and span_s > 0 else 0.0

            newest_seen = newest_published_dt(new_vids)

            with cv:
                if term_state.rate == 0.0:
                    term_state.rate = inst_rate
                else:
                    term_state.rate = RATE_ALPHA * inst_rate + \
                        (1 - RATE_ALPHA) * term_state.rate

                if newest_seen:
                    term_state.last_seen = newest_seen

                capacity = MAX_PAGES * RESULTS_PER_PAGE
                if term_state.rate > 0.0:
                    interval_s = capacity / (term_state.rate * SCHED_BUFFER)
                    interval_s = max(MIN_INTERVAL_S, min(
                        MAX_INTERVAL_S, interval_s))
                else:
                    interval_s = MAX_INTERVAL_S

                resume_ts = max(now_ts + interval_s, pause.until_ts)
                term_state.next_run_at = datetime.fromtimestamp(
                    resume_ts, tz=timezone.utc)
                heapq.heappush(heap, (resume_ts, term_id))
                cv.notify()

        else:
            with cv:
                if term_id not in term_states:
                    continue
                interval_s = MAX_INTERVAL_S
                resume_ts = max(now_ts + interval_s, pause.until_ts)
                term_state.next_run_at = datetime.fromtimestamp(
                    resume_ts, tz=timezone.utc)
                heapq.heappush(heap, (resume_ts, term_id))
                cv.notify()

        logging.info(
            "Term done term=%r: found=%d inserted_v=%d skipped_v=%d inserted_c=%d skipped_c=%d rate=%.4f next_in=%.0fs",
            term_name,
            found_v,
            ins_v,
            skip_v,
            ins_c,
            skip_c,
            term_state.rate,
            (term_state.next_run_at.timestamp() - now_ts),
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
                heap[:] = build_heap(term_states)
                update_all_term_statuses(term_states)
                cv.notify_all()

        except Exception:
            logging.exception("refresh worker error")


def monitor_loop():
    yt = youtube_client()
    term_states: Dict[int, TermState] = {}
    logging.info("=== MONITOR LOOP BEGINNING ===")
    while not term_states:
        load_term_state(term_states)  # reference db for up-to-date info
        if not term_states:
            logging.warning(
                f"No terms found for search term list:"
                f"{SEARCH_TERM_LIST_NAME}"
            )
            time.sleep(600)  # check again in 10m

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
            args=(yt, term_states, cv, stop, heap, pause),
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
