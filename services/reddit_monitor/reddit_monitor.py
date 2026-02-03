from __future__ import annotations

import csv
import heapq
import os
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple
import prawcore

from dotenv import load_dotenv

from db.db import init_pool, getcursor

from .queries import get_effective_term_list
from .scrape_runner import scrape_term_once

load_dotenv()

# Scrape frequency tuning
MULTIPLIER = 4          # buffer factor: scrape a bit more often than observed
MIN_SCRAPES_PER_DAY = 4
MAX_SCRAPES_PER_DAY = 500
SECONDS_PER_DAY = 86_400

# Metadata CSV: one line per term
#   term,scrapes_per_day
METADATA_PATH = Path(__file__).with_name("monitor_metadata.csv")


def _load_metadata(path: Path = METADATA_PATH) -> Dict[str, float]:
    """
    Load scrapes_per_day for each term from CSV, if it exists.

    File format:
        term,scrapes_per_day
        covid vaccine,4.0
        pfizer,12.5
    """
    if not path.exists():
        return {}

    rates: Dict[str, float] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            # Skip header or commented lines
            if row[0].strip().startswith("#"):
                continue
            if row[0].strip().lower() == "term":
                continue
            if len(row) < 2:
                continue
            term = row[0].strip()
            try:
                rate = float(row[1])
            except ValueError:
                continue
            rates[term] = rate
    return rates


def _save_metadata(term_rates: Dict[str, float], path: Path = METADATA_PATH) -> None:
    """
    Persist scrapes_per_day for each term.

    Overwrites the file on each call.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["term", "scrapes_per_day"])
        for term in sorted(term_rates.keys()):
            writer.writerow([term, f"{term_rates[term]:.6f}"])


class ScrapeScheduler:
    """
    Core scheduler:

    - At startup:
        * Fetches all search terms (taxonomy.vaccine_term) via get_effective_term_list.
        * Loads per-term scrape rates from monitor_metadata.csv, if present.
        * Converts scrapes_per_day -> intervals and schedules each term on a min-heap.
    - In the loop:
        * Pops next due term.
        * If due, runs a scrape for that term (in a thread pool),
          updates its rate based on observed new results, and re-schedules it.
        * If not yet due, sleeps until it is (or for a short time).
    """

    def __init__(self, max_workers: int = 2) -> None:
        self.lock = threading.Lock()
        self.task_heap: List[Tuple[float, str]] = []  # (next_scrape_ts, term)
        self.task_set: set[str] = set()
        self.max_workers = max_workers

        from concurrent.futures import ThreadPoolExecutor

        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # Per-term scraped-based parameters kept in memory and mirrored to CSV.
        self.term_rates: Dict[str, float] = {}      # term -> scrapes_per_day
        self.term_intervals: Dict[str, float] = {}  # term -> interval_seconds

        self._fatal_error = False  # guard against 2 threads trying to crash at once

        self._setup_initial_schedule()

    # --------- scheduling primitives ---------

    def _add_task(self, term: str, scrape_time: float) -> None:
        """Add term to heap if not already scheduled."""
        with self.lock:
            if term in self.task_set:
                return
            heapq.heappush(self.task_heap, (scrape_time, term))
            self.task_set.add(term)

    def _get_last_interval(self, term: str) -> float:
        """Return the last interval used for this term (seconds)."""
        with self.lock:
            return self.term_intervals.get(
                term,
                SECONDS_PER_DAY / float(MIN_SCRAPES_PER_DAY),
            )

    def _set_rate_and_interval(self, term: str, scrapes_per_day: float) -> float:
        """
        Clamp scrapes_per_day, compute interval, store both, and persist metadata.

        Returns the new interval in seconds.
        """
        rate = max(MIN_SCRAPES_PER_DAY, min(
            MAX_SCRAPES_PER_DAY, scrapes_per_day))
        interval = SECONDS_PER_DAY / rate

        with self.lock:
            self.term_rates[term] = rate
            self.term_intervals[term] = interval

        _save_metadata(self.term_rates)
        return interval

    def _setup_initial_schedule(self) -> None:
        """
        Called once at init.

        - Loads all effective terms from DB.
        - Reads saved scrapes_per_day from CSV if present.
        - For each term:
            * If rate exists in CSV -> use it.
            * Otherwise -> use MIN_SCRAPES_PER_DAY.
        - Staggers start times over each term's interval.
        """
        logging.info("ScrapeScheduler setup: loading terms and metadata...")
        now = time.time()

        # 1) Load terms from DB
        with getcursor() as cur:
            terms = get_effective_term_list(cur)

        logging.info("Found %d effective terms.", len(terms))

        # 2) Load persisted rates
        saved_rates = _load_metadata()
        logging.info("Loaded %d term rates from metadata.", len(saved_rates))

        # 3) Set up internal rate/interval state
        term_intervals: List[Tuple[str, float]] = []
        for term in terms:
            if term in saved_rates:
                rate = saved_rates[term]
            else:
                rate = float(MIN_SCRAPES_PER_DAY)

            interval = self._set_rate_and_interval(term, rate)
            term_intervals.append((term, interval))

        # 4) Sort by interval so we schedule higher-traffic terms first
        term_intervals.sort(key=lambda x: x[1])

        n = len(term_intervals) or 1
        for idx, (term, interval) in enumerate(term_intervals):
            spacing_offset = (interval / n) * idx
            next_scrape_time = now + spacing_offset
            pretty = datetime.fromtimestamp(next_scrape_time, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M"
            )
            logging.info(
                "Initial scrape time set for %r at %s (interval ~%.1fs)",
                term,
                pretty,
                interval,
            )
            self._add_task(term, next_scrape_time)

    # --------- main loop ---------
    def _handle_worker_result(self, future):
        exc = future.exception()
        if exc and not self._fatal_error:
            self._fatal_error = True
            logging.error("Worker crashed", exc_info=exc)
            os._exit(1)

    def scrape_loop(self) -> None:
        """
        Blocking main loop: continuously checks heap and runs scrapes when due.
        """
        logging.info("ScrapeScheduler loop started.")
        while True:
            with self.lock:
                if not self.task_heap:
                    time.sleep(1.0)
                    continue

                next_time, term = heapq.heappop(self.task_heap)
                self.task_set.remove(term)

            # spacing between scrape launches
            time.sleep(5.0)

            now = time.time()
            if next_time <= now:
                # Due now: run scrape in worker pool
                f = self.executor.submit(self._scrape_and_reschedule, term)
                f.add_done_callback(self._handle_worker_result)
            else:
                # Not yet due; reinsert and sleep until it's time.
                self._add_task(term, next_time)
                sleep_duration = max(0.0, next_time - now)
                logging.info(
                    "Next scrape for %r not due yet (sleeping %.1fs)",
                    term,
                    sleep_duration,
                )
                time.sleep(sleep_duration)

    # --------- per-term scrape ---------

    def _scrape_and_reschedule(self, term: str) -> None:
        """
        Perform a reddit scrape for `term`, update its scrape rate based on
        observed new results, then schedule the next run.

        Rate update heuristic:
          - Let n = new submissions returned this cycle.
          - Let dt = last scheduled interval for this term (approx time since last scrape).
          - If n == 0 -> scrapes_per_day = MIN_SCRAPES_PER_DAY.
          - Else:
                subs_per_day = (n / dt) * SECONDS_PER_DAY
                raw_scrapes_per_day = MULTIPLIER * (subs_per_day / 250)
                clamp between MIN_SCRAPES_PER_DAY and MAX_SCRAPES_PER_DAY.
        """
        logging.info(
            "[%s] Scraping term: %r",
            datetime.now(timezone.utc).isoformat(),
            term,
        )

        last_interval = self._get_last_interval(term)

        try:
            # scrape_term_once should return the number of *new* submissions inserted
            new_count = scrape_term_once(term) or 0

        except prawcore.exceptions.TooManyRequests as e:
            # Do not drop the term; back off and reschedule.
            logging.warning(
                "Rate limit from Reddit for term %r: %s. Applying backoff and rescheduling.",
                term,
                e,
            )
            min_interval = SECONDS_PER_DAY / MAX_SCRAPES_PER_DAY
            max_interval = SECONDS_PER_DAY / MIN_SCRAPES_PER_DAY  # 6h when MIN=4

            base_interval = max(last_interval, min_interval)
            backoff_interval = min(base_interval * 2.0, max_interval)
            next_scrape = time.time() + backoff_interval
            logging.info(
                "Backoff for %r: next scrape in ~%.1fs",
                term,
                backoff_interval,
            )
            self._add_task(term, next_scrape)
            return

        except Exception as e:
            logging.exception("Scrape failed for term %r: %s", term, e)
            new_count = 0

        # ------------------------
        # Compute new rate
        # ------------------------
        if new_count <= 0:
            scrapes_per_day = float(MIN_SCRAPES_PER_DAY)
        else:
            dt = max(last_interval, 1.0)  # avoid divide-by-zero
            subs_per_day = (new_count / dt) * SECONDS_PER_DAY
            raw_scrapes_per_day = MULTIPLIER * (subs_per_day / 250.0)
            scrapes_per_day = max(
                float(MIN_SCRAPES_PER_DAY),
                min(float(MAX_SCRAPES_PER_DAY), raw_scrapes_per_day),
            )

        new_interval = self._set_rate_and_interval(term, scrapes_per_day)
        next_scrape = time.time() + new_interval

        logging.info(
            "Updated rate for %r: %.2f scrapes/day (interval ~%.1fs, new_count=%d)",
            term,
            self.term_rates[term],
            new_interval,
            new_count,
        )

        self._add_task(term, next_scrape)


def _setup_logging() -> None:
    os.makedirs("logs", exist_ok=True)
    log_path = f"logs/reddit_monitor_{datetime.now():%Y%m%d_%H%M%S}.log"

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(fmt)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(console)
    root.addHandler(file_handler)


def main(prod=False):
    _setup_logging()
    if prod:
        init_pool(prefix="prod")
    else:
        init_pool(prefix="dev")
    scheduler = ScrapeScheduler()
    try:
        scheduler.scrape_loop()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received, shutting down scheduler...")
    finally:
        # Make sure we stop worker threads so the process can exit
        scheduler.executor.shutdown(wait=False)
        logging.info("Executor shut down; exiting.")


if __name__ == "__main__":
    main()
