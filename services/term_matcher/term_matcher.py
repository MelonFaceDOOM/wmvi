from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple

from dotenv import load_dotenv

from db.db import getcursor

from .queries import (
    check_indices,
    get_latest_post_registry_id,
    get_or_init_term_state,
    get_terms_by_ids,
    get_terms_by_names,
    get_vaccine_terms,
    find_post_ids_for_term_range,
    insert_post_term_matches,
    update_term_state,
)

load_dotenv()

# Name for this matcher implementation; persisted in both state + matches table.
MATCHER_VERSION = "tsv_en_plainto_v1"

# Timing knobs (can be tuned / made env-driven)
PER_TERM_SLEEP_SECONDS = 0.05   # small pause between terms
LOOP_SLEEP_SECONDS = 60.0       # pause between full passes over all terms


class TermMatcher:
    """
    Term matcher against sm.post_search_en, driven by matches.term_match_state.

    The core unit is `_process_single_term`, which:
      - Locates the cursor (last_checked_post_id) for (term_id, matcher_version).
      - Finds new posts in (cursor, max_post_id].
      - Inserts matches into matches.post_term_match.
      - Advances cursor to max_post_id.

    Public entrypoints:
      - run_loop()      : infinite loop, pass over all terms each cycle.
      - run_once()      : one pass over all terms, then return.
      - run_for_term_ids([...])
      - run_for_term_names([...])
    """

    def __init__(
        self,
        matcher_version: str = MATCHER_VERSION,
        per_term_sleep_seconds: float = PER_TERM_SLEEP_SECONDS,
        loop_sleep_seconds: float = LOOP_SLEEP_SECONDS,
    ) -> None:
        self.matcher_version = matcher_version
        self.per_term_sleep_seconds = per_term_sleep_seconds
        self.loop_sleep_seconds = loop_sleep_seconds
        self._indices_checked = False

    # --------- core loops ---------

    def run_loop(self) -> None:
        """
        Blocking main loop. Intended to run 24/7 under a process supervisor.
        """
        log = logging.getLogger(__name__)
        log.info(
            "TermMatcher loop starting (matcher_version=%s)...",
            self.matcher_version,
        )

        self._check_indices_once()

        while True:
            loop_start = time.time()

            # Load all terms at the start of the pass
            # Terms added while running will be found on next pass
            with getcursor() as cur:
                terms = get_vaccine_terms(cur)

            if not terms:
                log.info(
                    "No vaccine terms found; sleeping %.1fs",
                    self.loop_sleep_seconds,
                )
                time.sleep(self.loop_sleep_seconds)
                continue

            log.info("Starting pass over %d terms.", len(terms))
            self._run_pass_over_terms(terms)

            elapsed = time.time() - loop_start
            log.info(
                "Completed pass over %d terms in %.1fs; sleeping %.1fs",
                len(terms),
                elapsed,
                self.loop_sleep_seconds,
            )
            time.sleep(self.loop_sleep_seconds)

    def run_once(self) -> None:
        """
        Run a single pass over all terms, then return.
        Useful for CLI-driven, non-daemon runs.
        """
        log = logging.getLogger(__name__)
        self._check_indices_once()

        with getcursor() as cur:
            terms = get_vaccine_terms(cur)

        if not terms:
            log.info("No vaccine terms found; nothing to process.")
            return

        log.info("Running single pass over %d terms.", len(terms))
        self._run_pass_over_terms(terms)

    def run_for_term_ids(self, term_ids: Iterable[int]) -> None:
        """
        Run matcher for a specific set of term IDs (one pass, then return).
        """
        log = logging.getLogger(__name__)
        self._check_indices_once()

        term_ids_list = [int(tid) for tid in term_ids]
        if not term_ids_list:
            log.info("No term IDs provided; nothing to process.")
            return

        with getcursor() as cur:
            terms = get_terms_by_ids(cur, term_ids_list)

        if not terms:
            log.warning("No matching terms found for IDs: %r", term_ids_list)
            return

        log.info("Running matcher for %d specific terms (by id).", len(terms))
        self._run_pass_over_terms(terms)

    def run_for_term_names(self, term_names: Iterable[str]) -> None:
        """
        Run matcher for a specific set of term names (one pass, then return).
        """
        log = logging.getLogger(__name__)
        self._check_indices_once()

        names_list = [t.strip() for t in term_names if t and t.strip()]
        if not names_list:
            log.info("No term names provided; nothing to process.")
            return

        with getcursor() as cur:
            terms = get_terms_by_names(cur, names_list)

        if not terms:
            log.warning("No matching terms found for names: %r", names_list)
            return

        log.info("Running matcher for %d specific terms (by name).", len(terms))
        self._run_pass_over_terms(terms)

    # --------- internal pass driver ---------

    def _run_pass_over_terms(self, terms: List[Tuple[int, str]]) -> None:
        """
        Iterate over the provided list of (term_id, term_name) and process each once.
        """
        log = logging.getLogger(__name__)
        for term_id, term_name in terms:
            try:
                self._process_single_term(term_id, term_name)
            except Exception:
                log.exception(
                    "Error while processing term_id=%s name=%r",
                    term_id,
                    term_name,
                )
            if self.per_term_sleep_seconds > 0:
                time.sleep(self.per_term_sleep_seconds)

    # --------- per-term processing ---------

    def _process_single_term(self, term_id: int, term_name: str) -> None:
        """
        Process one term:

        - Look up or create its term_match_state.
        - Determine new post_registry.id range to scan.
        - Search for matches in that range.
        - Insert matches and advance cursor.
        """
        log = logging.getLogger(__name__)

        with getcursor() as cur:
            # 1) Load cursor and max id
            last_checked = get_or_init_term_state(cur, term_id, self.matcher_version)
            min_post_id = int(last_checked or 0)

            max_post_id = get_latest_post_registry_id(cur)

            if max_post_id <= min_post_id:
                # Nothing new to scan; just bump last_run_at
                update_term_state(cur, term_id, self.matcher_version, min_post_id)
                log.debug(
                    "Term %d (%r): up-to-date (cursor=%d, max_id=%d)",
                    term_id,
                    term_name,
                    min_post_id,
                    max_post_id,
                )
                return

            # 2) Find matches in (min_post_id, max_post_id]
            post_ids = find_post_ids_for_term_range(
                cur,
                term_name,
                min_post_id=min_post_id,
                max_post_id=max_post_id,
            )

            inserted = 0
            if post_ids:
                # Ensure deterministic order for logging/debug; DB doesn't care.
                sorted_ids = sorted(post_ids)
                inserted = insert_post_term_matches(
                    cur,
                    term_id=term_id,
                    matcher_version=self.matcher_version,
                    post_ids=sorted_ids,
                )

            # 3) Advance cursor to max_post_id and update last_run_at
            update_term_state(cur, term_id, self.matcher_version, max_post_id)

        log.info(
            "Term %d (%r): scanned posts (%d, %d], matched=%d, inserted=%d",
            term_id,
            term_name,
            min_post_id,
            max_post_id,
            len(post_ids),
            inserted,
        )

    # --------- helpers ---------

    def _check_indices_once(self) -> None:
        """
        Run the index check once per TermMatcher instance.
        """
        if self._indices_checked:
            return

        log = logging.getLogger(__name__)
        log.info("Checking expected GIN indices for term matcher...")
        with getcursor() as cur:
            check_indices(cur)
        self._indices_checked = True


# --------- logging helpers (reused by CLI) ---------


def setup_logging() -> None:
    """
    Mirror reddit_monitor style logging:

    - logs/term_matcher_YYYYmmdd_HHMMSS.log
    - log to both console and file.
    """
    os.makedirs("logs", exist_ok=True)
    log_path = Path("logs") / f"term_matcher_{datetime.now():%Y%m%d_%H%M%S}.log"

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


if __name__ == "__main__":
    # Keep this as a thin shim to the CLI.
    from db.db import init_pool, close_pool
    from .cli import main as cli_main

    setup_logging()
    init_pool()  # Uses env DEFAULT_DB / prefix
    try:
        cli_main()
    finally:
        close_pool()
