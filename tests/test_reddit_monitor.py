from __future__ import annotations

import os
import pytest
from db.db import getcursor

# the modules being tested
from services.reddit_monitor import reddit_monitor, scrape_runner, queries

from dotenv import load_dotenv
load_dotenv()

@pytest.fixture(scope="session")
def require_reddit_env():
    """Skip Reddit tests unless REddit creds are set."""
    needed = ["REDDIT_ID", "REDDIT_SECRET"]
    missing = [k for k in needed if not os.environ.get(k)]
    if missing:
        pytest.skip(f"Missing Reddit credentials: {', '.join(missing)}")

@pytest.fixture
def scrape_term_once_limited(monkeypatch):
    """Patch & run scrape_term_once, returning how many submissions were yielded."""
    def _run(term: str, n: int = 2) -> int:
        original = scrape_runner.get_submissions_until_duplicate

        yielded = {"count": 0}

        def limited_get_submissions(reddit, query_str, existing_submission_ids=None):
            for sub in original(reddit, query_str, existing_submission_ids):
                if yielded["count"] >= n:
                    break
                yielded["count"] += 1
                yield sub

        monkeypatch.setattr(
            scrape_runner,
            "get_submissions_until_duplicate",
            limited_get_submissions,
        )

        scrape_runner.scrape_term_once(term)
        return yielded["count"]

    return _run
    
# -----------------------
# Tests for scrape_runner
# -----------------------

def test_scrape_term_once_inserts_some_submissions(
    require_reddit_env,
    prepared_fresh_db,
    scrape_term_once_limited
):
    """
    End-to-end-ish test for scrape_runner.scrape_term_once:

      - Uses the real Reddit API (PRAW) but limits to a few submissions
        via monkeypatch.
      - Verifies that calling scrape_term_once results in >= same number
        of rows in sm.reddit_submission and at least one scrape.job/post_scrape link
    """
    term = "covid vaccine"  # generic, should always have results
   

    # Count rows before
    with getcursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sm.reddit_submission")
        (before_sub_count,) = cur.fetchone()
        cur.execute(
            "SELECT COUNT(*) FROM scrape.job WHERE name = %s",
            (f"reddit monitor: {term}",),
        )
        (before_job_count,) = cur.fetchone()

    new_count = scrape_term_once_limited(term, n=2)
    
    # Basic sanity on return value from scrape_term_once
    assert 0 <= new_count <= 2

    # Count rows after
    with getcursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sm.reddit_submission")
        (after_sub_count,) = cur.fetchone()

        cur.execute(
            "SELECT id FROM scrape.job WHERE name = %s",
            (f"reddit monitor: {term}",),
        )
        jobs = cur.fetchall()

        if jobs:
            (job_id,) = jobs[0]
            cur.execute(
                """
                SELECT COUNT(*)
                FROM scrape.post_scrape ps
                JOIN sm.post_registry pr ON pr.id = ps.post_id
                WHERE ps.scrape_job_id = %s AND pr.platform = 'reddit_submission'
                """,
                (job_id,),
            )
            (linked_count,) = cur.fetchone()
        else:
            linked_count = 0

    # We might hit only existing posts, so just require "no regression" in count.
    assert after_sub_count >= before_sub_count

    # There should now be at least one job row for this term.
    assert len(jobs) >= 1
    # And at least one link from that job to reddit_submission posts.
    assert linked_count >= 1


# -----------------------
# Tests for queries
# -----------------------

def test_queries_recent_submissions_for_term(
    require_reddit_env,
    prepared_fresh_db,
    scrape_term_once_limited
):
    """
    Basic behavior test for queries.get_recent_submissions_for_term
    and get_recent_submissions_for_all_terms:

      - Insert a taxonomy term for "covid vaccine".
      - Run a tiny scrape for that term (scrape_term_once with limited results).
      - Assert that queries sees some recent submissions for that term.
    """
    term = "covid vaccine"

    # Ensure taxonomy term exists
    with getcursor() as cur:
        cur.execute(
            """
            INSERT INTO taxonomy.vaccine_term(name, type)
            VALUES (%s, %s)
            ON CONFLICT (name) DO NOTHING
            """,
            (term, "vaccine"),
        )

    new_count = scrape_term_once_limited(term, n=1)
    assert 0 <= new_count <= 1  # should insert 1 but 0 is possible

    # Now check queries
    with getcursor() as cur:
        rows = queries.get_recent_submissions_for_term(cur, term, limit=10)
        assert isinstance(rows, list)
        # Expect rows as [(id, created_at_ts), ...] or similar
        for tup in rows:
            assert len(tup) >= 2

        all_terms = queries.get_recent_submissions_for_all_terms(
            cur,
            per_term_limit=10
        )
        assert isinstance(all_terms, dict)
        keys_lower = {k.lower() for k in all_terms.keys()}
        assert term.lower() in keys_lower


# -----------------------
# Tests for reddit_monitor (scheduler)
# -----------------------

def test_scheduler_setup_uses_term_intervals(prepared_fresh_db, monkeypatch):
    """
    Test for ScrapeScheduler._setup_initial_schedule:

      - Monkeypatch queries.get_effective_term_list to return
        a tiny synthetic term list.
      - Monkeypatch reddit_monitor._load_metadata to return no saved rates.
      - Monkeypatch reddit_monitor._save_metadata to avoid writing to disk.
      - Instantiate ScrapeScheduler and verify it seeds the task_heap
        with the expected terms.
      - Do *not* run scrape_loop (no long-running threads, no API calls).
    """

    # Use a synthetic term list instead of hitting the real taxonomy table
    def fake_get_effective_term_list(cur):
        return ["term-a", "term-b"]

    monkeypatch.setattr(
        reddit_monitor,
        "get_effective_term_list",
        fake_get_effective_term_list,
    )

    # Avoid reading any real metadata file
    monkeypatch.setattr(
        reddit_monitor,
        "_load_metadata",
        lambda path=reddit_monitor.METADATA_PATH: {},
    )

    # Avoid writing metadata to disk during test
    monkeypatch.setattr(
        reddit_monitor,
        "_save_metadata",
        lambda term_rates, path=reddit_monitor.METADATA_PATH: None,
    )

    scheduler = reddit_monitor.ScrapeScheduler(max_workers=1)
    try:
        # We expect 2 tasks in the heap, each a (time, term) tuple.
        assert len(scheduler.task_heap) == 2
        terms = {t for _, t in scheduler.task_heap}
        assert terms == {"term-a", "term-b"}

        # Task set should also track the same terms.
        assert scheduler.task_set == {"term-a", "term-b"}

        # Rates/intervals should be initialized (default MIN_SCRAPES_PER_DAY)
        for term in ["term-a", "term-b"]:
            assert term in scheduler.term_rates
            assert scheduler.term_rates[term] >= 1.0
            assert term in scheduler.term_intervals
            assert scheduler.term_intervals[term] > 0.0
    finally:
        # Avoid leaving background threads dangling in the test process.
        scheduler.executor.shutdown(wait=False)
