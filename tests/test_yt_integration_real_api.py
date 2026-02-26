"""
test the actual api calls (not dummy calls like test_yt_quota_client.py)
not called as part of standard test routine
call with pytest -m yt_integration -q
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from services.youtube.quota_client import (
    BudgetTracker,
    YTQuotaClient,
    YTQuotaExceeded,
    YTBudgetExceeded,
    YTUnexpectedError,
)

pytestmark = pytest.mark.yt_integration


def test_youtube_api_search_enrich_comments() -> None:
    """
    Integration smoke test (real network):
      - search.list (cost 100)
      - videos.list (cost 1)
      - commentThreads.list (cost 1)

    Goal: verify the live googleapiclient object + error envelope assumptions
    """
    if not os.getenv("YT_API_KEY"):
        pytest.skip("YT_API_KEY not set; skipping integration test")

    tracker = BudgetTracker(budget_units_per_day=2_000)
    client = YTQuotaClient.from_api_key(tracker=tracker)

    now = datetime.now(timezone.utc)
    published_after = (now - timedelta(days=7)).isoformat()
    published_before = now.isoformat()

    try:
        # 1) search
        ids, nxt = client.search_page(
            term_name="vaccine",
            region=None,
            published_after=published_after,
            published_before=published_before,
            page_token=None,
            max_results=5,
            order="date",
        )
    except YTQuotaExceeded:
        pytest.skip("YouTube quota exceeded for this API key/project")
    except YTBudgetExceeded:
        pytest.fail("Local budget unexpectedly exceeded in smoke test")
    except YTUnexpectedError as e:
        pytest.fail(f"Unexpected YouTube error during search_page: {e!r}")

    if not ids:
        pytest.skip("No videos returned for query window; nothing to smoke-test further")

    assert all(isinstance(v, str) and v for v in ids)
    assert nxt is None or isinstance(nxt, str)

    # search.list is always 100 units
    assert tracker.used_units_today() == 100

    # 2) enrich
    try:
        enriched = client.enrich_videos(ids)
    except YTQuotaExceeded:
        pytest.skip("YouTube quota exceeded for this API key/project")
    except YTUnexpectedError as e:
        pytest.fail(f"Unexpected YouTube error during enrich_videos: {e!r}")

    assert isinstance(enriched, list)
    # In practice the API should return one item per id; if not, at least ensure non-empty
    assert len(enriched) > 0
    assert all(isinstance(it, dict) for it in enriched)

    # videos.list costs 1 per call; for <= 50 ids it's exactly 1 call
    assert tracker.used_units_today() == 101

    # 3) comments (may be empty; that's fine)
    video_id = ids[0]
    try:
        items, nxt = client.fetch_comment_threads(
            video_id=video_id,
            max_threads=20,
            order="relevance",
        )
    except YTQuotaExceeded:
        pytest.skip("YouTube quota exceeded for this API key/project")
    except YTUnexpectedError as e:
        pytest.fail(f"Unexpected YouTube error during fetch_comment_threads: {e!r}")

    assert isinstance(items, list)
    assert nxt is None or isinstance(nxt, str)

    # commentThreads.list costs 1
    assert tracker.used_units_today() == 102
