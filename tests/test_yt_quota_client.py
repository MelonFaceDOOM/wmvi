from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
import json

import pytest

from services.youtube.quota_client import (
    BudgetTracker,
    YTBudgetExceeded,
    YTQuotaClient,
    YTQuotaExceeded,
    YTUnexpectedError
)

# ----------------------------
# Helpers for deterministic time
# ----------------------------

class Now:
    def __init__(self, dt: datetime) -> None:
        self.dt = dt

    def __call__(self) -> datetime:
        return self.dt

    def set(self, dt: datetime) -> None:
        self.dt = dt


class Sleeper:
    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, s: float) -> None:
        self.calls.append(float(s))


# ----------------------------
# Fake google client _Chain() + FakeYT
# Overall this replicates yt.*().list().execute() chains
# without using any real network/API interaction

# There are 3 aspects to the yt client we need to mimic:

# 1) Resource Groups (yt.videos(), yt.search(), etc)
#  - These don't touch the network. They just return objects that know how
#  - To handle their appropriate API endpoints

# 2) ResourceGroup.list() (i.e. yt.videos().list())
#  - list() is the endpoint are concerned with
#  - Still doesn't touch network, but builds all the info needed for the network request
#  - That's why we pass kwargs

# 3) ResourceGroup.list().execute()
#  - Does the actual API network request
#  - But we mimic it to just return a similar object with no network usage
# ----------------------------

class _Chain:
    def __init__(self, fn):
        self._fn = fn

    def list(self, **kwargs):
        self._kwargs = kwargs
        return self

    def execute(self):
        return self._fn(getattr(self, "_kwargs", {}))


class FakeYT:
    """
    Minimal fake of googleapiclient youtube client used by YTQuotaClient.
    Configure per-endpoint handlers that receive list() kwargs and return a dict or raise.
    """

    def __init__(
        self,
        *,
        on_search=None,
        on_videos=None,
        on_comment_threads=None,
        on_comments=None,
    ) -> None:
        self.on_search = on_search or (lambda kwargs: {"items": [], "nextPageToken": None})
        self.on_videos = on_videos or (lambda kwargs: {"items": []})
        self.on_comment_threads = on_comment_threads or (lambda kwargs: {"items": [], "nextPageToken": None})
        self.on_comments = on_comments or (lambda kwargs: {"items": [], "nextPageToken": None})

    def search(self):
        return _Chain(self.on_search)

    def videos(self):
        return _Chain(self.on_videos)

    def commentThreads(self):
        return _Chain(self.on_comment_threads)

    def comments(self):
        return _Chain(self.on_comments)


# ----------------------------
# Duck-typed HttpError stand-in for default_classify_error()
# ----------------------------

class HttpErrorLike(Exception):
    """
    Minimal duck-typed stand-in for googleapiclient.errors.HttpError:
      - .resp.status
      - .content (bytes JSON with error.errors[*].reason)
    """

    def __init__(self, *, status: int, reason: str):
        super().__init__(f"HttpErrorLike status={status} reason={reason}")
        self.resp = type("Resp", (), {"status": status})()
        self.content = json.dumps(
            {
                "error": {
                    "errors": [{"reason": reason}],
                    "code": status,
                    "message": "synthetic",
                }
            }
        ).encode("utf-8")


# ----------------------------
# BudgetTracker basics
# ----------------------------

def test_budget_blocks_search_when_under_100_units() -> None:
    """YTQuotaClient should not allow for a call it doesn't have budget for."""
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=99, now_fn=now)

    sleeper = Sleeper()
    client = YTQuotaClient(FakeYT(), tracker=tracker, sleep_fn=sleeper)

    called = {"n": 0}

    def exec_fn():
        called["n"] += 1
        return {"ok": True}

    with pytest.raises(YTBudgetExceeded):
        client.call("search.list", exec_fn)

    assert called["n"] == 0
    assert tracker.used_units_today() == 0
    assert sleeper.calls == []


def test_unit_accumulation_costs() -> None:
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)
    client = YTQuotaClient(FakeYT(), tracker=tracker, sleep_fn=Sleeper())

    client.call("videos.list", lambda: {"items": []})
    client.call("videos.list", lambda: {"items": []})
    assert tracker.used_units_today() == 2

    client.call("search.list", lambda: {"items": []})
    assert tracker.used_units_today() == 102


def test_pacific_day_rollover_resets_budget() -> None:
    # 2026-02-13 07:00 UTC = 2026-02-12 23:00 PT
    # 2026-02-13 09:00 UTC = 2026-02-13 01:00 PT
    now = Now(datetime(2026, 2, 13, 7, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)
    client = YTQuotaClient(FakeYT(), tracker=tracker, sleep_fn=Sleeper())

    client.call("videos.list", lambda: {"ok": True})
    assert tracker.used_units_today() == 1

    now.set(datetime(2026, 2, 13, 9, 0, tzinfo=timezone.utc))
    assert tracker.used_units_today() == 0

    client.call("videos.list", lambda: {"ok": True})
    assert tracker.used_units_today() == 1

# ----------------------------
# YTQuotaClient.call() behavior with real classifier
# ----------------------------

def test_call_quota_exceeded_maps_to_YTQuotaExceeded() -> None:
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)
    client = YTQuotaClient(FakeYT(), tracker=tracker, sleep_fn=Sleeper())

    def exec_fn():
        raise HttpErrorLike(status=403, reason="quotaExceeded")

    with pytest.raises(YTQuotaExceeded):
        client.call("search.list", exec_fn)

    # search.list costs 100
    assert tracker.used_units_today() == 100


def test_call_retries_retryable_then_succeeds_charges_and_sleeps() -> None:
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)

    sleeper = Sleeper()
    client = YTQuotaClient(
        FakeYT(),
        tracker=tracker,
        sleep_fn=sleeper,
        max_retries=5,
        base_backoff_s=0.1,
        max_backoff_s=0.2,
        jitter=0.0,
        charge_on_retry=True,
    )

    attempts = {"n": 0}

    def exec_fn():
        attempts["n"] += 1
        if attempts["n"] <= 2:
            raise HttpErrorLike(status=503, reason="backendError")
        return {"ok": True}

    out = client.call("videos.list", exec_fn)
    assert out["ok"] is True
    assert attempts["n"] == 3
    assert len(sleeper.calls) == 2
    assert tracker.used_units_today() == 3  # videos.list cost=1, charged each attempt


def test_call_non_retryable_raises_YTUnexpectedError() -> None:
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)
    client = YTQuotaClient(FakeYT(), tracker=tracker, sleep_fn=Sleeper())

    def exec_fn():
        raise HttpErrorLike(status=400, reason="badRequest")

    with pytest.raises(YTUnexpectedError):
        client.call("videos.list", exec_fn)

    assert tracker.used_units_today() == 1


# ----------------------------
# High-level API helper methods (FakeYT)
# Ensures internal routing is working correctly on each endpoint
# ----------------------------

def test_search_page_charges_100_and_parses_ids_and_next_token() -> None:
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)

    def on_search(kwargs: Dict[str, Any]) -> dict:
        assert kwargs["part"] == "id"
        assert kwargs["q"] == "vaccines"
        assert kwargs["maxResults"] == 50
        assert kwargs["order"] == "date"
        return {
            "items": [
                {"id": {"videoId": "a"}},
                {"id": {"videoId": "b"}},
                {"id": {"videoId": None}},  # should be ignored
            ],
            "nextPageToken": "NEXT",
        }

    yt = FakeYT(on_search=on_search)
    client = YTQuotaClient(yt, tracker=tracker, sleep_fn=Sleeper())

    ids, nxt = client.search_page(
        term_name="vaccines",
        region=None,
        published_after="2026-02-01T00:00:00+00:00",
        published_before="2026-02-13T00:00:00+00:00",
        page_token=None,
    )

    assert ids == ["a", "b"]
    assert nxt == "NEXT"
    assert tracker.used_units_today() == 100


def test_enrich_videos_charges_1_per_videos_list_call_chunked_by_50() -> None:
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)

    calls = {"n": 0}

    def on_videos(kwargs: Dict[str, Any]) -> dict:
        calls["n"] += 1
        ids = kwargs["id"].split(",")
        return {"items": [{"id": vid, "snippet": {"publishedAt": "2026-02-10T00:00:00Z"}} for vid in ids]}

    yt = FakeYT(on_videos=on_videos)
    client = YTQuotaClient(yt, tracker=tracker, sleep_fn=Sleeper())

    video_ids = [f"v{i}" for i in range(120)]  # 50 + 50 + 20 => 3 API calls
    items = client.enrich_videos(video_ids)

    assert len(items) == 120
    assert calls["n"] == 3
    assert tracker.used_units_today() == 3


def test_fetch_comment_threads_charges_1_and_returns_items() -> None:
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)

    def on_comment_threads(kwargs: Dict[str, Any]) -> dict:
        assert kwargs["videoId"] == "vid1"
        return {"items": [{"id": "t1"}, {"id": "t2"}], "nextPageToken": "MORE"}

    yt = FakeYT(on_comment_threads=on_comment_threads)
    client = YTQuotaClient(yt, tracker=tracker, sleep_fn=Sleeper())

    items, nxt = client.fetch_comment_threads(video_id="vid1", max_threads=100, order="relevance")
    assert [it["id"] for it in items] == ["t1", "t2"]
    assert nxt == "MORE"
    assert tracker.used_units_today() == 1


def test_fetch_comment_replies_charges_1_and_returns_items() -> None:
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)

    def on_comments(kwargs: Dict[str, Any]) -> dict:
        assert kwargs["parentId"] == "parent1"
        return {"items": [{"id": "r1"}], "nextPageToken": None}

    yt = FakeYT(on_comments=on_comments)
    client = YTQuotaClient(yt, tracker=tracker, sleep_fn=Sleeper())

    items, nxt = client.fetch_comment_replies(video_id="vid1", parent_comment_id="parent1", max_replies=50)
    assert [it["id"] for it in items] == ["r1"]
    assert nxt is None
    assert tracker.used_units_today() == 1


def test_fetch_comment_threads_comments_disabled_returns_empty_and_does_not_raise() -> None:
    """
    fetch_comment_threads special-cases commentsDisabled and returns ([], None).
    We simulate an HttpError-ish exception with .content containing that reason.
    """
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)

    def on_comment_threads(_kwargs: Dict[str, Any]) -> dict:
        raise HttpErrorLike(status=403, reason="commentsDisabled")

    yt = FakeYT(on_comment_threads=on_comment_threads)
    client = YTQuotaClient(yt, tracker=tracker, sleep_fn=Sleeper())

    items, nxt = client.fetch_comment_threads(video_id="vid1", max_threads=100)
    assert items == []
    assert nxt is None

    # Still charged 1 unit for the attempted call
    assert tracker.used_units_today() == 1


def test_fetch_comment_threads_video_not_found_returns_empty_and_does_not_raise() -> None:
    """
    fetch_comment_threads special-cases videoNotFound and returns ([], None).
    """
    now = Now(datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc))
    tracker = BudgetTracker(budget_units_per_day=500, now_fn=now)

    def on_comment_threads(_kwargs: Dict[str, Any]) -> dict:
        raise HttpErrorLike(status=404, reason="videoNotFound")

    yt = FakeYT(on_comment_threads=on_comment_threads)
    client = YTQuotaClient(yt, tracker=tracker, sleep_fn=Sleeper())

    items, nxt = client.fetch_comment_threads(video_id="vid1", max_threads=100)
    assert items == []
    assert nxt is None

    # Still charged 1 unit for the attempted call
    assert tracker.used_units_today() == 1
