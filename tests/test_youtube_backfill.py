from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import services.youtube.backfill.backfill as bf

from tests.helpers.youtube_fakes import (
    FakeScrapeWindowOutcome,
    FakeYTQuotaClient,
    fake_getcursor,
)


# ----------------------------
# oldest_video_ts_for_term
# ----------------------------

def test_oldest_video_ts_for_term_returns_none_when_null(monkeypatch) -> None:
    """
    oldest_video_ts_for_term should return None when:
      - no row found OR oldest_found_ts is NULL.
    """
    getcursor_fn, _cur = fake_getcursor(fetchone_value=(None,))
    monkeypatch.setattr(bf, "getcursor", getcursor_fn)

    assert bf.oldest_video_ts_for_term(123) is None


def test_save_oldest_found_ts_returns_none_when_no_row(monkeypatch) -> None:
    getcursor_fn, _cur = fake_getcursor(fetchone_value=None)
    commits: list[bool] = []

    def wrapped_getcursor(*_a, commit: bool = False, **_k):
        commits.append(commit)
        return getcursor_fn(commit=commit)

    monkeypatch.setattr(bf, "getcursor", wrapped_getcursor)

    out = bf.save_oldest_found_ts(123, datetime(2024, 6, 1, tzinfo=timezone.utc))
    assert out is None
    assert commits == [True]


def test_save_oldest_found_ts_returns_stored_value(monkeypatch) -> None:
    stored = datetime(2024, 5, 1, tzinfo=timezone.utc)
    getcursor_fn, _cur = fake_getcursor(fetchone_value=(stored,))
    commits: list[bool] = []

    def wrapped_getcursor(*_a, commit: bool = False, **_k):
        commits.append(commit)
        return getcursor_fn(commit=commit)

    monkeypatch.setattr(bf, "getcursor", wrapped_getcursor)

    out = bf.save_oldest_found_ts(123, datetime(2024, 6, 1, tzinfo=timezone.utc))
    assert out == stored
    assert commits == [True]


def test_window_notable_detects_inserts_and_empty() -> None:
    empty = FakeScrapeWindowOutcome()
    inserted = FakeScrapeWindowOutcome(found_v=1, ins_v=1)

    assert bf._window_notable(empty, hit_max_pages=False, early_stop=False) is False
    assert bf._window_notable(inserted, hit_max_pages=False, early_stop=False) is True
    assert bf._window_notable(empty, hit_max_pages=True, early_stop=False) is True


def test_oldest_video_ts_for_term_normalizes_to_utc(monkeypatch) -> None:
    naive = datetime(2024, 1, 1, 12, 0)  # naive

    getcursor_fn, _cur = fake_getcursor(fetchone_value=(naive,))
    monkeypatch.setattr(bf, "getcursor", getcursor_fn)

    out = bf.oldest_video_ts_for_term(123)
    assert out is not None
    assert out.tzinfo is not None
    assert out.tzinfo == timezone.utc


# ----------------------------
# backfill_term behavior
# ----------------------------

def test_backfill_term_raises_when_budget_too_low(monkeypatch) -> None:
    # Make oldest_ts exist so we enter loop
    monkeypatch.setattr(
        bf,
        "oldest_video_ts_for_term",
        lambda _term_id: datetime(2024, 6, 1, tzinfo=timezone.utc),
    )

    qyt = FakeYTQuotaClient(afford=False)

    with pytest.raises(bf.YTBudgetExceeded):
        bf.backfill_term(qyt, term_id=1, term_name="x")


def test_backfill_term_uses_backfill_end_when_no_existing_data(monkeypatch) -> None:
    # No existing data -> use BACKFILL_END_UTC as published_before
    monkeypatch.setattr(bf, "oldest_video_ts_for_term", lambda _term_id: None)
    monkeypatch.setattr(bf, "save_oldest_found_ts", lambda term_id, oldest_ts: oldest_ts)

    calls: list[tuple[datetime, datetime]] = []

    def fake_scrape_window(*, qyt, term_name, published_after, published_before, max_pages, **_):
        calls.append((published_after, published_before))
        return FakeScrapeWindowOutcome(
            pages=1,
            found_v=0,
            stops={"exhausted": 1},
        )

    monkeypatch.setattr(bf, "scrape_window", fake_scrape_window)

    # Reduce range so test ends quickly
    monkeypatch.setattr(bf, "BACKFILL_START_UTC", datetime(2024, 1, 1, tzinfo=timezone.utc))
    monkeypatch.setattr(bf, "BACKFILL_END_UTC", datetime(2024, 1, 2, tzinfo=timezone.utc))
    monkeypatch.setattr(bf, "INITIAL_WINDOW", timedelta(days=30))
    monkeypatch.setattr(bf, "OVERLAP", timedelta(minutes=0))

    qyt = FakeYTQuotaClient(afford=True)
    bf.backfill_term(qyt, term_id=1, term_name="x")

    assert calls, "Expected scrape_window to be called at least once"
    assert calls[0][1] == bf.BACKFILL_END_UTC


def test_backfill_term_shrinks_window_on_max_pages_and_retries_same_window(monkeypatch) -> None:
    monkeypatch.setattr(bf, "save_oldest_found_ts", lambda term_id, oldest_ts: oldest_ts)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 2, 1, tzinfo=timezone.utc)

    monkeypatch.setattr(bf, "BACKFILL_START_UTC", start)
    monkeypatch.setattr(bf, "BACKFILL_END_UTC", end)
    monkeypatch.setattr(bf, "INITIAL_WINDOW", timedelta(days=30))
    monkeypatch.setattr(bf, "MIN_WINDOW", timedelta(days=7))
    monkeypatch.setattr(bf, "OVERLAP", timedelta(0))  # <-- prevents boundary ping-pong
    monkeypatch.setattr(bf, "MAX_PAGES", 10)

    monkeypatch.setattr(bf, "oldest_video_ts_for_term", lambda _term_id: end)

    calls: list[tuple[datetime, datetime]] = []
    n = {"k": 0}

    def fake_scrape_window(*, qyt, term_name, published_after, published_before, max_pages, **_):
        calls.append((published_after, published_before))
        n["k"] += 1
        if n["k"] == 1:
            return FakeScrapeWindowOutcome(stops={"max_pages": 1}, pages=max_pages or 0)
        return FakeScrapeWindowOutcome(stops={}, pages=1)

    monkeypatch.setattr(bf, "scrape_window", fake_scrape_window)

    qyt = FakeYTQuotaClient(afford=True)
    bf.backfill_term(qyt, term_id=1, term_name="x")

    assert len(calls) >= 2
    assert calls[0][1] == calls[1][1]  # retried same published_before


def test_backfill_term_advances_published_before_backward_with_overlap(monkeypatch) -> None:
    monkeypatch.setattr(bf, "save_oldest_found_ts", lambda term_id, oldest_ts: oldest_ts)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 20, tzinfo=timezone.utc)

    monkeypatch.setattr(bf, "BACKFILL_START_UTC", start)
    monkeypatch.setattr(bf, "BACKFILL_END_UTC", end)
    monkeypatch.setattr(bf, "INITIAL_WINDOW", timedelta(days=7))
    monkeypatch.setattr(bf, "MIN_WINDOW", timedelta(days=7))
    monkeypatch.setattr(bf, "OVERLAP", timedelta(minutes=5))
    monkeypatch.setattr(bf, "MAX_PAGES", 10)

    monkeypatch.setattr(bf, "oldest_video_ts_for_term", lambda _term_id: end)

    calls: list[tuple[datetime, datetime]] = []

    def fake_scrape_window(*, qyt, term_name, published_after, published_before, max_pages, **_):
        calls.append((published_after, published_before))
        return FakeScrapeWindowOutcome(stops={}, pages=1)

    monkeypatch.setattr(bf, "scrape_window", fake_scrape_window)

    qyt = FakeYTQuotaClient(afford=True)
    bf.backfill_term(qyt, term_id=1, term_name="x")

    assert len(calls) >= 2
    first_after, _first_before = calls[0]
    _second_after, second_before = calls[1]

    assert second_before == first_after + bf.OVERLAP


# ----------------------------
# run_backfill behavior
# ----------------------------

def test_is_rate_limited_detects_429() -> None:
    from services.youtube.quota_client import YTUnexpectedError, is_rate_limited

    assert is_rate_limited(YTUnexpectedError("x", status=429, reason=None))
    assert not is_rate_limited(YTUnexpectedError("x", status=403, reason="forbidden"))


def test_run_backfill_sleeps_and_retries_term_on_rate_limit(monkeypatch) -> None:
    from services.youtube.quota_client import YTUnexpectedError

    monkeypatch.setattr(bf, "load_search_terms", lambda _name: [(1, "one")])
    monkeypatch.setattr(bf, "RATE_LIMIT_BACKOFF_S", 0.0)

    calls: list[str] = []

    def fake_backfill_term(_qyt, *, term_id: int, term_name: str) -> None:
        calls.append(term_name)
        if len(calls) == 1:
            raise YTUnexpectedError("rate limited", status=429, reason=None)

    monkeypatch.setattr(bf, "backfill_term", fake_backfill_term)
    monkeypatch.setattr(
        bf.YTQuotaClient,
        "from_api_key",
        classmethod(lambda cls, *, tracker: FakeYTQuotaClient(afford=True)),
    )

    bf.run_backfill()
    assert calls == ["one", "one"]


def test_run_backfill_stops_on_budget_exceeded(monkeypatch) -> None:
    monkeypatch.setattr(bf, "load_search_terms", lambda _name: [(1, "one"), (2, "two")])

    def fake_backfill_term(_qyt, *, term_id: int, term_name: str) -> None:
        if term_id == 2:
            raise bf.YTBudgetExceeded("boom")

    monkeypatch.setattr(bf, "backfill_term", fake_backfill_term)

    monkeypatch.setattr(
        bf.YTQuotaClient,
        "from_api_key",
        classmethod(lambda cls, *, tracker: FakeYTQuotaClient(afford=True)),
    )

    # Should return without raising
    bf.run_backfill()