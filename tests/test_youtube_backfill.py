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