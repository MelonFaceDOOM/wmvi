from __future__ import annotations

import heapq
import threading
import time
from datetime import datetime, timedelta, timezone

import services.youtube.monitor.monitor as mon


def _dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def test_schedule_term_updates_state_and_pushes_heap() -> None:
    lock = threading.Lock()
    cv = threading.Condition(lock)

    term_states = {
        1: mon.TermState(name="t1", last_seen=datetime(2026, 2, 1, tzinfo=timezone.utc)),
    }
    heap: list[tuple[float, int]] = []
    pause = mon.PauseState(until_ts=0.0)

    with cv:
        mon.schedule_term(
            cv=cv,
            heap=heap,
            pause=pause,
            term_states=term_states,
            term_id=1,
            interval_s=60.0,
        )

    assert len(heap) == 1
    run_at_ts, term_id = heap[0]
    assert term_id == 1
    assert abs(term_states[1].next_run_at.timestamp() - run_at_ts) < 1e-6


def test_pop_next_runnable_skips_missing_term_id() -> None:
    lock = threading.Lock()
    cv = threading.Condition(lock)
    stop = threading.Event()

    now_ts = datetime.now(timezone.utc).timestamp()

    term_states = {
        2: mon.TermState(
            name="t2",
            last_seen=datetime(2026, 2, 1, tzinfo=timezone.utc),
            next_run_at=_dt(now_ts),
        ),
    }

    # heap has a missing term_id=1 first (should be skipped), then real term_id=2
    heap: list[tuple[float, int]] = [(now_ts, 1), (now_ts, 2)]
    heapq.heapify(heap)

    pause = mon.PauseState(until_ts=0.0)

    with cv:
        out = mon.pop_next_runnable(cv=cv, stop=stop, heap=heap, term_states=term_states, pause=pause)

    assert out is not None
    run_at_ts, term_id = out
    assert term_id == 2
    assert abs(run_at_ts - now_ts) < 1.0  # coarse tolerance


def test_pop_next_runnable_skips_stale_heap_entry_timestamp_mismatch() -> None:
    lock = threading.Lock()
    cv = threading.Condition(lock)
    stop = threading.Event()

    now_ts = datetime.now(timezone.utc).timestamp()

    # term 1 says next_run_at is NOW (valid)
    term_states = {
        1: mon.TermState(
            name="t1",
            last_seen=datetime(2026, 2, 1, tzinfo=timezone.utc),
            next_run_at=_dt(now_ts),
        )
    }

    # heap has a stale entry first (old timestamp), then correct one
    heap: list[tuple[float, int]] = [(now_ts - 10.0, 1), (now_ts, 1)]
    heapq.heapify(heap)

    pause = mon.PauseState(until_ts=0.0)

    with cv:
        out = mon.pop_next_runnable(cv=cv, stop=stop, heap=heap, term_states=term_states, pause=pause)

    assert out is not None
    run_at_ts, term_id = out
    assert term_id == 1
    assert abs(run_at_ts - now_ts) < 1e-3


def test_pop_next_runnable_respects_global_pause() -> None:
    lock = threading.Lock()
    cv = threading.Condition(lock)
    stop = threading.Event()

    now_ts = datetime.now(timezone.utc).timestamp()

    term_states = {
        1: mon.TermState(
            name="t1",
            last_seen=datetime(2026, 2, 1, tzinfo=timezone.utc),
            next_run_at=_dt(now_ts),
        )
    }
    heap: list[tuple[float, int]] = [(now_ts, 1)]
    heapq.heapify(heap)

    # Pause everything for ~0.6s (reduce flakiness vs tiny pauses)
    pause_until = now_ts + 0.6
    pause = mon.PauseState(until_ts=pause_until)

    t_start = time.time()
    with cv:
        out = mon.pop_next_runnable(cv=cv, stop=stop, heap=heap, term_states=term_states, pause=pause)
    elapsed = time.time() - t_start

    assert out is not None
    run_at_ts, term_id = out
    assert term_id == 1
    assert elapsed >= 0.5


def test_load_term_state_updates_last_seen_removes_stale_adds_new(monkeypatch) -> None:
    # initial state: term 1 and term 2 exist
    t0 = datetime(2026, 2, 10, tzinfo=timezone.utc)
    term_states = {
        1: mon.TermState(name="one", last_seen=t0, next_run_at=t0 + timedelta(minutes=5), rate=1.0),
        2: mon.TermState(name="two", last_seen=t0, next_run_at=t0 + timedelta(minutes=6), rate=2.0),
    }

    # db now contains term 1 and term 3 (term 2 is stale)
    def fake_load_search_terms(_list_name: str):
        return [(1, "one"), (3, "three")]

    # db status has newer last_found_ts for term 1, and some last_found_ts for term 3
    db_t1 = t0 + timedelta(days=1)
    db_t3 = t0 - timedelta(days=3)

    def fake_load_status_table():
        return {1: db_t1, 3: db_t3}

    monkeypatch.setattr(mon, "load_search_terms", fake_load_search_terms)
    monkeypatch.setattr(mon, "load_status_table", fake_load_status_table)

    mon.load_term_state(term_states)

    # term 2 removed
    assert 2 not in term_states

    # term 1 last_seen updated to max(local, db)
    assert term_states[1].last_seen == db_t1

    # term 3 added
    assert 3 in term_states
    assert term_states[3].name == "three"
    assert term_states[3].last_seen == db_t3

    # new term scheduled after latest existing (term 1 had +5m; term 2 removed; latest is term1)
    assert term_states[3].next_run_at >= term_states[1].next_run_at + timedelta(minutes=1)