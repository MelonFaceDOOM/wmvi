"""Unit tests for get_posts_for_search_term (no DB)."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.get_posts_for_search_term import (
    build_date_params,
    build_posts_payload,
    fetch_and_write,
    iter_post_batches,
    parse_utc_datetime,
    write_posts_json,
)


def test_parse_utc_datetime_date_and_iso():
    d = parse_utc_datetime("2024-01-15")
    assert d == datetime(2024, 1, 15, tzinfo=timezone.utc)
    d2 = parse_utc_datetime("2024-01-15T12:00:00Z")
    assert d2 == datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    assert parse_utc_datetime(None) is None
    with pytest.raises(ValueError):
        parse_utc_datetime("not-a-date")


def test_build_date_params_order():
    since = datetime(2024, 1, 1, tzinfo=timezone.utc)
    until = datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert build_date_params(since, until) == (since, since, until, until)
    assert build_date_params(None, until) == (None, None, until, until)


def test_write_posts_json_envelope(tmp_path: Path):
    posts = [{"post_id": 1, "text": "hi", "hits": []}]
    out = tmp_path / "posts.json"
    payload = write_posts_json(
        out,
        posts,
        terms=["measles", "mmr"],
        since=datetime(2024, 1, 1, tzinfo=timezone.utc),
        until=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    assert out.is_file()
    assert payload["post_count"] == 1
    assert payload["terms"] == ["measles", "mmr"]
    assert payload["posts"][0]["post_id"] == 1
    rebuilt = build_posts_payload(posts, terms=["measles"])
    assert rebuilt["post_count"] == 1


def test_sql_id_lookup_avoids_range_join_on_view():
    from scripts.get_posts_for_search_term import _sql_filter_post_ids, _sql_hit_post_ids

    hits_sql = _sql_hit_post_ids()
    assert "matches.post_term_hit" in hits_sql
    assert "posts_all" not in hits_sql

    filter_sql = _sql_filter_post_ids()
    assert "COALESCE(p.created_at_ts, p.date_entered)" in filter_sql
    assert "sm.posts_all" in filter_sql
    assert "p.post_id = ANY(%s)" in filter_sql
    assert "post_term_hit" not in filter_sql


def test_matching_post_ids_filters_in_chunks(monkeypatch):
    import scripts.get_posts_for_search_term as mod

    class FakeCur:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple]] = []
            self._rows: list[tuple[int]] = []

        def execute(self, sql, params):
            self.calls.append((sql, params))
            if "post_term_hit" in sql:
                self._rows = [(5,), (1,), (3,), (2,), (4,)]
            else:
                ids = params[0]
                self._rows = [(i,) for i in ids if i != 3]

        def fetchall(self):
            return self._rows

    monkeypatch.setattr(mod, "_ID_FILTER_CHUNK", 2)
    cur = FakeCur()
    since = datetime(2023, 1, 1, tzinfo=timezone.utc)
    ids = mod._matching_post_ids_cur(cur, terms=["measles"], since=since, until=None)

    assert ids == [1, 2, 4, 5]
    filter_calls = [c for c in cur.calls if "post_term_hit" not in c[0]]
    assert [c[1][0] for c in filter_calls] == [[1, 2], [3, 4], [5]]
    assert filter_calls[0][1][1:] == (since, since, None, None)


def test_iter_post_batches_empty_terms(monkeypatch):
    def boom(*_args, **_kwargs):
        raise AssertionError("empty terms must not open a pool")

    monkeypatch.setattr("scripts.get_posts_for_search_term.init_pool", boom)
    assert list(iter_post_batches([])) == []


def test_iter_post_batches_closes_cursors_before_yield(monkeypatch):
    import scripts.get_posts_for_search_term as mod

    state = {"open": 0}
    id_calls: list[tuple] = []

    @contextmanager
    def fake_getcursor(*_args, **_kwargs):
        state["open"] += 1
        try:
            yield object()
        finally:
            state["open"] -= 1

    def fake_find(terms, *, since, until):
        assert state["open"] == 0
        assert terms == ["measles"]
        id_calls.append((since, until))
        return [1, 2]

    def fake_hydrate(_cur_posts, _cur_hits, *, terms, post_ids):
        assert state["open"] == 2
        assert terms == ["measles"]
        return [
            {
                "post_id": post_id,
                "text": "hello" if post_id == 1 else "world!",
                "hits": [{"term_name": "measles"}],
            }
            for post_id in post_ids
        ]

    monkeypatch.setattr(mod, "getcursor", fake_getcursor)
    monkeypatch.setattr(mod, "init_pool", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "close_pool", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "find_matching_post_ids", fake_find)
    monkeypatch.setattr(mod, "_hydrate_posts", fake_hydrate)
    monkeypatch.setattr(mod, "_PAGE_CHAR_BUDGET", 5)

    seen: list[list[int]] = []
    for batch in mod.iter_post_batches(["measles"]):
        assert state["open"] == 0
        seen.append([post["post_id"] for post in batch])

    assert seen == [[1], [2]]
    assert id_calls == [(None, None)]


def test_iter_post_batches_uses_given_ids_and_limit(monkeypatch):
    import scripts.get_posts_for_search_term as mod

    @contextmanager
    def fake_getcursor(*_args, **_kwargs):
        yield object()

    def no_lookup(*_a, **_k):
        raise AssertionError("post_ids given; id lookup must not run")

    hydrated: list[list[int]] = []

    def fake_hydrate(_cur_posts, _cur_hits, *, terms, post_ids):
        hydrated.append(list(post_ids))
        return [{"post_id": i, "text": "x", "hits": [{}]} for i in post_ids]

    monkeypatch.setattr(mod, "getcursor", fake_getcursor)
    monkeypatch.setattr(mod, "init_pool", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "close_pool", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "find_matching_post_ids", no_lookup)
    monkeypatch.setattr(mod, "_hydrate_posts", fake_hydrate)

    batches = list(
        mod.iter_post_batches(
            ["measles"],
            post_ids=[7, 3, 5, 1, 9],
            row_fetch_size=2,
            limit=4,
        )
    )
    assert hydrated == [[1, 3], [5, 7]]
    assert [p["post_id"] for b in batches for p in b] == [1, 3, 5, 7]


def test_fetch_and_write_streams_pages(tmp_path: Path, monkeypatch):
    def fake_batches(*_args, **_kwargs):
        yield [{"post_id": 1, "text": "a", "hits": []}]
        yield [{"post_id": 2, "text": "b", "hits": []}]

    monkeypatch.setattr("scripts.get_posts_for_search_term.iter_post_batches", fake_batches)
    out = tmp_path / "posts.json"
    summary = fetch_and_write(terms=["measles"], out_path=out)
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert [post["post_id"] for post in payload["posts"]] == [1, 2]
    assert payload["post_count"] == 2
    assert payload["matched_post_count"] == 2
    assert payload["terms"] == ["measles"]
    assert summary["post_count"] == 2
    assert not (tmp_path / "posts.json.tmp").exists()


def test_fetch_and_write_empty(tmp_path: Path, monkeypatch):
    def fake_batches(*_args, **_kwargs):
        if False:
            yield []

    monkeypatch.setattr("scripts.get_posts_for_search_term.iter_post_batches", fake_batches)
    out = tmp_path / "posts.json"
    summary = fetch_and_write(terms=["measles"], out_path=out)
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["posts"] == []
    assert payload["post_count"] == 0
    assert summary["post_count"] == 0
