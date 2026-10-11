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


def test_sql_includes_date_filter():
    from scripts.get_posts_for_search_term import _sql_fetch_post_id_page

    sql = _sql_fetch_post_id_page()
    assert "COALESCE(p.created_at_ts, p.date_entered)" in sql
    assert "sm.posts_all" in sql
    assert "ph.post_id > %s" in sql
    assert "LIMIT %s" in sql


def test_iter_post_batches_empty_terms(monkeypatch):
    def boom(*_args, **_kwargs):
        raise AssertionError("empty terms must not open a pool")

    monkeypatch.setattr("scripts.get_posts_for_search_term.init_pool", boom)
    assert list(iter_post_batches([])) == []


def test_iter_post_batches_closes_cursors_before_yield(monkeypatch):
    import scripts.get_posts_for_search_term as mod

    state = {"open": 0}
    id_calls: list[int] = []

    @contextmanager
    def fake_getcursor(*_args, **_kwargs):
        state["open"] += 1
        try:
            yield object()
        finally:
            state["open"] -= 1

    def fake_ids(_cur, *, terms, last_post_id, date_params, page_limit):
        assert state["open"] == 1
        assert terms == ["measles"]
        assert date_params == (None, None, None, None)
        assert page_limit == mod.DEFAULT_ROW_FETCH_SIZE
        id_calls.append(last_post_id)
        if last_post_id == 0:
            return [1, 2]
        return []

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
    monkeypatch.setattr(mod, "_fetch_id_page", fake_ids)
    monkeypatch.setattr(mod, "_hydrate_posts", fake_hydrate)
    monkeypatch.setattr(mod, "_PAGE_CHAR_BUDGET", 5)
    monkeypatch.setattr(mod, "_HYDRATE_SLICE", 10)

    seen: list[list[int]] = []
    for batch in mod.iter_post_batches(["measles"]):
        assert state["open"] == 0
        seen.append([post["post_id"] for post in batch])

    assert seen == [[1], [2]]
    assert id_calls == [0, 2]


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
