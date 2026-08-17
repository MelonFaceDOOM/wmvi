"""Unit tests for get_posts_for_search_term (no DB)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.get_posts_for_search_term import (
    build_date_params,
    build_posts_payload,
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
