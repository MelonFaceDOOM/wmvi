"""Unit tests for scripts.list_vaccine_terms (no DB)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts.list_vaccine_terms import list_subset_names, list_term_names, main, write_terms_file


def test_write_terms_file(tmp_path: Path):
    out = tmp_path / "terms.txt"
    write_terms_file(out, ["measles", "mmr vaccine"])
    assert out.read_text(encoding="utf-8") == "measles\nmmr vaccine\n"


def test_list_term_names_all():
    cur = MagicMock()
    cur.fetchall.return_value = [("measles",), ("mmr",)]
    ctx = MagicMock()
    ctx.__enter__.return_value = cur
    ctx.__exit__.return_value = False
    with patch("scripts.list_vaccine_terms.getcursor", return_value=ctx):
        names = list_term_names()
    assert names == ["measles", "mmr"]
    sql = cur.execute.call_args[0][0]
    assert "FROM taxonomy.vaccine_term" in sql
    assert "vaccine_term_subset" not in sql


def test_list_term_names_subset():
    cur = MagicMock()
    cur.fetchall.return_value = [("measles",)]
    ctx = MagicMock()
    ctx.__enter__.return_value = cur
    ctx.__exit__.return_value = False
    with patch("scripts.list_vaccine_terms.getcursor", return_value=ctx):
        names = list_term_names(subset="core_search_terms")
    assert names == ["measles"]
    args = cur.execute.call_args[0]
    assert "vaccine_term_subset" in args[0]
    assert args[1] == ("core_search_terms",)


def test_list_subset_names():
    cur = MagicMock()
    cur.fetchall.return_value = [("core_search_terms", "desc", 20)]
    ctx = MagicMock()
    ctx.__enter__.return_value = cur
    ctx.__exit__.return_value = False
    with patch("scripts.list_vaccine_terms.getcursor", return_value=ctx):
        rows = list_subset_names()
    assert rows == [
        {"name": "core_search_terms", "description": "desc", "term_count": 20}
    ]


def test_main_out_file(tmp_path: Path):
    out = tmp_path / "t.txt"
    with (
        patch("scripts.list_vaccine_terms.init_pool"),
        patch("scripts.list_vaccine_terms.close_pool"),
        patch(
            "scripts.list_vaccine_terms.list_term_names",
            return_value=["a", "b"],
        ),
    ):
        rc = main(["--out", str(out), "--prod"])
    assert rc == 0
    assert out.read_text(encoding="utf-8") == "a\nb\n"
