"""Demo filter clause, 90d trending rank, platform grouping, anti-share."""

from __future__ import annotations

import sqlite3

from apps.claims.demo import platforms as plat
from apps.claims.demo.db import (
    DemoFilters,
    dashboard_stats,
    list_leaves,
    list_narratives,
    member_claims,
    platform_counts,
    trending_cutoff_date,
    weekly_counts,
)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE narratives (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            blurb TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE leaves (
            id INTEGER PRIMARY KEY,
            narrative_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            blurb TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE claims (
            idx INTEGER PRIMARY KEY,
            claim_key TEXT NOT NULL,
            claim_text TEXT NOT NULL,
            leaf_id INTEGER NOT NULL,
            narrative_id INTEGER NOT NULL
        );
        CREATE TABLE occurrences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            claim_idx INTEGER NOT NULL,
            leaf_id INTEGER NOT NULL,
            narrative_id INTEGER NOT NULL,
            ts TEXT,
            week TEXT,
            platform TEXT,
            alignment REAL,
            url TEXT,
            post_id TEXT,
            snippet TEXT
        );
        """
    )
    conn.execute("INSERT INTO meta VALUES ('ts_max', '2026-08-18T00:00:00+00:00')")
    conn.executemany(
        "INSERT INTO narratives(id, title) VALUES (?, ?)",
        [(1, "Old volume"), (2, "Recent spike")],
    )
    conn.executemany(
        "INSERT INTO leaves(id, narrative_id, title) VALUES (?, ?, ?)",
        [(10, 1, "Old leaf"), (11, 1, "Old anti leaf"), (20, 2, "New leaf")],
    )
    conn.executemany(
        "INSERT INTO claims(idx, claim_key, claim_text, leaf_id, narrative_id) VALUES (?, ?, ?, ?, ?)",
        [
            (0, "a", "old claim", 10, 1),
            (1, "b", "old anti claim", 11, 1),
            (2, "c", "new claim", 20, 2),
        ],
    )
    old = "2026-01-15T00:00:00+00:00"
    recent = "2026-07-01T00:00:00+00:00"
    rows = [(0, 10, 1, old, "2026-W03", "reddit_comment", 1.0, f"p{i}") for i in range(4)]
    rows.append((0, 10, 1, old, "2026-W03", "reddit_submission", 1.0, "psub"))
    rows.append((1, 11, 1, old, "2026-W03", "reddit_comment", 0.25, "panti"))
    rows.append((2, 20, 2, recent, "2026-W27", "youtube_comment", 0.25, "py1"))
    rows.append((2, 20, 2, recent, "2026-W27", "youtube_comment", 0.25, "py2"))
    conn.executemany(
        "INSERT INTO occurrences(claim_idx, leaf_id, narrative_id, ts, week, platform, alignment, post_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return conn


def test_trending_cutoff_is_90_days_before_ts_max() -> None:
    assert trending_cutoff_date(_conn()) == "2026-05-20"


def test_reddit_group_keys_and_labels() -> None:
    assert plat.group_to_keys("reddit") == ("reddit_comment", "reddit_submission")
    assert plat.keys_for_groups(["reddit"]) == ("reddit_comment", "reddit_submission")
    assert plat.label("reddit_comment") == "Reddit"
    assert plat.label("youtube_comment") == "YouTube comment"


def test_volume_vs_trending_rank() -> None:
    conn = _conn()
    volume = list_narratives(conn, DemoFilters(sort="volume"))
    trending = list_narratives(conn, DemoFilters(sort="trending"))
    assert [r["title"] for r in volume] == ["Old volume", "Recent spike"]
    assert [r["title"] for r in trending] == ["Recent spike"]
    new = trending[0]
    assert new["id"] == 2
    assert new["n_occ"] == 2
    assert new["n_90d"] == 2
    old = next(r for r in volume if r["id"] == 1)
    assert old["n_occ"] == 6 and old["n_90d"] == 0


def test_trending_clips_dashboard_stats() -> None:
    conn = _conn()
    vol = dashboard_stats(conn, DemoFilters(sort="volume"))
    tr = dashboard_stats(conn, DemoFilters(sort="trending"))
    assert vol["n_occurrences"] == 8
    assert tr["n_occurrences"] == 2
    assert vol["n_claims"] == 3
    assert tr["n_claims"] == 1
    assert vol["n_posts"] == 8
    assert tr["n_posts"] == 2


def test_anti_share_from_filtered_counts() -> None:
    rows = {r["id"]: r for r in list_narratives(_conn(), DemoFilters(sort="volume"))}
    old, new = rows[1], rows[2]
    assert old["n_anti"] == 1 and old["n_occ"] == 6
    assert round(100 * old["n_anti"] / old["n_occ"]) == 17
    assert new["n_anti"] == 2 and round(100 * new["n_anti"] / new["n_occ"]) == 100


def test_platform_and_anti() -> None:
    conn = _conn()
    yt = plat.keys_for_groups(["youtube_comment"])
    reddit = plat.keys_for_groups(["reddit"])
    yt_anti = list_narratives(conn, DemoFilters(anti=True, platforms=yt, sort="volume"))
    assert [r["id"] for r in yt_anti] == [2]
    assert yt_anti[0]["n_occ"] == 2
    reddit_anti = list_narratives(
        conn, DemoFilters(anti=True, platforms=reddit, sort="volume")
    )
    assert [r["id"] for r in reddit_anti] == [1]
    assert reddit_anti[0]["n_occ"] == 1
    stats = dashboard_stats(conn, DemoFilters(anti=True, platforms=yt))
    assert stats["n_occurrences"] == 2


def test_reddit_bar_is_grouped() -> None:
    by_id = {r["platform"]: r for r in platform_counts(_conn(), DemoFilters(sort="volume"))}
    assert by_id["reddit"]["n"] == 6
    assert by_id["reddit"]["label"] == "Reddit"
    assert by_id["youtube_comment"]["n"] == 2


def test_weekly_stacked_collapses_reddit() -> None:
    rows = weekly_counts(_conn(), DemoFilters(sort="volume"), by_platform=True)
    reddit_weeks = [r for r in rows if r["platform"] == "Reddit"]
    assert len(reddit_weeks) == 1
    assert reddit_weeks[0]["n"] == 6


def test_leaves_and_member_filter() -> None:
    conn = _conn()
    leaves = list_leaves(conn, 1, DemoFilters(sort="volume"))
    assert [r["id"] for r in leaves] == [10, 11]
    assert leaves[0]["n_occ"] == 5
    assert leaves[1]["n_anti"] == 1
    reddit_anti = DemoFilters(anti=True, platforms=plat.keys_for_groups(["reddit"]))
    assert member_claims(conn, 10, reddit_anti) == []
    members = member_claims(conn, 11, DemoFilters(anti=True, sort="volume"))
    assert len(members) == 1
    assert members[0]["n_occ"] == 1
