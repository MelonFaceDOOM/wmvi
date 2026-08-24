"""Read-only queries against measles2_demo.sqlite."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal

from apps.claims.demo import ANTI_CUTOFF
from apps.claims.demo import platforms as plat

TRENDING_DAYS = 90
SortName = Literal["trending", "volume"]


@dataclass(frozen=True)
class DemoFilters:
    anti: bool = False
    platforms: tuple[str, ...] = ()
    sort: SortName = "trending"


def connect(bundle: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{Path(bundle).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def meta(conn: sqlite3.Connection) -> dict[str, str]:
    return {str(r["key"]): str(r["value"]) for r in conn.execute("SELECT key, value FROM meta")}


def ts_max_date(conn: sqlite3.Connection) -> str:
    return (meta(conn).get("ts_max") or "")[:10]


def trending_cutoff_date(conn: sqlite3.Connection) -> str | None:
    raw = ts_max_date(conn)
    if len(raw) < 10:
        return None
    try:
        end = date.fromisoformat(raw[:10])
    except ValueError:
        return None
    return (end - timedelta(days=TRENDING_DAYS)).isoformat()


def _since_date(conn: sqlite3.Connection, filters: DemoFilters) -> str | None:
    if filters.sort == "trending":
        return trending_cutoff_date(conn)
    return None


def _filter_clause(
    filters: DemoFilters, alias: str = "o", *, since: str | None = None
) -> tuple[str, list[Any]]:
    parts: list[str] = []
    params: list[Any] = []
    if filters.anti:
        parts.append(
            f"AND {alias}.alignment IS NOT NULL AND {alias}.alignment <= {ANTI_CUTOFF}"
        )
    if filters.platforms:
        placeholders = ",".join("?" for _ in filters.platforms)
        parts.append(f"AND {alias}.platform IN ({placeholders})")
        params.extend(filters.platforms)
    if since:
        parts.append(
            f"AND {alias}.ts IS NOT NULL AND substr({alias}.ts, 1, 10) >= ?"
        )
        params.append(since)
    return (" ".join(parts), params)


def _int_fields(row: dict[str, Any], *keys: str) -> dict[str, Any]:
    out = dict(row)
    for key in keys:
        if key in out:
            out[key] = int(out[key] or 0)
    return out


def available_groups(conn: sqlite3.Connection) -> list[str]:
    keys = [
        str(r["platform"] or "unknown")
        for r in conn.execute("SELECT DISTINCT platform FROM occurrences")
    ]
    return plat.groups_from_keys(keys)


def dashboard_stats(conn: sqlite3.Connection, filters: DemoFilters) -> dict[str, Any]:
    extra, params = _filter_clause(filters, since=_since_date(conn, filters))
    n_posts = conn.execute(
        f"SELECT COUNT(DISTINCT post_id) FROM occurrences o "
        f"WHERE post_id IS NOT NULL AND post_id != '' {extra}",
        params,
    ).fetchone()[0]
    n_occ = conn.execute(
        f"SELECT COUNT(*) FROM occurrences o WHERE 1=1 {extra}",
        params,
    ).fetchone()[0]
    n_claims = conn.execute(
        f"SELECT COUNT(DISTINCT claim_idx) FROM occurrences o WHERE 1=1 {extra}",
        params,
    ).fetchone()[0]
    bar_filters = DemoFilters(anti=filters.anti, platforms=(), sort=filters.sort)
    return {
        "n_posts": int(n_posts or 0),
        "n_occurrences": int(n_occ or 0),
        "n_claims": int(n_claims or 0),
        "platforms": platform_counts(conn, filters=bar_filters),
        "meta": meta(conn),
    }


def _list_order(filters: DemoFilters) -> str:
    if filters.sort == "trending":
        return "n_90d DESC, n_occ DESC, n.id"
    return "n_occ DESC, n.id"


def _occ_metrics_sql(alias: str = "o") -> str:
    return f"""
        COUNT({alias}.id) AS n_occ,
        COALESCE(SUM(CASE
            WHEN {alias}.ts IS NOT NULL AND substr({alias}.ts, 1, 10) >= ?
            THEN 1 ELSE 0 END), 0) AS n_90d,
        COALESCE(SUM(CASE
            WHEN {alias}.alignment IS NOT NULL AND {alias}.alignment <= {ANTI_CUTOFF}
            THEN 1 ELSE 0 END), 0) AS n_anti
    """


def _cutoff_or_never(conn: sqlite3.Connection) -> str:
    return trending_cutoff_date(conn) or "9999-12-31"


def list_narratives(conn: sqlite3.Connection, filters: DemoFilters) -> list[dict[str, Any]]:
    extra, fparams = _filter_clause(filters, since=_since_date(conn, filters))
    cutoff = _cutoff_or_never(conn)
    rows = conn.execute(
        f"""
        SELECT n.id, n.title, n.blurb,
               COUNT(DISTINCT o.leaf_id) AS n_leaves,
               {_occ_metrics_sql()}
        FROM narratives n
        LEFT JOIN occurrences o ON o.narrative_id = n.id {extra}
        WHERE n.id >= 0
        GROUP BY n.id
        HAVING n_occ > 0
        ORDER BY {_list_order(filters)}
        """,
        [cutoff, *fparams],
    )
    return [_int_fields(dict(r), "id", "n_leaves", "n_occ", "n_90d", "n_anti") for r in rows]


def narrative_row(conn: sqlite3.Connection, narrative_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM narratives WHERE id = ?", (int(narrative_id),)).fetchone()
    return dict(r) if r else None


def _leaf_order(filters: DemoFilters) -> str:
    if filters.sort == "trending":
        return "n_90d DESC, n_occ DESC, l.id"
    return "n_occ DESC, l.id"


def list_leaves(
    conn: sqlite3.Connection, narrative_id: int, filters: DemoFilters
) -> list[dict[str, Any]]:
    extra, fparams = _filter_clause(filters, since=_since_date(conn, filters))
    cutoff = _cutoff_or_never(conn)
    rows = conn.execute(
        f"""
        SELECT l.id, l.title, l.blurb, l.narrative_id,
               {_occ_metrics_sql()}
        FROM leaves l
        LEFT JOIN occurrences o ON o.leaf_id = l.id {extra}
        WHERE l.narrative_id = ?
        GROUP BY l.id
        HAVING n_occ > 0
        ORDER BY {_leaf_order(filters)}
        """,
        [cutoff, *fparams, int(narrative_id)],
    )
    return [
        _int_fields(dict(r), "id", "narrative_id", "n_occ", "n_90d", "n_anti") for r in rows
    ]


def leaf_row(conn: sqlite3.Connection, leaf_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM leaves WHERE id = ?", (int(leaf_id),)).fetchone()
    return dict(r) if r else None


def weekly_counts(
    conn: sqlite3.Connection,
    filters: DemoFilters,
    *,
    narrative_id: int | None = None,
    leaf_id: int | None = None,
    by_platform: bool = False,
) -> list[dict[str, Any]]:
    extra, fparams = _filter_clause(filters, since=_since_date(conn, filters))
    where = ["week IS NOT NULL", "week != ''"]
    params: list[Any] = []
    if narrative_id is not None:
        where.append("narrative_id = ?")
        params.append(int(narrative_id))
    if leaf_id is not None:
        where.append("leaf_id = ?")
        params.append(int(leaf_id))
    dims = "week, platform" if by_platform else "week"
    sql = f"""
        SELECT {dims}, COUNT(*) AS n
        FROM occurrences o
        WHERE {' AND '.join(where)} {extra}
        GROUP BY {dims}
        ORDER BY week
    """
    rows = [dict(r) for r in conn.execute(sql, [*params, *fparams])]
    if by_platform:
        return plat.collapse_weekly(rows)
    return [{"week": r["week"], "n": int(r["n"])} for r in rows]


def platform_counts(
    conn: sqlite3.Connection,
    filters: DemoFilters,
    *,
    leaf_id: int | None = None,
    narrative_id: int | None = None,
) -> list[dict[str, Any]]:
    extra, fparams = _filter_clause(filters, since=_since_date(conn, filters))
    where = ["1=1"]
    params: list[Any] = []
    if leaf_id is not None:
        where.append("leaf_id = ?")
        params.append(int(leaf_id))
    if narrative_id is not None:
        where.append("narrative_id = ?")
        params.append(int(narrative_id))
    sql = f"""
        SELECT platform, COUNT(*) AS n FROM occurrences o
        WHERE {' AND '.join(where)} {extra}
        GROUP BY platform ORDER BY n DESC
    """
    raw = [
        {"platform": r["platform"] or "unknown", "n": int(r["n"])}
        for r in conn.execute(sql, [*params, *fparams])
    ]
    return plat.collapse_counts(raw)


def member_claims(
    conn: sqlite3.Connection, leaf_id: int, filters: DemoFilters, limit: int = 50
) -> list[dict[str, Any]]:
    extra, fparams = _filter_clause(
        filters, alias="o", since=_since_date(conn, filters)
    )
    rows = conn.execute(
        f"""
        SELECT c.idx, c.claim_text, COUNT(o.id) AS n_occ
        FROM claims c
        JOIN occurrences o ON o.claim_idx = c.idx
        WHERE c.leaf_id = ? {extra}
        GROUP BY c.idx
        ORDER BY n_occ DESC, c.idx
        LIMIT ?
        """,
        [int(leaf_id), *fparams, int(limit)],
    )
    return [_int_fields(dict(r), "idx", "n_occ") for r in rows]


def claim_posts(
    conn: sqlite3.Connection, claim_idx: int, filters: DemoFilters, limit: int = 20
) -> list[dict[str, Any]]:
    extra, fparams = _filter_clause(filters, since=_since_date(conn, filters))
    rows = conn.execute(
        f"""
        SELECT ts, platform, alignment, url, post_id, snippet
        FROM occurrences o
        WHERE claim_idx = ? {extra}
        ORDER BY ts DESC
        LIMIT ?
        """,
        [int(claim_idx), *fparams, int(limit)],
    )
    return [dict(r) for r in rows]
