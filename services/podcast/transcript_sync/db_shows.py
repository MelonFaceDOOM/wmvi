from __future__ import annotations

from psycopg2.extras import execute_values

from .format import ShowRow
from .rss_url import normalize_rss_url

_SHOWS_BY_ID_SELECT = """
SELECT id, title, rss_url, etag, last_modified, last_fetch_ts, last_http_status, last_error
FROM podcasts.shows
WHERE id = ANY(%s)
ORDER BY id
"""

_UPSERT_SHOWS_SQL = """
INSERT INTO podcasts.shows (
    title,
    rss_url,
    etag,
    last_modified,
    last_fetch_ts,
    last_http_status,
    last_error
)
VALUES %s
ON CONFLICT (rss_url) DO UPDATE SET
    title = EXCLUDED.title,
    etag = EXCLUDED.etag,
    last_modified = EXCLUDED.last_modified,
    last_fetch_ts = EXCLUDED.last_fetch_ts,
    last_http_status = EXCLUDED.last_http_status,
    last_error = EXCLUDED.last_error
RETURNING id, rss_url
"""


def _row_from_db(
    show_id,
    title,
    rss_url,
    etag,
    last_modified,
    last_fetch_ts,
    last_http_status,
    last_error,
) -> ShowRow | None:
    canonical = normalize_rss_url(str(rss_url) if rss_url is not None else None)
    if canonical is None:
        return None
    return ShowRow(
        rss_url=canonical,
        title=str(title),
        source_show_id=int(show_id),
        etag=etag,
        last_modified=last_modified,
        last_fetch_ts=last_fetch_ts,
        last_http_status=last_http_status,
        last_error=last_error,
    )


def fetch_shows_by_ids(cur, podcast_ids: list[int]) -> list[ShowRow]:
    if not podcast_ids:
        return []
    cur.execute(_SHOWS_BY_ID_SELECT, (podcast_ids,))
    rows: list[ShowRow] = []
    seen_rss: set[str] = set()
    for record in cur.fetchall():
        row = _row_from_db(*record)
        if row is None or row.rss_url in seen_rss:
            continue
        seen_rss.add(row.rss_url)
        rows.append(row)
    return rows


def upsert_shows(cur, rows: list[ShowRow]) -> dict[str, int]:
    """
    Upsert shows by canonical rss_url. Returns map rss_url -> target podcast id.
    """
    if not rows:
        return {}

    by_rss: dict[str, ShowRow] = {}
    for row in rows:
        canonical = normalize_rss_url(row.rss_url)
        if canonical is None:
            continue
        by_rss[canonical] = ShowRow(
            rss_url=canonical,
            title=row.title,
            source_show_id=row.source_show_id,
            etag=row.etag,
            last_modified=row.last_modified,
            last_fetch_ts=row.last_fetch_ts,
            last_http_status=row.last_http_status,
            last_error=row.last_error,
        )

    if not by_rss:
        return {}

    values = [
        (
            r.title,
            r.rss_url,
            r.etag,
            r.last_modified,
            r.last_fetch_ts,
            r.last_http_status,
            r.last_error,
        )
        for r in by_rss.values()
    ]
    template = "(%s, %s, %s, %s, %s::timestamptz, %s, %s)"
    execute_values(
        cur,
        _UPSERT_SHOWS_SQL,
        values,
        template=template,
        page_size=len(values),
        fetch=True,
    )
    out: dict[str, int] = {}
    for show_id, rss in cur.fetchall():
        key = normalize_rss_url(str(rss))
        if key:
            out[key] = int(show_id)
    return out
