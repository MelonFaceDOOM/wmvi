from __future__ import annotations

from .format import ShowRow

_SHOWS_SELECT = """
SELECT id, title, rss_url, etag, last_modified, last_fetch_ts, last_http_status, last_error
FROM podcasts.shows
ORDER BY id
"""

_INSERT_NEW_SHOWS_SQL = """
INSERT INTO podcasts.shows (
    id,
    title,
    rss_url,
    etag,
    last_modified,
    last_fetch_ts,
    last_http_status,
    last_error
)
OVERRIDING SYSTEM VALUE
VALUES %s
ON CONFLICT (id) DO NOTHING
RETURNING id
"""


def fetch_all_shows(cur) -> list[ShowRow]:
    cur.execute(_SHOWS_SELECT)
    rows: list[ShowRow] = []
    for (
        show_id,
        title,
        rss_url,
        etag,
        last_modified,
        last_fetch_ts,
        last_http_status,
        last_error,
    ) in cur.fetchall():
        rows.append(
            ShowRow(
                id=int(show_id),
                title=str(title),
                rss_url=rss_url,
                etag=etag,
                last_modified=last_modified,
                last_fetch_ts=last_fetch_ts,
                last_http_status=last_http_status,
                last_error=last_error,
            )
        )
    return rows


def insert_new_shows(cur, rows: list[ShowRow]) -> int:
    """Insert shows that do not yet exist in prod (by id). Returns insert count."""
    if not rows:
        return 0

    from psycopg2.extras import execute_values

    values = [
        (
            r.id,
            r.title,
            r.rss_url,
            r.etag,
            r.last_modified,
            r.last_fetch_ts,
            r.last_http_status,
            r.last_error,
        )
        for r in rows
    ]
    template = "(%s, %s, %s, %s, %s, %s::timestamptz, %s, %s)"
    execute_values(
        cur,
        _INSERT_NEW_SHOWS_SQL,
        values,
        template=template,
        page_size=len(values),
        fetch=True,
    )
    return len(cur.fetchall())
