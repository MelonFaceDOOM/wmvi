from __future__ import annotations

from datetime import datetime


def get_last_imported_bundle_at(cur) -> datetime | None:
    cur.execute(
        """
        SELECT last_imported_bundle_at
        FROM sm.content_sync_state
        WHERE id = 'global'
        """
    )
    row = cur.fetchone()
    if not row:
        return None
    return row[0]


def set_last_imported_bundle_at(cur, ts: datetime) -> None:
    cur.execute(
        """
        INSERT INTO sm.content_sync_state (id, last_imported_bundle_at)
        VALUES ('global', %s)
        ON CONFLICT (id) DO UPDATE
        SET last_imported_bundle_at = EXCLUDED.last_imported_bundle_at
        """,
        (ts,),
    )
