from __future__ import annotations

from datetime import datetime

_IMPORT_STATE_ID = "global"


def get_last_imported_at(cur) -> datetime | None:
    cur.execute(
        """
        SELECT last_imported_at
        FROM sm.podcast_transcript_import_state
        WHERE id = %s
        """,
        (_IMPORT_STATE_ID,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return row[0]


def set_last_imported_at(cur, ts: datetime) -> None:
    cur.execute(
        """
        INSERT INTO sm.podcast_transcript_import_state (id, last_imported_at)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE
           SET last_imported_at = EXCLUDED.last_imported_at
        """,
        (_IMPORT_STATE_ID, ts),
    )
