from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# CONFIG
# ----------------------------
DO_INSERT = False        # <-- set True to actually insert into PROD
BATCH_KEYS = 25_000      # keys per round-trip for diffing (tune)
BATCH_INSERT = 5_000     # rows per execute_values insert (tune)
PRINT_EVERY = 10         # progress print cadence (batches)

# ----------------------------
# Creds + connections
# ----------------------------

@dataclass(frozen=True)
class PgCreds:
    host: str
    port: str
    user: str
    password: str
    database: str
    sslmode: str = "require"


def _get_creds(prefix: str, db_override: Optional[str] = None) -> PgCreds:
    host = os.environ[f"{prefix}_PGHOST"]
    port = os.environ.get(f"{prefix}_PGPORT", "5432")
    user = os.environ[f"{prefix}_PGUSER"]
    password = os.environ[f"{prefix}_PGPASSWORD"]
    database = db_override or os.environ[f"{prefix}_PGDATABASE"]
    sslmode = os.environ.get(f"{prefix}_PGSSLMODE", "require")
    return PgCreds(host=host, port=port, user=user, password=password, database=database, sslmode=sslmode)


def _connect(creds: PgCreds) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=creds.host,
        port=creds.port,
        user=creds.user,
        password=creds.password,
        dbname=creds.database,
        sslmode=creds.sslmode,
    )


def _chunked(seq: Sequence, n: int) -> Iterable[Sequence]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


# ----------------------------
# Video sync
# ----------------------------

VIDEO_COLS = [
    "video_id",
    "url",
    "title",
    "description",
    "created_at_ts",
    "channel_id",
    "channel_title",
    "duration_iso",
    "view_count",
    "like_count",
    "comment_count",
    "is_en",
    "transcript",
    "transcript_updated_at",
    "transcription_started_at",
    "duration_seconds",
]

VIDEO_INSERT_SQL = f"""
INSERT INTO youtube.video ({", ".join(VIDEO_COLS)})
VALUES %s
ON CONFLICT (video_id) DO NOTHING
"""


def _dev_fetch_video_ids_batch(dev_cur, last_video_id: Optional[str], limit: int) -> List[str]:
    # keyset pagination
    if last_video_id is None:
        dev_cur.execute(
            "SELECT video_id FROM youtube.video ORDER BY video_id LIMIT %s",
            (limit,),
        )
    else:
        dev_cur.execute(
            "SELECT video_id FROM youtube.video WHERE video_id > %s ORDER BY video_id LIMIT %s",
            (last_video_id, limit),
        )
    return [r[0] for r in dev_cur.fetchall()]


def _prod_existing_video_ids(prod_cur, ids: List[str]) -> set[str]:
    if not ids:
        return set()
    prod_cur.execute("SELECT video_id FROM youtube.video WHERE video_id = ANY(%s)", (ids,))
    return {r[0] for r in prod_cur.fetchall()}


def _dev_fetch_videos_rows(dev_cur, ids: List[str]) -> List[tuple]:
    if not ids:
        return []
    sql = f"SELECT {', '.join(VIDEO_COLS)} FROM youtube.video WHERE video_id = ANY(%s)"
    dev_cur.execute(sql, (ids,))
    return dev_cur.fetchall()


def sync_videos(dev_cur, prod_cur) -> Tuple[int, int]:
    """
    Returns: (missing_count, attempted_insert_count)
    """
    missing_total = 0
    attempted_insert_total = 0

    last_id: Optional[str] = None
    batch_idx = 0

    while True:
        batch_ids = _dev_fetch_video_ids_batch(dev_cur, last_id, BATCH_KEYS)
        if not batch_ids:
            break
        last_id = batch_ids[-1]
        batch_idx += 1

        existing = _prod_existing_video_ids(prod_cur, batch_ids)
        missing = [vid for vid in batch_ids if vid not in existing]
        missing_total += len(missing)

        if DO_INSERT and missing:
            rows = _dev_fetch_videos_rows(dev_cur, missing)
            for chunk in _chunked(rows, BATCH_INSERT):
                execute_values(prod_cur, VIDEO_INSERT_SQL, list(chunk), page_size=BATCH_INSERT)
                attempted_insert_total += len(chunk)

        if batch_idx % PRINT_EVERY == 0:
            print(f"[videos] batches={batch_idx} scanned={batch_idx*BATCH_KEYS:,} missing_so_far={missing_total:,}")

    return missing_total, attempted_insert_total


# ----------------------------
# Comment sync
# ----------------------------

COMMENT_COLS = [
    "video_id",
    "comment_id",
    "comment_url",
    "text",
    "filtered_text",
    "created_at_ts",
    "like_count",
    "is_en",
    "parent_comment_id",
    "reply_count",
]

COMMENT_INSERT_SQL = f"""
INSERT INTO youtube.comment ({", ".join(COMMENT_COLS)})
VALUES %s
ON CONFLICT (video_id, comment_id) DO NOTHING
"""


def _dev_fetch_comment_keys_batch(
    dev_cur,
    last_key: Optional[Tuple[str, str]],
    limit: int,
) -> List[Tuple[str, str]]:
    # Keyset pagination on (video_id, comment_id)
    if last_key is None:
        dev_cur.execute(
            """
            SELECT video_id, comment_id
            FROM youtube.comment
            ORDER BY video_id, comment_id
            LIMIT %s
            """,
            (limit,),
        )
    else:
        dev_cur.execute(
            """
            SELECT video_id, comment_id
            FROM youtube.comment
            WHERE (video_id, comment_id) > (%s, %s)
            ORDER BY video_id, comment_id
            LIMIT %s
            """,
            (last_key[0], last_key[1], limit),
        )
    return [(r[0], r[1]) for r in dev_cur.fetchall()]


def _prod_existing_comment_keys(prod_cur, keys: List[Tuple[str, str]]) -> set[Tuple[str, str]]:
    if not keys:
        return set()

    vids = [k[0] for k in keys]
    cids = [k[1] for k in keys]

    prod_cur.execute(
        """
        SELECT c.video_id, c.comment_id
        FROM youtube.comment c
        JOIN unnest(%s::text[], %s::text[]) AS x(video_id, comment_id)
          ON c.video_id = x.video_id AND c.comment_id = x.comment_id
        """,
        (vids, cids),
    )
    return {(r[0], r[1]) for r in prod_cur.fetchall()}


def _dev_fetch_comments_rows(dev_cur, keys: List[Tuple[str, str]]) -> List[tuple]:
    if not keys:
        return []
    vids = [k[0] for k in keys]
    cids = [k[1] for k in keys]
    sql = f"""
        SELECT c.{", c.".join(COMMENT_COLS)}
        FROM youtube.comment c
        JOIN unnest(%s::text[], %s::text[]) AS x(video_id, comment_id)
          ON c.video_id = x.video_id AND c.comment_id = x.comment_id
    """
    dev_cur.execute(sql, (vids, cids))
    return dev_cur.fetchall()


def sync_comments(dev_cur, prod_cur) -> Tuple[int, int]:
    """
    Returns: (missing_count, attempted_insert_count)
    """
    missing_total = 0
    attempted_insert_total = 0

    last_key: Optional[Tuple[str, str]] = None
    batch_idx = 0

    while True:
        batch_keys = _dev_fetch_comment_keys_batch(dev_cur, last_key, BATCH_KEYS)
        if not batch_keys:
            break
        last_key = batch_keys[-1]
        batch_idx += 1

        existing = _prod_existing_comment_keys(prod_cur, batch_keys)
        missing = [k for k in batch_keys if k not in existing]
        missing_total += len(missing)

        if DO_INSERT and missing:
            rows = _dev_fetch_comments_rows(dev_cur, missing)
            for chunk in _chunked(rows, BATCH_INSERT):
                execute_values(prod_cur, COMMENT_INSERT_SQL, list(chunk), page_size=BATCH_INSERT)
                attempted_insert_total += len(chunk)

        if batch_idx % PRINT_EVERY == 0:
            scanned = batch_idx * BATCH_KEYS
            print(f"[comments] batches={batch_idx} scanned≈{scanned:,} missing_so_far={missing_total:,}")

    return missing_total, attempted_insert_total


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    dev = _get_creds("DEV")
    prod = _get_creds("PROD")

    if dev.host == prod.host and dev.database == prod.database:
        raise RuntimeError("DEV appears to point at PROD (same host+db). Refusing to continue.")

    dev_conn = _connect(dev)
    prod_conn = _connect(prod)
    with dev_conn.cursor() as dev_cur, prod_conn.cursor() as prod_cur:
        dev_cur.execute("SELECT COUNT(*) FROM youtube.comment")
        dev_count = dev_cur.fetchone()[0]
        prod_cur.execute("SELECT COUNT(*) FROM youtube.comment")
        prod_count = prod_cur.fetchone()[0]
        print(f"DEV: {dev_count} PROD: {prod_count}")

    #
    # try:
    #     dev_conn.autocommit = False
    #     prod_conn.autocommit = False
    #
    #     with dev_conn.cursor() as dev_cur, prod_conn.cursor() as prod_cur:
    #         print(f"[mode] DO_INSERT={DO_INSERT}")
    #
    #         # 1) videos first
    #         missing_v, attempted_v = sync_videos(dev_cur, prod_cur)
    #         print(f"[videos] missing_total={missing_v:,} attempted_insert={attempted_v:,}")
    #         if DO_INSERT:
    #             prod_conn.commit()
    #             print("[videos] committed")
    #
    #         # 2) comments second
    #         missing_c, attempted_c = sync_comments(dev_cur, prod_cur)
    #         print(f"[comments] missing_total={missing_c:,} attempted_insert={attempted_c:,}")
    #         if DO_INSERT:
    #             prod_conn.commit()
    #             print("[comments] committed")
    #
    # finally:
    #     try:
    #         dev_conn.close()
    #     except Exception:
    #         pass
    #     try:
    #         prod_conn.close()
    #     except Exception:
    #         pass


if __name__ == "__main__":
    main()