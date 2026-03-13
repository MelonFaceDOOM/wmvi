"""reddit scraping was ran for a while on dev
this script will add all the dev submissions/comments to prod"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# CONFIG
# ----------------------------
DO_INSERT = False        # <-- set True to actually insert into PROD
BATCH_KEYS = 50_000      # ids per batch to diff (tune)
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


def _chunked(seq: Sequence[Any], n: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


# ----------------------------
# Common helpers (single-PK tables)
# ----------------------------

def _fetch_ids_batch(cur, *, table: str, last_id: Optional[str], limit: int) -> List[str]:
    # keyset pagination on id
    if last_id is None:
        cur.execute(f"SELECT id FROM {table} ORDER BY id LIMIT %s", (limit,))
    else:
        cur.execute(f"SELECT id FROM {table} WHERE id > %s ORDER BY id LIMIT %s", (last_id, limit))
    return [r[0] for r in cur.fetchall()]


def _existing_ids(cur, *, table: str, ids: List[str]) -> set[str]:
    if not ids:
        return set()
    cur.execute(f"SELECT id FROM {table} WHERE id = ANY(%s)", (ids,))
    return {r[0] for r in cur.fetchall()}


def _fetch_rows(cur, *, table: str, cols: List[str], ids: List[str]) -> List[tuple]:
    if not ids:
        return []
    col_sql = ", ".join(cols)
    cur.execute(f"SELECT {col_sql} FROM {table} WHERE id = ANY(%s)", (ids,))
    return cur.fetchall()


def _insert_rows(cur, *, table: str, cols: List[str], rows: List[tuple]) -> int:
    """
    Inserts rows using execute_values and ON CONFLICT DO NOTHING.
    Returns attempted rows count (not actual inserted count).
    """
    if not rows:
        return 0
    col_sql = ", ".join(cols)
    sql = f"INSERT INTO {table} ({col_sql}) VALUES %s ON CONFLICT (id) DO NOTHING"
    # execute_values uses %s placeholder for VALUES
    execute_values(cur, sql, rows, page_size=min(BATCH_INSERT, len(rows)))
    return len(rows)


def sync_table_single_pk(
    *,
    dev_cur,
    prod_cur,
    table: str,
    cols: List[str],
    label: str,
) -> Tuple[int, int]:
    """
    DEV -> PROD sync for a table with PK (id).
    Returns: (missing_total, attempted_insert_total)
    """
    missing_total = 0
    attempted_insert_total = 0
    batch_idx = 0
    last_id: Optional[str] = None

    while True:
        batch_ids = _fetch_ids_batch(dev_cur, table=table, last_id=last_id, limit=BATCH_KEYS)
        if not batch_ids:
            break

        last_id = batch_ids[-1]
        batch_idx += 1

        existing = _existing_ids(prod_cur, table=table, ids=batch_ids)
        missing = [i for i in batch_ids if i not in existing]
        missing_total += len(missing)

        if DO_INSERT and missing:
            rows = _fetch_rows(dev_cur, table=table, cols=cols, ids=missing)
            # rows already in correct tuple order
            for chunk in _chunked(rows, BATCH_INSERT):
                attempted_insert_total += _insert_rows(prod_cur, table=table, cols=cols, rows=list(chunk))

        if batch_idx % PRINT_EVERY == 0:
            scanned = batch_idx * BATCH_KEYS
            print(f"[{label}] batches={batch_idx} scanned≈{scanned:,} missing_so_far={missing_total:,}")

    return missing_total, attempted_insert_total


# ----------------------------
# Column sets (exclude generated columns)
# ----------------------------

# reddit_submission has generated: url_hash, tsv_en
REDDIT_SUBMISSION_COLS = [
    "id",
    "date_entered",
    "url",
    "domain",
    "title",
    "filtered_text",
    "permalink",
    "created_at_ts",
    "url_overridden_by_dest",
    "subreddit_id",
    "subreddit",
    "upvote_ratio",
    "score",
    "gilded",
    "num_comments",
    "num_crossposts",
    "pinned",
    "stickied",
    "over_18",
    "is_created_from_ads_ui",
    "is_self",
    "is_video",
    "media",
    "gildings",
    "all_awardings",
    "is_en",
    "selftext",
    "shared_url",
]

# reddit_comment has generated: tsv_en
REDDIT_COMMENT_COLS = [
    "id",
    "date_entered",
    "link_id",
    "parent_comment_id",
    "body",
    "filtered_text",
    "permalink",
    "created_at_ts",
    "subreddit_id",
    "subreddit_type",
    "total_awards_received",
    "subreddit",
    "score",
    "gilded",
    "stickied",
    "is_submitter",
    "gildings",
    "all_awardings",
    "is_en",
    "reply_count",
]


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

    try:
        dev_conn.autocommit = False
        prod_conn.autocommit = False

        with dev_conn.cursor() as dev_cur, prod_conn.cursor() as prod_cur:
            print(f"[mode] DO_INSERT={DO_INSERT}")

            # 1) submissions first
            miss_s, att_s = sync_table_single_pk(
                dev_cur=dev_cur,
                prod_cur=prod_cur,
                table="sm.reddit_submission",
                cols=REDDIT_SUBMISSION_COLS,
                label="submissions",
            )
            print(f"[submissions] missing_total={miss_s:,} attempted_insert={att_s:,}")
            if DO_INSERT:
                prod_conn.commit()
                print("[submissions] committed")

            # 2) comments second
            miss_c, att_c = sync_table_single_pk(
                dev_cur=dev_cur,
                prod_cur=prod_cur,
                table="sm.reddit_comment",
                cols=REDDIT_COMMENT_COLS,
                label="comments",
            )
            print(f"[comments] missing_total={miss_c:,} attempted_insert={att_c:,}")
            if DO_INSERT:
                prod_conn.commit()
                print("[comments] committed")

    finally:
        try:
            dev_conn.close()
        except Exception:
            pass
        try:
            prod_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()