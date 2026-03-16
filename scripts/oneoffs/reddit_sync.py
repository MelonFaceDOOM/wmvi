from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extensions import connection as PGConn
from psycopg2.extensions import cursor as PGCursor

from dotenv import load_dotenv

from ingestion.row_model import insert_rows_returning
from ingestion.reddit.submission import RedditSubmissionRow
from ingestion.reddit.comment import RedditCommentRow

load_dotenv()

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

DRY_RUN = False

ID_BATCH = 50_000
INSERT_BATCH = 5_000
MAX_REPLY_PASSES = 12  # replies may be deep; multiple passes let parents land first

# -----------------------------------------------------------------------------
# Credentials / connections
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class PgCreds:
    host: str
    port: str
    user: str
    password: str
    database: str
    sslmode: str = "require"


def _get_creds(prefix: str) -> PgCreds:
    return PgCreds(
        host=os.environ[f"{prefix}_PGHOST"],
        port=os.environ.get(f"{prefix}_PGPORT", "5432"),
        user=os.environ[f"{prefix}_PGUSER"],
        password=os.environ[f"{prefix}_PGPASSWORD"],
        database=os.environ[f"{prefix}_PGDATABASE"],
        sslmode=os.environ.get(f"{prefix}_PGSSLMODE", "require"),
    )


def _connect(creds: PgCreds) -> PGConn:
    return psycopg2.connect(
        host=creds.host,
        port=creds.port,
        user=creds.user,
        password=creds.password,
        dbname=creds.database,
        sslmode=creds.sslmode,
    )


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------

def _chunks(seq: Sequence[Any], n: int) -> Iterator[list[Any]]:
    for i in range(0, len(seq), n):
        yield list(seq[i : i + n])


def _dicts_from_rows(cols: Sequence[str], rows: Sequence[tuple[Any, ...]]) -> list[dict[str, Any]]:
    return [{c: v for c, v in zip(cols, r)} for r in rows]


def _fetch_existing_ids(prod_cur: PGCursor, *, table: str, ids: list[str]) -> set[str]:
    if not ids:
        return set()
    prod_cur.execute(f"SELECT id FROM {table} WHERE id = ANY(%s)", (ids,))
    return {str(r[0]) for r in prod_cur.fetchall()}


def _fetch_existing_ids_by_key(prod_cur: PGCursor, *, table: str, key_col: str, values: list[str]) -> set[str]:
    if not values:
        return set()
    prod_cur.execute(f"SELECT {key_col} FROM {table} WHERE {key_col} = ANY(%s)", (values,))
    return {str(r[0]) for r in prod_cur.fetchall()}


def _server_side_stream_rows(
    conn: PGConn,
    *,
    table: str,
    cols: Sequence[str],
    where_sql: str = "",
    params: tuple[Any, ...] = (),
    order_by: str = "id",
    itersize: int = 10_000,
) -> Iterator[dict[str, Any]]:
    """
    Stream rows from DEV without loading everything into RAM.
    """
    col_sql = ", ".join(cols)
    cur = conn.cursor(name=f"stream_{table.replace('.', '_')}_{order_by}")
    cur.itersize = itersize

    sql = f"SELECT {col_sql} FROM {table} {where_sql} ORDER BY {order_by}"
    cur.execute(sql, params)
    try:
        for tup in cur:
            yield {c: v for c, v in zip(cols, tup)}
    finally:
        cur.close()


def _coerce_submission(d: dict[str, Any]) -> dict[str, Any]:
    """
    DEV -> Row object coercions (only where DB types might not match dataclass types).
    """
    # numeric -> float
    if "upvote_ratio" in d and d["upvote_ratio"] is not None:
        d["upvote_ratio"] = float(d["upvote_ratio"])
    elif "upvote_ratio" in d and d["upvote_ratio"] is None:
        d["upvote_ratio"] = 0.0
    return d


def _coerce_comment(d: dict[str, Any]) -> dict[str, Any]:
    return d


# -----------------------------------------------------------------------------
# Robust insert: never lose whole chunks
# -----------------------------------------------------------------------------

def _robust_insert_rows(
    prod_conn: PGConn,
    prod_cur: PGCursor,
    *,
    rows: list[Any],  # InsertableRow instances
    batch_size: int,
    label: str,
) -> int:
    """
    Tries to insert rows in batches. If a batch fails (FK / NOT NULL / etc),
    rollback and bisect until it finds the bad row(s), logs and skips them.
    Returns count inserted (not attempted).
    """
    inserted_total = 0
    if not rows:
        return 0

    def _try_batch(batch: list[Any]) -> int:
        nonlocal inserted_total
        if not batch:
            return 0

        try:
            ins, _skip, _keys = insert_rows_returning(rows=batch, cur=prod_cur, page_size=min(batch_size, len(batch)))
            inserted_total += ins
            return ins
        except Exception as e:
            prod_conn.rollback()

            if len(batch) == 1:
                r = batch[0]
                # high signal row dump (don’t print huge JSONs)
                print(f"[{label}] SKIP 1 row due to insert error: {type(e).__name__}: {e}")
                print(f"[{label}]   row_type={type(r).__name__} pk={getattr(r, 'id', None)}")
                return 0

            mid = len(batch) // 2
            _try_batch(batch[:mid])
            _try_batch(batch[mid:])
            return 0

    # chunk the overall set to keep recursion depth reasonable
    for chunk in _chunks(rows, batch_size):
        _try_batch(chunk)

    # only commit if caller is doing real inserts
    prod_conn.commit()
    return inserted_total


# -----------------------------------------------------------------------------
# Phase 1: submissions
# -----------------------------------------------------------------------------

def sync_submissions(*, dev_conn: PGConn, prod_conn: PGConn) -> tuple[int, int, int]:
    """
    Returns (dev_total, missing_in_prod, inserted_prod)
    """
    cols = list(RedditSubmissionRow.cols())
    dev_total = 0
    missing_total = 0
    inserted_total = 0

    prod_cur = prod_conn.cursor()
    try:
        buf: list[dict[str, Any]] = []

        for d in _server_side_stream_rows(
            dev_conn, table="sm.reddit_submission", cols=cols, order_by="id"
        ):
            dev_total += 1
            buf.append(d)
            if len(buf) < ID_BATCH:
                continue

            miss, ins = _process_submission_buffer(buf, prod_conn=prod_conn, prod_cur=prod_cur)
            missing_total += miss
            inserted_total += ins
            buf = []

        if buf:
            miss, ins = _process_submission_buffer(buf, prod_conn=prod_conn, prod_cur=prod_cur)
            missing_total += miss
            inserted_total += ins

        return dev_total, missing_total, inserted_total
    finally:
        prod_cur.close()


def _process_submission_buffer(
    buf: list[dict[str, Any]],
    *,
    prod_conn: PGConn,
    prod_cur: PGCursor,
) -> tuple[int, int]:
    """
    Returns (missing_in_prod_for_this_buf, inserted_for_this_buf)
    """
    ids = [str(d["id"]) for d in buf if d.get("id")]
    existing = _fetch_existing_ids(prod_cur, table="sm.reddit_submission", ids=ids)
    missing = [d for d in buf if str(d.get("id")) not in existing]
    missing_count = len(missing)

    if DRY_RUN or not missing:
        return missing_count, 0

    rows = [RedditSubmissionRow(**_coerce_submission(dict(d))) for d in missing]
    inserted = _robust_insert_rows(
        prod_conn, prod_cur, rows=rows, batch_size=INSERT_BATCH, label="reddit_submission"
    )
    return missing_count, inserted


# -----------------------------------------------------------------------------
# Phase 2+: comments in passes
# -----------------------------------------------------------------------------

def sync_comments_phased(
    *,
    dev_conn: PGConn,
    prod_conn: PGConn,
) -> tuple[int, int, int]:
    """
    Returns (dev_total, missing_in_prod, inserted_prod)

    Phases:
      Pass 0: parent_comment_id IS NULL AND link_id exists in prod submissions
      Pass 1..N: parent_comment_id IS NOT NULL AND link_id exists AND parent exists in prod comments
    """
    cols = list(RedditCommentRow.cols())
    dev_total = 0
    missing_total = 0
    inserted_total = 0

    prod_cur = prod_conn.cursor()
    try:
        # pass 0: top-level only
        dt, miss, ins = _comment_pass(
            dev_conn=dev_conn,
            prod_conn=prod_conn,
            prod_cur=prod_cur,
            cols=cols,
            pass_label="pass0_top_level",
            where_sql="WHERE parent_comment_id IS NULL",
            require_parent_exists=False,
        )
        dev_total += dt
        missing_total += miss
        inserted_total += ins

        # reply passes
        for i in range(1, MAX_REPLY_PASSES + 1):
            dt, miss, ins = _comment_pass(
                dev_conn=dev_conn,
                prod_conn=prod_conn,
                prod_cur=prod_cur,
                cols=cols,
                pass_label=f"pass{i}_replies",
                where_sql="WHERE parent_comment_id IS NOT NULL",
                require_parent_exists=True,
            )
            dev_total += dt
            missing_total += miss
            inserted_total += ins

            print(f"[reddit_comment] replies pass {i}: inserted={ins} missing_seen={miss}")
            if ins == 0:
                break

        return dev_total, missing_total, inserted_total
    finally:
        prod_cur.close()


def _comment_pass(
    *,
    dev_conn: PGConn,
    prod_conn: PGConn,
    prod_cur: PGCursor,
    cols: list[str],
    pass_label: str,
    where_sql: str,
    require_parent_exists: bool,
) -> tuple[int, int, int]:
    """
    One streaming pass over DEV comments with prefilters to satisfy FKs.
    Returns (dev_seen, missing_seen, inserted)
    """
    dev_seen = 0
    missing_seen = 0
    inserted = 0

    buf: list[dict[str, Any]] = []

    for d in _server_side_stream_rows(
        dev_conn,
        table="sm.reddit_comment",
        cols=cols,
        where_sql=where_sql,
        order_by="id",
    ):
        dev_seen += 1
        buf.append(d)
        if len(buf) < ID_BATCH:
            continue

        miss, ins = _process_comment_buffer(
            buf,
            prod_conn=prod_conn,
            prod_cur=prod_cur,
            require_parent_exists=require_parent_exists,
            label=pass_label,
        )
        missing_seen += miss
        inserted += ins
        buf = []

    if buf:
        miss, ins = _process_comment_buffer(
            buf,
            prod_conn=prod_conn,
            prod_cur=prod_cur,
            require_parent_exists=require_parent_exists,
            label=pass_label,
        )
        missing_seen += miss
        inserted += ins

    return dev_seen, missing_seen, inserted


def _process_comment_buffer(
    buf: list[dict[str, Any]],
    *,
    prod_conn: PGConn,
    prod_cur: PGCursor,
    require_parent_exists: bool,
    label: str,
) -> tuple[int, int]:
    """
    For a buffer of DEV comment dicts:
      1) keep only ids missing in prod
      2) keep only rows whose link_id exists in prod submissions
      3) if require_parent_exists: keep only rows whose parent exists in prod comments
      4) robust insert the eligible rows
    Returns (missing_seen, inserted)
    """
    ids = [str(d["id"]) for d in buf if d.get("id")]
    existing = _fetch_existing_ids(prod_cur, table="sm.reddit_comment", ids=ids)
    missing = [d for d in buf if str(d.get("id")) not in existing]
    missing_seen = len(missing)

    if DRY_RUN or not missing:
        return missing_seen, 0

    # FK 1: submission must exist
    link_ids = [str(d["link_id"]) for d in missing if d.get("link_id")]
    link_exists = _fetch_existing_ids_by_key(prod_cur, table="sm.reddit_submission", key_col="id", values=link_ids)
    eligible = [d for d in missing if str(d.get("link_id")) in link_exists]

    # FK 2: parent must exist (for replies pass)
    if require_parent_exists:
        parent_ids = [str(d["parent_comment_id"]) for d in eligible if d.get("parent_comment_id")]
        parent_exists = _fetch_existing_ids_by_key(prod_cur, table="sm.reddit_comment", key_col="id", values=parent_ids)
        eligible = [d for d in eligible if str(d.get("parent_comment_id")) in parent_exists]

    if not eligible:
        return missing_seen, 0

    rows = [RedditCommentRow(**_coerce_comment(dict(d))) for d in eligible]
    ins = _robust_insert_rows(prod_conn, prod_cur, rows=rows, batch_size=INSERT_BATCH, label=f"reddit_comment:{label}")
    return missing_seen, ins


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    dev = _get_creds("DEV")
    prod = _get_creds("PROD")

    if dev.host == prod.host and dev.database == prod.database:
        raise RuntimeError("DEV appears to point at PROD (same host+db). Refusing to continue.")

    print("[sync] DEV :", f"{dev.host}:{dev.port}/{dev.database}")
    print("[sync] PROD:", f"{prod.host}:{prod.port}/{prod.database}")
    print("[sync] DRY_RUN =", DRY_RUN)

    dev_conn = _connect(dev)
    prod_conn = _connect(prod)

    try:
        # submissions
        s_dev, s_miss, s_ins = sync_submissions(dev_conn=dev_conn, prod_conn=prod_conn)
        print()
        print("[reddit_submission]")
        print(f"  dev_total      : {s_dev:,}")
        print(f"  missing_in_prod: {s_miss:,}")
        print(f"  inserted_prod  : {s_ins:,}")

        # comments (phased)
        c_dev, c_miss, c_ins = sync_comments_phased(dev_conn=dev_conn, prod_conn=prod_conn)
        print()
        print("[reddit_comment]")
        print(f"  dev_total_seen : {c_dev:,}  (note: counts across passes; not unique)")
        print(f"  missing_seen   : {c_miss:,} (note: counts across passes; not unique)")
        print(f"  inserted_prod  : {c_ins:,}")

        if DRY_RUN:
            print("\n[sync] DRY_RUN: no inserts performed.")
        else:
            print("\n[sync] Done (inserts committed).")

    finally:
        dev_conn.close()
        prod_conn.close()


if __name__ == "__main__":
    main()