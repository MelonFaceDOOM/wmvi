from __future__ import annotations

import argparse
import os
import sys
import tempfile
from typing import Tuple

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def db_creds_from_env(prefix: str, db_override: str | None = None) -> str:
    """
    Build a DSN from env vars like DEV_PGHOST / PROD_PGHOST, etc.

    Required:
        {prefix}PGHOST
        {prefix}PGUSER
        {prefix}PGPASSWORD

    Optional:
        {prefix}PGPORT (default '5432')
        {prefix}PGDATABASE (default from env unless db_override provided)
        {prefix}PGSSLMODE (default 'require')
    """
    host = os.environ[f"{prefix}_PGHOST"]
    port = os.environ.get(f"{prefix}_PGPORT", "5432")
    user = os.environ[f"{prefix}_PGUSER"]
    pwd = os.environ[f"{prefix}_PGPASSWORD"]
    db = db_override or os.environ[f"{prefix}_PGDATABASE"]
    ssl = os.environ.get(f"{prefix}_PGSSLMODE", "require")
    return f"host={host} port={port} dbname={db} user={user} password={pwd} sslmode={ssl}"


def connect_from_prefix(prefix: str) -> psycopg2.extensions.connection:
    dsn = db_creds_from_env(prefix)
    return psycopg2.connect(dsn)


# Order matters because of FKs and triggers
TABLES_IN_ORDER = [
    # taxonomy / scrape
    ("taxonomy", "vaccine_term", False),  # GENERATED ALWAYS id
    ("scrape", "job", False),

    # core registry
    ("sm", "post_registry", False),

    # social media (triggers will fire, but ON CONFLICT DO NOTHING keeps registry consistent)
    ("sm", "tweet", False),
    ("sm", "reddit_submission", False),
    ("sm", "reddit_comment", False),
    ("sm", "telegram_post", False),
    ("sm", "youtube_video", False),
    ("sm", "youtube_comment", False),

    # podcasts
    ("podcasts", "shows", True),   # GENERATED ALWAYS id
    ("podcasts", "episodes", False),
    ("podcasts", "transcript_segments", False),

    # link tables (depend on registry / taxonomy / podcast tables)
    ("scrape", "post_scrape", False),
    ("matches", "post_term_match", False),
]


def copy_table(
    src_conn: psycopg2.extensions.connection,
    dst_conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    override_identity: bool = False,  # kept for signature compatibility (not used by COPY)
    truncate_first: bool = False,
) -> Tuple[int, int]:
    """
    Copy all rows from src.schema.table -> dst.schema.table.

    - Uses COPY BINARY via a temporary file (streaming, so we don't hold everything in RAM).
    - If truncate_first=True, TRUNCATEs the destination table before copy.
    - Returns (rows_copied, 0). The second value is kept for consistency with
      other insert-(inserted, skipped) style APIs.
    """
    fq = f"{schema}.{table}"

    with src_conn.cursor() as src_cur, dst_conn.cursor() as dst_cur:
        if truncate_first:
            # CASCADE so that dependent rows (FKs, etc.) are removed too.
            dst_cur.execute(f"TRUNCATE TABLE {fq} CASCADE")

        # COPY TO STDOUT streams rows out of the source table into a file.
        src_sql = f"COPY {fq} TO STDOUT WITH (FORMAT binary)"

        # Temporary file avoids loading the whole table into memory.
        with tempfile.TemporaryFile() as tmp:
            src_cur.copy_expert(src_sql, tmp)

            # Rewind the file to the beginning before reading from it.
            tmp.seek(0)

            # COPY FROM STDIN consumes the binary stream and inserts into the dest table.
            # Note: OVERRIDING SYSTEM VALUE is only valid for INSERT, not COPY.
            dst_sql = f"COPY {fq} FROM STDIN WITH (FORMAT binary)"
            dst_cur.copy_expert(dst_sql, tmp)

        # After bulk loading, keep identity/serial sequences in sync for tables that have one.
        sync_id_sequence(dst_conn, schema, table)

        dst_conn.commit()

    # Get row count from destination to report what we copied.
    with dst_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {fq}")
        (cnt,) = cur.fetchone()

    return cnt, 0


def sync_id_sequence(conn: psycopg2.extensions.connection, schema: str, table: str) -> None:
    """
    If the table has an identity/serial sequence on column 'id', set that sequence so that
    the next nextval() call continues after the current MAX(id).

    - If the table is empty, reset sequence to 1 (with is_called = false),
      so the first nextval() returns 1.
    """
    fq = f"{schema}.{table}"
    pk_col = "id"

    with conn.cursor() as cur:
        # Only attempt to sync sequences for tables that *actually* have an "id" column.
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name   = %s
              AND column_name  = %s
            """,
            (schema, table, pk_col),
        )
        if cur.fetchone() is None:
            return  # no id column -> nothing to sync

        # pg_get_serial_sequence returns the sequence name if the column
        # is backed by a serial/identity sequence; NULL otherwise.
        cur.execute("SELECT pg_get_serial_sequence(%s, %s)", (fq, pk_col))
        row = cur.fetchone()
        if not row or row[0] is None:
            # No serial/identity sequence backing this column.
            return

        seq_name = row[0]

        # COALESCE(MAX(id), 0) lets us handle empty tables easily.
        cur.execute(f"SELECT COALESCE(MAX({pk_col}), 0) FROM {fq}")
        (max_id,) = cur.fetchone()
        max_id = int(max_id or 0)

        if max_id <= 0:
            # Empty table: reset sequence so nextval() returns 1.
            # setval(seq, 1, false) => next nextval() will return 1.
            cur.execute("SELECT setval(%s, %s, false)", (seq_name, 1))
        else:
            # Non-empty: set sequence to max_id so nextval() returns max_id+1.
            # setval(seq, value, is_called=true) => next nextval() => value+1.
            cur.execute("SELECT setval(%s, %s, true)", (seq_name, max_id))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Clone application tables from a source DB to a destination DB "
            "using env prefixes (e.g. DEV_, PROD_). "
            "Assumes schema already exists on the destination."
        )
    )
    parser.add_argument(
        "--src-prefix",
        default="DEV",
        help="Environment prefix for source DB (default: DEV)",
    )
    parser.add_argument(
        "--dst-prefix",
        default="PROD",
        help="Environment prefix for destination DB (default: PROD)",
    )
    parser.add_argument(
        "--truncate-dst",
        action="store_true",
        help="TRUNCATE each destination table before copying.",
    )

    args = parser.parse_args(argv)

    src_prefix = args.src_prefix
    dst_prefix = args.dst_prefix

    print(f"[db_clone] Source prefix: {src_prefix}")
    print(f"[db_clone] Dest   prefix: {dst_prefix}")
    print(f"[db_clone] TRUNCATE dst tables first: {args.truncate_dst}")

    try:
        src_conn = connect_from_prefix(src_prefix)
        dst_conn = connect_from_prefix(dst_prefix)
    except KeyError as e:
        print(f"Missing environment variable for prefix: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        total_tables = len(TABLES_IN_ORDER)
        for idx, (schema, table, override_identity) in enumerate(TABLES_IN_ORDER, start=1):
            print(
                f"[{idx}/{total_tables}] Copying {schema}.{table} "
                f"(override_identity={override_identity}) ...",
                flush=True,
            )
            rows_copied, _ = copy_table(
                src_conn,
                dst_conn,
                schema=schema,
                table=table,
                override_identity=override_identity,
                truncate_first=args.truncate_dst,
            )
            print(f"    -> rows in destination after copy: {rows_copied}", flush=True)

        print("[db_clone] Done.")
    finally:
        try:
            src_conn.close()
        except Exception:
            pass
        try:
            dst_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
