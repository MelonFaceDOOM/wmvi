"""transcription ran for a while in dev before prod was made, so those transcripts should be transferred over!"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


BATCH_SIZE = 500


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def connect(prefix: str) -> psycopg2.extensions.connection:
    # DEV_/PROD_ vars
    host = env(f"{prefix}_PGHOST")
    port = int(env(f"{prefix}_PGPORT", "5432"))
    user = env(f"{prefix}_PGUSER")
    password = env(f"{prefix}_PGPASSWORD")
    dbname = env(f"{prefix}_PGDATABASE")

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )
    conn.autocommit = False
    return conn


def sanity_check_guids(dev_cur, prod_cur) -> None:
    """
    Check: for any episode IDs that exist in BOTH dev and prod, do their GUIDs match?
    Prints a small sample if mismatches exist.
    """
    # NOTE: This can be heavy if episodes is huge. It only checks overlapping IDs.
    dev_cur.execute(
        """
        SELECT COUNT(*)::bigint
        FROM podcasts.episodes d
        JOIN podcasts.episodes p ON p.id = d.id
        WHERE d.guid IS DISTINCT FROM p.guid;
        """
    )
    mismatches = int(dev_cur.fetchone()[0])

    if mismatches == 0:
        print("[sanity] GUIDs match for all overlapping IDs.")
        return

    print(f"[sanity] WARNING: {
          mismatches} GUID mismatches for overlapping IDs. Sample:")
    dev_cur.execute(
        """
        SELECT d.id, d.guid AS dev_guid, p.guid AS prod_guid
        FROM podcasts.episodes d
        JOIN podcasts.episodes p ON p.id = d.id
        WHERE d.guid IS DISTINCT FROM p.guid
        ORDER BY d.id
        LIMIT 20;
        """
    )
    for row in dev_cur.fetchall():
        print(f"  id={row[0]} dev_guid={row[1]!r} prod_guid={row[2]!r}")


def fetch_dev_batch(dev_cur, last_id: str | None) -> List[Tuple[str, str, Any]]:
    """
    Fetch next batch of (id, transcript, transcript_updated_at) from dev where transcript is not null.
    Uses keyset pagination on id (text).
    """
    if last_id is None:
        dev_cur.execute(
            """
            SELECT id, transcript, transcript_updated_at
            FROM podcasts.episodes
            WHERE transcript IS NOT NULL
            ORDER BY id
            LIMIT %s;
            """,
            (BATCH_SIZE,),
        )
    else:
        dev_cur.execute(
            """
            SELECT id, transcript, transcript_updated_at
            FROM podcasts.episodes
            WHERE transcript IS NOT NULL
              AND id > %s
            ORDER BY id
            LIMIT %s;
            """,
            (last_id, BATCH_SIZE),
        )
    return dev_cur.fetchall()


def update_prod_transcripts(prod_cur, rows: List[Tuple[str, str, Any]]) -> int:
    """
    Update prod podcasts.episodes transcript fields where:
      - id exists
      - prod.transcript IS NULL
    """
    sql = """
    UPDATE podcasts.episodes p
    SET
        transcript = v.transcript,
        transcript_updated_at = COALESCE(v.transcript_updated_at, p.transcript_updated_at)
    FROM (
        VALUES %s
    ) AS v(id, transcript, transcript_updated_at)
    WHERE p.id = v.id
      AND p.transcript IS NULL;
    """

    # Key part: force correct type for the 3rd column.
    # This makes v.transcript_updated_at be timestamptz (not text).
    template = "(%s, %s, %s::timestamptz)"

    execute_values(
        prod_cur,
        sql,
        rows,
        template=template,
        page_size=1000,
    )
    return prod_cur.rowcount


def main() -> None:
    load_dotenv()

    dev_conn = connect("DEV")
    prod_conn = connect("PROD")

    # Use dedicated cursors
    dev_cur = dev_conn.cursor()
    prod_cur = prod_conn.cursor()

    try:
        print("[init] connected to dev/prod")

        # Sanity check: overlapping IDs have same GUID
        # sanity_check_guids(dev_cur, prod_cur)
        dev_conn.commit()   # read-only but keep things clean
        prod_conn.commit()

        total_seen = 0
        total_updated = 0
        batch_num = 0

        last_id: str | None = None

        while True:
            batch = fetch_dev_batch(dev_cur, last_id)
            if not batch:
                break

            batch_num += 1
            total_seen += len(batch)
            last_id = batch[-1][0]

            # Apply updates in prod as a single statement
            updated = update_prod_transcripts(prod_cur, batch)
            prod_conn.commit()

            total_updated += updated

            print(
                f"[batch {batch_num}] fetched={len(batch)} "
                f"updated_in_prod={updated} "
                f"totals: seen={total_seen} updated={total_updated} "
                f"last_id={last_id!r}"
            )

        print(f"[done] batches={batch_num} total_seen={
              total_seen} total_updated={total_updated}")

    except Exception:
        # Roll back both in case we were mid-txn
        try:
            dev_conn.rollback()
        except Exception:
            pass
        try:
            prod_conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            dev_cur.close()
        except Exception:
            pass
        try:
            prod_cur.close()
        except Exception:
            pass
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
