from __future__ import annotations

import argparse
import sys
from datetime import datetime

from db.db import init_pool, close_pool, getcursor


def die(msg: str) -> None:
    print(f"[error] {msg}", file=sys.stderr)
    raise SystemExit(1)


def fetch_terms() -> list[tuple[int, str]]:
    with getcursor() as cur:
        cur.execute(
            """
            SELECT id, name
            FROM taxonomy.vaccine_term
            ORDER BY id
            """
        )
        return [(int(r[0]), str(r[1])) for r in cur.fetchall()]


def find_latest_submission_for_term(term: str) -> tuple[datetime, str] | None:
    """
    Best-effort approximation: locate newest submission whose text contains the term.
    """
    pattern = f"%{term}%"

    with getcursor() as cur:
        cur.execute(
            """
            SELECT created_at_ts, id
            FROM sm.reddit_submission
            WHERE
                title ILIKE %s
                OR selftext ILIKE %s
                OR filtered_text ILIKE %s
            ORDER BY created_at_ts DESC, id DESC
            LIMIT 1
            """,
            (pattern, pattern, pattern),
        )
        row = cur.fetchone()

    if not row:
        return None

    # created_at_ts is timestamptz coming from DB; keep as-aware datetime
    ts = row[0]
    sid = str(row[1])
    return (ts, sid)


def upsert_status(term_id: int, last_found_ts: datetime, last_found_id: str) -> None:
    with getcursor() as cur:
        cur.execute(
            """
            INSERT INTO sm.reddit_submission_search_status
                (term_id, last_found_ts, last_found_id)
            VALUES
                (%s, %s, %s)
            ON CONFLICT (term_id) DO UPDATE
            SET last_found_ts = EXCLUDED.last_found_ts,
                last_found_id = EXCLUDED.last_found_id,
                last_updated = now()
            """,
            (term_id, last_found_ts, last_found_id),
        )


def main() -> None:
    ap = argparse.ArgumentParser(
        prog="python -m scripts.backfill_reddit_submission_search_status",
        description="Backfill sm.reddit_submission_search_status from existing sm.reddit_submission rows.",
    )
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Run against PROD (default: dev).",
    )
    args = ap.parse_args()

    init_pool(prefix="prod" if args.prod else "dev")

    try:
        terms = fetch_terms()

        updated = 0
        missing = 0

        for i, (term_id, term) in enumerate(terms, start=1):
            res = find_latest_submission_for_term(term)
            if res is None:
                continue
            else:
                last_found_ts, last_found_id = res
                print(
                    f"[{i}/{len(terms)}] term_id={term_id} {term!r}: "
                    f"last_found_ts={last_found_ts.isoformat()} last_found_id="
                    f"{last_found_id}"
                )

            upsert_status(term_id, last_found_ts, last_found_id)
            updated += 1

        print(f"[done] processed={updated} no_match={missing}")

    finally:
        close_pool()


if __name__ == "__main__":
    main()
