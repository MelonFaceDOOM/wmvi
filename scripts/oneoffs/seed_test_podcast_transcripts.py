"""
Populate TEST DB with synthetic podcast transcripts for export smoke tests.

Updates existing episodes (no new rows) that currently have no transcript.
All touched rows share the same transcript_updated_at so you can pass that
timestamp to clear_test_podcast_transcripts.py.

Usage (from repo root, .env with TEST_* set):

  python -m scripts.oneoffs.seed_test_podcast_transcripts --test
  python -m scripts.oneoffs.seed_test_podcast_transcripts --test --count 10 --dry-run

Requires: python -m scripts.migrate_db --test (or equivalent schema).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv

from db.db import close_pool, getcursor, init_pool

load_dotenv()

MARKER_PREFIX = "[wmvi_test_seed]"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--test",
        action="store_true",
        required=True,
        help="Required safety flag: use TEST_* pool only.",
    )
    ap.add_argument(
        "--count",
        type=int,
        default=5,
        help="Max episodes to update (default 5).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without writing.",
    )
    args = ap.parse_args()

    init_pool(prefix="TEST")
    try:
        mark_ts = datetime.now(timezone.utc)
        iso = mark_ts.isoformat()

        with getcursor(commit=False) as cur:
            cur.execute(
                """
                SELECT id
                FROM podcasts.episodes
                WHERE transcript IS NULL
                   OR btrim(transcript) = ''
                ORDER BY id
                LIMIT %s
                """,
                (args.count,),
            )
            ids = [str(r[0]) for r in cur.fetchall()]

        if not ids:
            print("No episodes without transcript; nothing to seed.")
            return

        print(f"Will touch {len(ids)} episode(s): {ids[:5]}{'...' if len(ids) > 5 else ''}")
        print(f"Shared transcript_updated_at for cleanup: {iso}")
        print()
        print("Clear later with:")
        print(
            f'  python -m scripts.oneoffs.clear_test_podcast_transcripts --test --after "{iso}"'
        )
        print()

        if args.dry_run:
            print("Dry-run: no changes committed.")
            return

        body = (
            f"{MARKER_PREFIX} Synthetic transcript for export/import testing. "
            f"Episode id in body for sanity."
        )

        with getcursor(commit=True) as cur:
            cur.execute(
                """
                UPDATE podcasts.episodes
                   SET transcript = %s || ' Episode: ' || id || '.',
                       transcript_updated_at = %s
                 WHERE id = ANY(%s)
                """,
                (body, mark_ts, ids),
            )
            n = cur.rowcount
        print(f"Updated {n} row(s). transcript_updated_at = {iso}")
    finally:
        close_pool()


if __name__ == "__main__":
    main()
