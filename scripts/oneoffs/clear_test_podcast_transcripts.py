"""
Remove synthetic podcast transcripts seeded by seed_test_podcast_transcripts.py.

Deletes transcript_segments for matching episodes, then clears transcript fields.
Only rows where transcript starts with '[wmvi_test_seed]' AND
transcript_updated_at >= --after are affected (safety guard).

Usage:

  python -m scripts.oneoffs.clear_test_podcast_transcripts --test --after "2026-05-27T12:34:56.789123+00:00"
  python -m scripts.oneoffs.clear_test_podcast_transcripts --test --after "..." --dry-run

Pass the exact --after value printed by the seed script.
"""

from __future__ import annotations

import argparse
from datetime import datetime

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
        help="Required: use TEST_* pool only.",
    )
    ap.add_argument(
        "--after",
        required=True,
        help="ISO8601 timestamp: clear seeded rows with transcript_updated_at >= this.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Show counts only; no deletes/updates.",
    )
    args = ap.parse_args()

    after_ts = datetime.fromisoformat(args.after.replace("Z", "+00:00"))
    if after_ts.tzinfo is None:
        raise SystemExit("--after must include timezone (e.g. ...+00:00)")

    init_pool(prefix="TEST")
    try:
        with getcursor(commit=False) as cur:
            cur.execute(
                """
                SELECT id
                FROM podcasts.episodes
                WHERE transcript IS NOT NULL
                  AND transcript LIKE %s
                  AND transcript_updated_at >= %s
                """,
                (MARKER_PREFIX + "%", after_ts),
            )
            ids = [str(r[0]) for r in cur.fetchall()]

        print(f"Matching episodes: {len(ids)}")
        if ids and len(ids) <= 20:
            print("Ids:", ids)

        if args.dry_run:
            print("Dry-run: no changes.")
            return

        if not ids:
            print("Nothing to clear.")
            return

        with getcursor(commit=True) as cur:
            cur.execute(
                """
                DELETE FROM podcasts.transcript_segments
                WHERE episode_id = ANY(%s)
                """,
                (ids,),
            )
            seg_del = cur.rowcount

            cur.execute(
                """
                UPDATE podcasts.episodes
                   SET transcript = NULL,
                       transcript_updated_at = NULL,
                       transcription_started_at = NULL
                 WHERE id = ANY(%s)
                """,
                (ids,),
            )
            ep_up = cur.rowcount

        print(f"Deleted {seg_del} transcript_segment row(s); cleared {ep_up} episode(s).")
    finally:
        close_pool()


if __name__ == "__main__":
    main()
