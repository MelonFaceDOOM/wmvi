"""fills in podcasts.episodes.transcript using podcasts.transcript_segments
just meant to be run once to migrate old data into new system"""


from __future__ import annotations

import time

from dotenv import load_dotenv

from db.db import init_pool, close_pool, getcursor

load_dotenv()

BATCH_SIZE = 100
PROGRESS_EVERY = 10  # batches


BATCH_UPDATE_SQL = """
WITH batch AS (
    SELECT e.id
    FROM podcasts.episodes e
    WHERE e.transcript IS NULL
      AND EXISTS (
          SELECT 1
          FROM podcasts.transcript_segments s
          WHERE s.episode_id = e.id
      )
    ORDER BY e.id
    FOR UPDATE SKIP LOCKED
    LIMIT %s
),
agg AS (
    SELECT
        s.episode_id,
        string_agg(
            COALESCE(s.filtered_text, s.text),
            ' ' ORDER BY s.seg_idx
        ) AS transcript
    FROM podcasts.transcript_segments s
    JOIN batch b ON b.id = s.episode_id
    GROUP BY s.episode_id
)
UPDATE podcasts.episodes e
SET transcript = agg.transcript
FROM agg
WHERE e.id = agg.episode_id
RETURNING e.id;
"""


COUNT_REMAINING_SQL = """
SELECT COUNT(*)
FROM podcasts.episodes e
WHERE e.transcript IS NULL
  AND EXISTS (
      SELECT 1
      FROM podcasts.transcript_segments s
      WHERE s.episode_id = e.id
  )
"""


REGISTER_SQL = """
INSERT INTO sm.post_registry (platform, key1, key2)
SELECT 'podcast_episode', e.id, NULL
FROM podcasts.episodes e
WHERE e.transcript IS NOT NULL
ON CONFLICT DO NOTHING;
"""


def main() -> None:
    init_pool()

    start = time.time()
    total_processed = 0
    batch_count = 0

    try:
        with getcursor() as cur:
            cur.execute(COUNT_REMAINING_SQL)
            total = cur.fetchone()[0]

        print(f"Starting concat: {total} episodes remaining")

        while True:
            with getcursor(commit=True) as cur:
                cur.execute(BATCH_UPDATE_SQL, (BATCH_SIZE,))
                rows = cur.fetchall()
                updated = len(rows)

            if updated == 0:
                break

            total_processed += updated
            batch_count += 1

            if batch_count % PROGRESS_EVERY == 0:
                elapsed = time.time() - start
                rate = total_processed / elapsed if elapsed > 0 else 0
                remaining = max(total - total_processed, 0)
                eta_min = (remaining / rate / 60) if rate > 0 else 0

                print(
                    f"Processed {total_processed}/{total} "
                    f"(rate={rate:.2f} eps/s, ETA~{eta_min:.1f} min)"
                )

        elapsed_total = time.time() - start
        print(
            f"Concat complete: processed={total_processed} "
            f"elapsed={elapsed_total/60:.1f} min"
        )

        # ---- register in post_registry (once) ----
        with getcursor(commit=True) as cur:
            cur.execute(REGISTER_SQL)
            registered = cur.rowcount or 0

        print(f"Registered {registered} podcast episodes in post_registry")

    finally:
        close_pool()


if __name__ == "__main__":
    main()
