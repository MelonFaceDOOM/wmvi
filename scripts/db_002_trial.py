from __future__ import annotations

import time

from db.db import init_pool, close_pool, getcursor


def main() -> None:
    # Connect to DEV DB
    init_pool(prefix="DEV")
    try:
        with getcursor() as cur:
            # Basic info about post_registry ID range
            cur.execute(
                """
                SELECT COALESCE(min(id), 0), COALESCE(max(id), 0)
                FROM sm.post_registry
                """
            )
            min_id, max_id = cur.fetchone()
            print(f"post_registry id range: {min_id}–{max_id}")

            if max_id <= min_id:
                print("No posts in sm.post_registry; nothing to test.")
                return

            # Choose a window near the tail of the registry to mimic matcher usage
            window_size = 100_000  # adjust as needed
            window_start = max(min_id, max_id - window_size)
            window_end = max_id
            print(f"Using test window: ({window_start}, {window_end}]")

            # Grab a few sample terms
            cur.execute(
                """
                SELECT id, name
                FROM taxonomy.vaccine_term
                ORDER BY id
                LIMIT 5
                """
            )
            terms = cur.fetchall()
            if not terms:
                print("No terms in taxonomy.vaccine_term; nothing to test.")
                return

            for term_id, term_name in terms:
                term_name = (term_name or "").strip()
                if not term_name:
                    continue

                print(f"\n=== Term {term_id}: {term_name!r} ===")

                # Global search timing
                t0 = time.perf_counter()
                cur.execute(
                    """
                    SELECT count(*)
                    FROM sm.post_search_en
                    WHERE tsv_en @@ plainto_tsquery('english', %s)
                    """,
                    (term_name,),
                )
                (count_all,) = cur.fetchone()
                t1 = time.perf_counter()
                print(
                    f"Global search: {count_all} matches "
                    f"in {t1 - t0:.3f}s"
                )

                # Windowed search timing (what the matcher will actually do)
                t0 = time.perf_counter()
                cur.execute(
                    """
                    SELECT count(*)
                    FROM sm.post_search_en
                    WHERE post_id > %s
                      AND post_id <= %s
                      AND tsv_en @@ plainto_tsquery('english', %s)
                    """,
                    (window_start, window_end, term_name),
                )
                (count_window,) = cur.fetchone()
                t1 = time.perf_counter()
                print(
                    f"Window search: {count_window} matches "
                    f"in {t1 - t0:.3f}s "
                    f"for range ({window_start}, {window_end}]"
                )

                # Optional: show a few sample IDs for sanity
                cur.execute(
                    """
                    SELECT post_id
                    FROM sm.post_search_en
                    WHERE tsv_en @@ plainto_tsquery('english', %s)
                    ORDER BY post_id
                    LIMIT 10
                    """,
                    (term_name,),
                )
                sample_ids = [row[0] for row in cur.fetchall()]
                print(f"Sample post_ids: {sample_ids}")

    finally:
        close_pool()


if __name__ == "__main__":
    main()
