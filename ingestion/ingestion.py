from __future__ import annotations
from typing import Tuple, Iterable, Sequence, Optional
from psycopg2.extras import Json
from db.db import getcursor


def ensure_scrape_job(
    name: str,
    description: str,
    platforms: list[str],
    status: str = "completed",
) -> int:
    """
    Get or create a scrape.job by name.
    Returns the job_id.
    Assumes caller-func has initalized the db pool (db.init_pool)
    """
    with getcursor() as cur:
        cur.execute("SELECT id FROM scrape.job WHERE name = %s", (name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            """
            INSERT INTO scrape.job(name, description, platforms, status)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (name, description, platforms, status),
        )
        (job_id,) = cur.fetchone()
        return job_id

def link_post_to_job(
    job_id: int,
    post_id: int,
    cur=None
) -> None:
    """
    links a post from the unified post registry to a scrape job
    this is done rather than just adding a scrape_job_id col to post registry
    because one post could have been collected from multiple jobs
    cur: optional cursor; if None, uses db.getcursor().
    """
    if cur is None:
        with getcursor() as cur2:
            cur2.execute(
                """
                INSERT INTO scrape.post_scrape(scrape_job_id, post_id)
                VALUES (%s, %s)
                ON CONFLICT (scrape_job_id, post_id) DO NOTHING
                """,
                (job_id, post_id),
            )
    else:
        cur.execute(
            """
            INSERT INTO scrape.post_scrape(scrape_job_id, post_id)
            VALUES (%s, %s)
            ON CONFLICT (scrape_job_id, post_id) DO NOTHING
            """,
            (job_id, post_id),
        )



def insert_batch(
    insert_sql: str,
    rows: list[dict],
    json_cols: Optional[Sequence[str]] = None,
    cur=None,
) -> tuple[int, int]:
    """
    Generic batched insert helper.

    - insert_sql: parametrized INSERT ... VALUES ... ON CONFLICT ...
                  using %(col)s style placeholders.
    - rows: list of dicts matching the placeholders.
    - json_cols: optional list/tuple of column names that should be wrapped
                 with psycopg2.extras.Json before insertion.
    - cur: optional cursor; if None, uses db.getcursor().

    Returns (inserted, skipped), where:
      inserted = number of rows actually inserted
      skipped  = len(rows) - inserted  (e.g. due to ON CONFLICT DO NOTHING)
    """
    if not rows:
        return 0, 0

    json_cols_set = set(json_cols or ())

    prepared: list[dict] = []
    for r in rows:
        d = dict(r)  # copy so we don't mutate caller's dicts
        for col in json_cols_set:
            if col in d and d[col] is not None:
                d[col] = Json(d[col])
        prepared.append(d)

    if cur is None:
        with getcursor() as cur2:
            cur2.executemany(insert_sql, prepared)
            inserted = cur2.rowcount
    else:
        cur.executemany(insert_sql, prepared)
        inserted = cur.rowcount

    skipped = len(rows) - inserted
    return inserted, skipped


def fetch_post_ids_for_single_key(
    platform: str,
    key1_values: list[str],
    cur=None
) -> list[int]:
    if not key1_values:
        return []
        
    if cur is None:
        with getcursor() as cur2:
            cur2.execute(
                """
                SELECT id
                FROM sm.post_registry
                WHERE platform = %s
                  AND key1 = ANY(%s)
                  AND key2 IS NULL
                """,
                (platform, key1_values),
            )
            return [row[0] for row in cur2.fetchall()]
    else:
        cur.execute(
            """
            SELECT id
            FROM sm.post_registry
            WHERE platform = %s
              AND key1 = ANY(%s)
              AND key2 IS NULL
            """,
            (platform, key1_values),
        )
        return [row[0] for row in cur.fetchall()]



def fetch_post_ids_for_dual_key(
    platform: str,
    key_pairs: list[tuple[str, str]],
    cur=None
) -> list[int]:
    """
    Given (key1, key2) pairs for a platform, return matching post_registry ids.

    key_pairs: list of (key1, key2) – will be stringified.
    """
    # Normalize & dedupe
    norm_pairs = {
        (str(k1), str(k2))
        for (k1, k2) in key_pairs
        if k1 is not None and k2 is not None
    }
    if not norm_pairs:
        return []

    key1_list = list({k1 for (k1, _k2) in norm_pairs})
    if cur is None:
        with getcursor() as cur2:
            cur2.execute(
                """
                SELECT id, key1, key2
                FROM sm.post_registry
                WHERE platform = %s
                  AND key1 = ANY(%s)
                """,
                (platform, key1_list),
            )
            rows = cur2.fetchall()
    else:
        cur.execute(
            """
            SELECT id, key1, key2
            FROM sm.post_registry
            WHERE platform = %s
              AND key1 = ANY(%s)
            """,
            (platform, key1_list),
        )
        rows = cur.fetchall()

    # Build map (key1, key2) -> post_id
    registry_map = {(k1, k2): post_id for (post_id, k1, k2) in rows}

    # Return all post_ids that match our requested pairs
    return [
        registry_map[pair]
        for pair in norm_pairs
        if pair in registry_map
    ]
    
def bulk_link_single_key(
    *,
    job_id: int,
    platform: str,
    key1_values: list[str],
    cur=None,
) -> None:
    """
    Insert scrape.post_scrape rows for a single-key platform in one SQL.

    key1_values: list of key1 strings (e.g. reddit ids, video ids).
    """
    # Dedup and filter empties
    vals = sorted({str(v) for v in key1_values if v is not None})
    if not vals:
        return

    if cur is None:
        with getcursor(commit=True) as cur2:
            cur2.execute(
                """
                INSERT INTO scrape.post_scrape (scrape_job_id, post_id)
                SELECT %s, pr.id
                FROM sm.post_registry AS pr
                WHERE pr.platform = %s
                  AND pr.key1 = ANY(%s)
                  AND pr.key2 IS NULL
                ON CONFLICT (scrape_job_id, post_id) DO NOTHING
                """,
                (job_id, platform, vals),
            )
    else:
        cur.execute(
            """
            INSERT INTO scrape.post_scrape (scrape_job_id, post_id)
            SELECT %s, pr.id
            FROM sm.post_registry AS pr
            WHERE pr.platform = %s
              AND pr.key1 = ANY(%s)
              AND pr.key2 IS NULL
            ON CONFLICT (scrape_job_id, post_id) DO NOTHING
            """,
            (job_id, platform, vals),
        )
        
def bulk_link_dual_key(
    *,
    job_id: int,
    platform: str,
    key1_values: list[str],
    key2_values: list[str],
    cur=None,
) -> None:
    """
    Insert scrape.post_scrape rows for a dual-key platform (key1+key2) in one SQL.

    key1_values / key2_values: parallel lists; (key1[i], key2[i]) is one pair.
    """
    if len(key1_values) != len(key2_values):
        raise ValueError("key1_values and key2_values must be same length")

    pairs = {
        (str(k1), str(k2))
        for k1, k2 in zip(key1_values, key2_values)
        if k1 is not None and k2 is not None
    }
    if not pairs:
        return

    key1_list = [p[0] for p in pairs]
    key2_list = [p[1] for p in pairs]

    sql = """
        WITH keys AS (
            SELECT
                unnest(%s::text[]) AS key1,
                unnest(%s::text[]) AS key2
        )
        INSERT INTO scrape.post_scrape (scrape_job_id, post_id)
        SELECT %s, pr.id
        FROM sm.post_registry AS pr
        JOIN keys k
          ON pr.key1 = k.key1
         AND pr.key2 = k.key2
        WHERE pr.platform = %s
        ON CONFLICT (scrape_job_id, post_id) DO NOTHING
    """

    params = (key1_list, key2_list, job_id, platform)

    if cur is None:
        with getcursor(commit=True) as cur2:
            cur2.execute(sql, params)
    else:
        cur.execute(sql, params)