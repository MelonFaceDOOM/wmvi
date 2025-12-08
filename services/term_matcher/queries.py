from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from psycopg2.extras import execute_values

# Expected GIN indices for tsv_en full-text search.
REQUIRED_INDICES = [
    "sm.tweet_tsv_en_gin",
    "sm.rs_tsv_en_gin",
    "sm.rc_tsv_en_gin",
    "sm.yv_tsv_en_gin",
    "sm.yc_tsv_en_gin",
    "sm.telegram_tsv_en_gin"
]


def check_indices(cur) -> None:
    """
    Warn if the expected GIN indices are missing.

    `cur` is a DB cursor from db.db.getcursor().
    """
    import logging

    missing = []
    for idx_name in REQUIRED_INDICES:
        cur.execute("SELECT to_regclass(%s)", (idx_name,))
        (reg,) = cur.fetchone()
        if reg is None:
            missing.append(idx_name)

    if missing:
        logging.warning(
            "The following indices were not found; term matching may be slow: %s",
            ", ".join(missing),
        )
    else:
        logging.info("All expected tsv_en GIN indices found.")


# --------- taxonomy / terms ---------


def get_vaccine_terms(cur) -> List[Tuple[int, str]]:
    """
    Return all vaccine terms as a list of (term_id, name).
    """
    cur.execute(
        """
        SELECT id, name
        FROM taxonomy.vaccine_term
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def get_terms_by_ids(cur, term_ids: Sequence[int]) -> List[Tuple[int, str]]:
    """
    Fetch (id, name) for a given list of term IDs.
    Returns only those that exist; order by id.
    """
    if not term_ids:
        return []
    cur.execute(
        """
        SELECT id, name
        FROM taxonomy.vaccine_term
        WHERE id = ANY(%s)
        ORDER BY id
        """,
        (list(term_ids),),
    )
    rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def get_terms_by_names(cur, term_names: Sequence[str]) -> List[Tuple[int, str]]:
    """
    Fetch (id, name) for a given list of *exact* term names.
    Returns only those that exist; order by id.
    """
    cleaned = [t.strip() for t in term_names if t and t.strip()]
    if not cleaned:
        return []
    cur.execute(
        """
        SELECT id, name
        FROM taxonomy.vaccine_term
        WHERE name = ANY(%s)
        ORDER BY id
        """,
        (cleaned,),
    )
    rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def get_vaccine_terms_like(cur, pattern: str) -> List[Tuple[int, str]]:
    """
    Fetch (id, name) for terms whose names ILIKE the given pattern.
    """
    pattern = pattern.strip()
    if not pattern:
        return get_vaccine_terms(cur)

    cur.execute(
        """
        SELECT id, name
        FROM taxonomy.vaccine_term
        WHERE name ILIKE '%' || %s || '%'
        ORDER BY id
        """,
        (pattern,),
    )
    rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


# --------- post registry helpers ---------


def get_latest_post_registry_id(cur) -> int:
    """
    Return the current maximum post_registry.id (0 if table is empty).
    """
    cur.execute("SELECT COALESCE(max(id), 0) FROM sm.post_registry")
    (max_id,) = cur.fetchone()
    return int(max_id or 0)


# --------- term_match_state CRUD ---------


def get_or_init_term_state(
    cur,
    term_id: int,
    matcher_version: str,
) -> Optional[int]:
    """
    Fetch last_checked_post_id for (term_id, matcher_version).
    If no row exists, initialize one with NULL cursor and return None.
    """
    cur.execute(
        """
        SELECT last_checked_post_id
        FROM matches.term_match_state
        WHERE term_id = %s
          AND matcher_version = %s
        """,
        (term_id, matcher_version),
    )
    row = cur.fetchone()
    if row is not None:
        (last_checked,) = row
        return int(last_checked) if last_checked is not None else None

    # Initialize state row with NULL cursor
    cur.execute(
        """
        INSERT INTO matches.term_match_state (term_id, matcher_version, last_checked_post_id)
        VALUES (%s, %s, NULL)
        ON CONFLICT (term_id, matcher_version) DO NOTHING
        """,
        (term_id, matcher_version),
    )
    return None


def update_term_state(
    cur,
    term_id: int,
    matcher_version: str,
    last_checked_post_id: int,
) -> None:
    """
    Update last_checked_post_id and last_run_at for (term_id, matcher_version).
    If the row does not exist (race/init), insert it.
    """
    cur.execute(
        """
        UPDATE matches.term_match_state
           SET last_checked_post_id = %s,
               last_run_at = now()
         WHERE term_id = %s
           AND matcher_version = %s
        """,
        (last_checked_post_id, term_id, matcher_version),
    )

    # nothing changed; assume no row exists with stated term_id and matcher_version
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO matches.term_match_state
                (term_id, matcher_version, last_checked_post_id, last_run_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (term_id, matcher_version) DO NOTHING
            """,
            (term_id, matcher_version, last_checked_post_id),
        )


# --------- matching queries ---------


def find_post_ids_for_term_range(
    cur,
    term_name: str,
    min_post_id: int,
    max_post_id: int,
) -> Set[int]:
    """
    For a given term string, query sm.post_search_en--
        (the union table with all of the tsv_en from each sm source)
    --using full-text search
    within the post_registry.id range (min_post_id, max_post_id].

    Returns a set of post_registry IDs (BIGINT).
    """

    term_name = (term_name or "").strip()
    if not term_name:
        return set()

    if max_post_id <= min_post_id:
        return set()

    post_ids: Set[int] = set()

    cur.execute(
        """
        SELECT post_id
        FROM sm.post_search_en
        WHERE post_id > %s
          AND post_id <= %s
          AND tsv_en @@ plainto_tsquery('english', %s)
        """,
        (min_post_id, max_post_id, term_name),
    )
    rows = cur.fetchall()
    for (pid,) in rows:
        post_ids.add(int(pid))

    return post_ids


def insert_post_term_matches(
    cur,
    term_id: int,
    matcher_version: str,
    post_ids: Sequence[int],
) -> int:
    """
    TODO: in the future, this could be made more efficient by combining
      it with find_post_ids_for_term_range()
      
    Insert matches for (term, posts) into matches.post_term_match.

    - Uses ON CONFLICT (post_id, term_id) DO NOTHING.
    - Returns the number of rows actually inserted (best-effort via rowcount).
    """
    if not post_ids:
        return 0

    rows = [(int(pid), term_id, matcher_version) for pid in post_ids]

    execute_values(
        cur,
        """
        INSERT INTO matches.post_term_match
            (post_id, term_id, matcher_version)
        VALUES %s
        ON CONFLICT (post_id, term_id) DO NOTHING
        """,
        rows,
    )

    inserted = cur.rowcount or 0
    return int(inserted)


# --------- stats / reporting ---------


def get_term_stats(
    cur,
    matcher_version: str,
    term_ids: Optional[Sequence[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Return per-term stats for the given matcher_version:

    - term_id
    - name
    - last_checked_post_id
    - match_count (from matches.post_term_match)
    - coverage (0–1 float; fraction of post_registry scanned)
    """
    max_post_id = get_latest_post_registry_id(cur)

    where_clause = ""
    params: List[Any] = [matcher_version]

    if term_ids:
        where_clause = "WHERE t.id = ANY(%s)"
        params.append(list(term_ids))

    cur.execute(
        f"""
        WITH match_counts AS (
            SELECT term_id, COUNT(*) AS match_count
            FROM matches.post_term_match
            GROUP BY term_id
        )
        SELECT
            t.id AS term_id,
            t.name,
            s.last_checked_post_id,
            COALESCE(mc.match_count, 0) AS match_count
        FROM taxonomy.vaccine_term t
        LEFT JOIN matches.term_match_state s
          ON s.term_id = t.id
         AND s.matcher_version = %s
        LEFT JOIN match_counts mc
          ON mc.term_id = t.id
        {where_clause}
        ORDER BY t.id
        """,
        params,
    )
    rows = cur.fetchall()

    stats: List[Dict[str, Any]] = []
    for term_id, name, last_checked, match_count in rows:
        last_checked_int = int(last_checked) if last_checked is not None else None
        match_count_int = int(match_count or 0)
        if max_post_id > 0 and last_checked_int is not None:
            coverage = float(last_checked_int) / float(max_post_id)
        else:
            coverage = 0.0
        stats.append(
            {
                "term_id": int(term_id),
                "name": str(name),
                "last_checked_post_id": last_checked_int,
                "match_count": match_count_int,
                "coverage": coverage,
                "max_post_id": max_post_id,
            }
        )

    return stats
