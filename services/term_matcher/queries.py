from __future__ import annotations

from typing import Iterable, List, Tuple

from psycopg2.extras import execute_values


# --------------------
# term loading
# --------------------

def get_all_terms(cur) -> List[Tuple[int, str]]:
    cur.execute(
        """
        SELECT id, name
        FROM taxonomy.vaccine_term
        ORDER BY id
        """
    )
    return [(int(i), str(n)) for i, n in cur.fetchall()]


def get_terms_by_names(cur, names: Iterable[str]) -> List[Tuple[int, str]]:
    names = [n for n in (n.strip() for n in names) if n]
    if not names:
        return []

    cur.execute(
        """
        SELECT id, name
        FROM taxonomy.vaccine_term
        WHERE name = ANY(%s)
        ORDER BY id
        """,
        (names,),
    )
    return [(int(i), str(n)) for i, n in cur.fetchall()]


# --------------------
# cursor helpers
# --------------------

def get_latest_post_id(cur) -> int:
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM sm.post_registry")
    return int(cur.fetchone()[0])


def get_or_init_term_state(cur, term_id: int, matcher_version: str) -> int:
    cur.execute(
        """
        SELECT last_checked_post_id
        FROM matches.term_match_state
        WHERE term_id = %s AND matcher_version = %s
        """,
        (term_id, matcher_version),
    )
    row = cur.fetchone()
    if row:
        return int(row[0] or 0)

    cur.execute(
        """
        INSERT INTO matches.term_match_state (term_id, matcher_version)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (term_id, matcher_version),
    )
    return 0


def update_term_state(cur, term_id: int, matcher_version: str, last_post_id: int) -> None:
    cur.execute(
        """
        UPDATE matches.term_match_state
        SET last_checked_post_id = %s,
            last_run_at = now()
        WHERE term_id = %s AND matcher_version = %s
        """,
        (last_post_id, term_id, matcher_version),
    )


# --------------------
# matching
# --------------------

def fetch_candidate_posts(
    cur,
    term: str,
    min_post_id: int,
    max_post_id: int,
) -> List[Tuple[int, str]]:
    """
    Fetch posts that *might* contain the term.

    We use FTS for candidate selection,
    but span extraction happens in Python.
    """
    cur.execute(
        """
        SELECT p.post_id, p.text
        FROM sm.posts_all p
        JOIN sm.post_search_en s
          ON s.post_id = p.post_id
        WHERE p.post_id > %s
          AND p.post_id <= %s
          AND s.tsv_en @@ plainto_tsquery('english', %s)
        """,
        (min_post_id, max_post_id, term),
    )
    return [(int(pid), txt or "") for pid, txt in cur.fetchall()]


def insert_term_hits(
    cur,
    rows: List[Tuple[int, int, int, int, str]],
) -> int:
    """
    rows:
      (post_id, term_id, match_start, match_end, matcher_version)
    """
    if not rows:
        return 0

    execute_values(
        cur,
        """
        INSERT INTO matches.post_term_hit
            (post_id, term_id, match_start, match_end, matcher_version)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    return cur.rowcount or 0
