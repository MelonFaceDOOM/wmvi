from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple

from psycopg2.extensions import cursor as PGCursor

# Type alias: list of (submission_id, created_utc_seconds)
RecentSubs = List[Tuple[str, float]]


# -------------------------
# Term list helpers
# -------------------------

def _fetch_all_vaccine_terms(cur: PGCursor) -> List[str]:
    """
    Fetch all term names from taxonomy.vaccine_term.

    Returns a list of raw names (as stored in DB).
    """
    cur.execute(
        """
        SELECT name
        FROM taxonomy.vaccine_term
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    terms = [r[0] for r in rows if r and r[0]]
    return terms


def _normalize_term(name: str) -> str:
    """
    Normalize a term for super-term logic.

    Currently:
      - Lowercase
      - Strip apostrophes (to mimic previous behavior)
    """
    return name.lower().replace("'", "")


def _is_super_term(a: str, b: str) -> bool:
    """
    Returns True if `b` is a super-term of `a`.

    Example:
      a = "pneu c"
      b = "pneu c 13"
      -> True
    """
    a_words = a.split()
    b_words = b.split()
    if len(b_words) <= len(a_words):
        return False
    return all(w in b_words for w in a_words)


def get_effective_term_list(cur: PGCursor) -> List[str]:
    """
    Return a list of term names (normalized) with super-terms removed.

    These normalized names are what reddit_monitor uses and what will be
    written into monitor_metadata.csv.
    """
    raw_terms = _fetch_all_vaccine_terms(cur)
    norm_terms = sorted({_normalize_term(t) for t in raw_terms if t})

    super_terms: List[Tuple[str, str]] = []
    good_terms: List[str] = []

    for a in norm_terms:
        a_is_super = False
        for b in norm_terms:
            if a is b:
                continue
            if _is_super_term(b, a):
                a_is_super = True
                super_terms.append((a, b))
        if not a_is_super:
            good_terms.append(a)

    return good_terms


# -------------------------
# Recent submission queries (still available if needed elsewhere)
# -------------------------

def get_recent_submissions_for_term(
    cur: PGCursor,
    term: str,
    limit: int = 50,
    lookback_days: int = 30,
) -> RecentSubs:
    """
    Return up to `limit` recent submissions matching `term`.

    - Searches in sm.reddit_submission.filtered_text using ILIKE.
    - Converts created_at_ts to UNIX seconds (float).
    """
    pattern = f"%{term}%"
    cur.execute(
        """
        SELECT
            id,
            EXTRACT(EPOCH FROM created_at_ts) AS created_utc
        FROM sm.reddit_submission
        WHERE filtered_text ILIKE %s
          AND created_at_ts >= now() - (%s * INTERVAL '1 day')
        ORDER BY created_at_ts DESC
        LIMIT %s
        """,
        (pattern, lookback_days, limit),
    )
    rows = cur.fetchall()
    return [(r[0], float(r[1])) for r in rows]


def get_recent_submissions_for_all_terms(
    cur: PGCursor,
    per_term_limit: int = 50,
    lookback_days: int = 30,
) -> Dict[str, RecentSubs]:
    """
    For each effective term, fetch recent submissions and return a mapping:

        { term: [(submission_id, created_utc_seconds), ...], ... }

    (No longer used by the scheduler, but kept for possible analysis/tests.)
    """
    terms = get_effective_term_list(cur)
    result: Dict[str, RecentSubs] = {}

    for term in terms:
        subs = get_recent_submissions_for_term(
            cur,
            term=term,
            limit=per_term_limit,
            lookback_days=lookback_days,
        )
        result[term] = subs

    return result
