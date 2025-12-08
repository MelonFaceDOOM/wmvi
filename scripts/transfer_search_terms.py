from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Tuple, Set

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from dotenv import load_dotenv
load_dotenv()

"""
the backfill aspect of this is no longer valid.
the new canonical method of making matches is defined in:
    services.term_matcher
"""


# -------------------------------
# DB helpers
# -------------------------------

def db_creds_from_env(prefix: str) -> str:
    host = os.environ[f"{prefix}_PGHOST"]
    user = os.environ[f"{prefix}_PGUSER"]
    pwd  = os.environ[f"{prefix}_PGPASSWORD"]
    port = os.environ.get(f"{prefix}_PGPORT", "5432")
    db   = os.environ.get(f"{prefix}_PGDATABASE", "postgres")
    ssl  = os.environ.get(f"{prefix}_PGSSLMODE", "require")

    return (
        f"host={host} port={port} dbname={db} user={user} "
        f"password={pwd} sslmode={ssl}"
    )


def connect_from_prefix(prefix: str) -> psycopg2.extensions.connection:
    dsn = db_creds_from_env(prefix)
    return psycopg2.connect(dsn)


# -------------------------------
# Step 1: import search_term -> taxonomy.vaccine_term
# -------------------------------

def _map_term_type(linked_type: str | None) -> str:
    """
    Map old linked_concept_type -> new taxonomy.vaccine_term.type.

    Allowed target values:
      'vaccine' | 'disease' | 'person' | 'organization'

    Fallback: 'vaccine'.
    """
    if not linked_type:
        return "vaccine"
    t = linked_type.strip().lower()
    if t in ("vaccine", "disease", "person", "organization"):
        return t
    return "vaccine"


def import_terms(src_conn, dst_conn) -> Dict[int, int]:
    """
    Copy terms from old search_term into taxonomy.vaccine_term.

    Returns:
        mapping {old_search_term_id -> new_vaccine_term_id}
    """
    mapping: Dict[int, int] = {}

    with src_conn.cursor() as cur_src:
        cur_src.execute(
            """
            SELECT id, name, linked_concept_type
            FROM search_term
            ORDER BY id
            """
        )
        rows = cur_src.fetchall()

    if not rows:
        print("[terms] No rows found in old search_term.", file=sys.stderr)
        return mapping

    with dst_conn.cursor() as cur_dst:
        for old_id, name, linked_type in rows:
            term_type = _map_term_type(linked_type)

            # Insert into taxonomy.vaccine_term, dedup by name
            cur_dst.execute(
                """
                INSERT INTO taxonomy.vaccine_term(name, type)
                VALUES (%s, %s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (name, term_type),
            )
            row = cur_dst.fetchone()
            if row is None:
                # term already existed; fetch its id
                cur_dst.execute(
                    "SELECT id FROM taxonomy.vaccine_term WHERE name = %s",
                    (name,),
                )
                (new_id,) = cur_dst.fetchone()
            else:
                (new_id,) = row

            mapping[old_id] = new_id

    dst_conn.commit()
    print(f"[terms] Imported/linked {len(mapping)} terms into taxonomy.vaccine_term.")
    return mapping


# -------------------------------
# Step 2: match terms to posts in new DB
# -------------------------------

#   Match against these tables, using their tsv_en GIN indices:
#   sm.tweet, sm.reddit_submission, sm.reddit_comment,
#   sm.youtube_video, sm.youtube_comment


PLATFORM_QUERIES = [
    (
        "tweet",
        """
        SELECT pr.id
        FROM sm.tweet t
        JOIN sm.post_registry pr
          ON pr.platform = 'tweet'
         AND pr.key1 = t.id::text
        WHERE t.tsv_en @@ plainto_tsquery('english', %s)
        """
    ),

    (
        "reddit_submission",
        """
        SELECT pr.id
        FROM sm.reddit_submission rs
        JOIN sm.post_registry pr
          ON pr.platform = 'reddit_submission'
         AND pr.key1 = rs.id::text
        WHERE rs.tsv_en @@ plainto_tsquery('english', %s)
        """
    ),

    (
        "reddit_comment",
        """
        SELECT pr.id
        FROM sm.reddit_comment rc
        JOIN sm.post_registry pr
          ON pr.platform = 'reddit_comment'
         AND pr.key1 = rc.id::text
        WHERE rc.tsv_en @@ plainto_tsquery('english', %s)
        """
    ),

    (
        "youtube_video",
        """
        SELECT pr.id
        FROM sm.youtube_video yv
        JOIN sm.post_registry pr
          ON pr.platform = 'youtube_video'
         AND pr.key1 = yv.video_id
        WHERE yv.tsv_en @@ plainto_tsquery('english', %s)
        """
    ),

    (
        "youtube_comment",
        """
        SELECT pr.id
        FROM sm.youtube_comment yc
        JOIN sm.post_registry pr
          ON pr.platform = 'youtube_comment'
         AND pr.key1 = yc.video_id
         AND pr.key2 = yc.comment_id
        WHERE yc.tsv_en @@ plainto_tsquery('english', %s)
        """
    ),
]


REQUIRED_indices = [
    "sm.tweet_tsv_en_gin",
    "sm.rs_tsv_en_gin",
    "sm.rc_tsv_en_gin",
    "sm.yv_tsv_en_gin",
    "sm.yc_tsv_en_gin",
]


def check_indices(dst_conn) -> None:
    """
    Warn if the expected GIN indices are missing. Queries will still work,
    but will be slower on large datasets.
    """
    missing = []
    with dst_conn.cursor() as cur:
        for qname in REQUIRED_indices:
            cur.execute("SELECT to_regclass(%s)", (qname,))
            (reg,) = cur.fetchone()
            if reg is None:
                missing.append(qname)

    if missing:
        print(
            "[warn] The following indices were not found; term matching may be slow:\n"
            "       " + ", ".join(missing),
            file=sys.stderr,
        )
    else:
        print("[indices] All expected tsv_en GIN indices found.")


def find_post_ids_for_term(cur, term_name: str) -> Set[int]:
    """
    For a given term string, query each platform table using its tsv_en index
    via plainto_tsquery, and return a set of post_registry IDs (BIGINT).
    """
    post_ids: Set[int] = set()

    for platform, query in PLATFORM_QUERIES:
        cur.execute(query, (term_name,))
        rows = cur.fetchall()
        for (pid,) in rows:
            post_ids.add(int(pid))

    return post_ids


def backfill_matches(dst_conn) -> None:
    """
    For every taxonomy.vaccine_term, search posts and insert into matches.post_term_match.

    Uses:
      - full-text search (tsv_en @@ plainto_tsquery('english', term_name))
      - ON CONFLICT DO NOTHING on (post_id, term_id)
    """
    from decimal import Decimal

    with dst_conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, name
            FROM taxonomy.vaccine_term
            ORDER BY id
            """
        )
        terms = cur.fetchall()

    if not terms:
        print("[matches] No terms in taxonomy.vaccine_term; nothing to backfill.")
        return

    print(f"[matches] Backfilling matches for {len(terms)} terms...")

    with dst_conn.cursor() as cur:
        for term_id, term_name in terms:
            term_name = term_name or ""
            term_name = term_name.strip()
            if not term_name:
                continue

            post_ids = find_post_ids_for_term(cur, term_name)
            if not post_ids:
                continue

            rows = [
                # (post_id, term_id, matcher_version, confidence)
                (pid, term_id, "legacy_fulltext_v1", Decimal("1.0"))
                for pid in post_ids
            ]

            # Bulk insert with ON CONFLICT DO NOTHING
            execute_values(
                cur,
                """
                INSERT INTO matches.post_term_match
                    (post_id, term_id, matcher_version, confidence)
                VALUES %s
                ON CONFLICT (post_id, term_id) DO NOTHING
                """,
                rows,
            )

    dst_conn.commit()
    print("[matches] Backfill complete.")


# -------------------------------
# main
# -------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill taxonomy.vaccine_term and matches.post_term_match "
            "from an old search_term table and existing posts."
        )
    )
    parser.add_argument(
        "--src-prefix",
        default="OLD",
        help="Env prefix for OLD DB (with search_term). Default: OLD",
    )
    parser.add_argument(
        "--dst-prefix",
        default="DEV",
        help="Env prefix for NEW/WMVI DB (with taxonomy/matches/sm.*). Default: DEV",
    )

    args = parser.parse_args(argv)

    try:
        src_conn = connect_from_prefix(args.src_prefix)
        dst_conn = connect_from_prefix(args.dst_prefix)
    except KeyError as e:
        print(f"Missing required env var for prefix: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        print(f"[info] Connecting from src={args.src_prefix} to dst={args.dst_prefix}")
        term_map = import_terms(src_conn, dst_conn)
        if not term_map:
            print("[info] No terms imported; skipping match backfill.")
            return

        check_indices(dst_conn)
        backfill_matches(dst_conn)
    finally:
        try:
            src_conn.close()
        except Exception:
            pass
        try:
            dst_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
