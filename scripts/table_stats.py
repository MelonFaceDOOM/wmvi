"""
check basic stats on each table to make sure data is flowing well
might want to expand in the future with more checks
"""


from __future__ import annotations
import argparse
from dataclasses import dataclass

from db.db import init_pool, close_pool, getcursor


# -----------------------------
# Config
# -----------------------------

PLATFORM_SPEC: dict[str, tuple[str, str, str | None]] = {
    "tweet": (
        "sm.tweet",
        "id",
        "",
    ),
    "reddit_submission": (
        "sm.reddit_submission",
        "id",
        "",
    ),
    "reddit_comment": (
        "sm.reddit_comment",
        "id",
        "",
    ),
    "telegram_post": (
        "sm.telegram_post",
        "channel_id",
        "message_id",
    ),
    "youtube_comment": (
        "youtube.comment",
        "video_id",
        "comment_id",
    ),
    "youtube_video": (
        "youtube.video",
        "video_id",
        "",
    ),
    "podcast_episode": (
        "podcasts.episodes",
        "id",
        "",
    ),
    "news_article": (
        "news.article",
        "id",
        "",
    ),
}

REQUIRED_1_TO_1 = [
    ("telegram_post"),
    ("reddit_comment"),
    ("reddit_submission"),
    ("tweet"),
    ("youtube_comment"),
    ("podcast_episode"),
]

OPTIONAL = [
    ("youtube_video"),
    ("news_article"),
]


@dataclass
class TableCheckResult:
    table: str
    platform: str
    table_rows: int
    registry_rows: int
    duplicate_registry_rows: int
    orphaned_registry_rows: int


# -----------------------------
# Helpers
# -----------------------------

def qval(sql: str, params: tuple = ()) -> int:
    with getcursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()[0])


def orphaned_registry_count(
    platform: str,
    table: str,
    key1_col: str,
    key2_col: str,
) -> int:
    if key2_col == "":
        sql = f"""
            SELECT COUNT(*)
            FROM sm.post_registry pr
            LEFT JOIN {table} t
              ON pr.key1 = t.{key1_col}::text
            WHERE pr.platform = %s
              AND t.{key1_col} IS NULL
        """
        params = (platform,)
    else:
        sql = f"""
            SELECT COUNT(*)
            FROM sm.post_registry pr
            LEFT JOIN {table} t
              ON pr.key1 = t.{key1_col}::text
             AND pr.key2 = t.{key2_col}::text
            WHERE pr.platform = %s
              AND t.{key1_col} IS NULL
        """
        params = (platform,)

    return qval(sql, params)


def duplicate_registry_count(
    platform: str,
    key2_col: str | None,
) -> int:
    if key2_col is None:
        sql = """
            SELECT COUNT(*)
            FROM (
                SELECT key1
                FROM sm.post_registry
                WHERE platform = %s
                GROUP BY key1
                HAVING COUNT(*) > 1
            ) s
        """
    else:
        sql = """
            SELECT COUNT(*)
            FROM (
                SELECT key1, key2
                FROM sm.post_registry
                WHERE platform = %s
                GROUP BY key1, key2
                HAVING COUNT(*) > 1
            ) s
        """

    return qval(sql, (platform,))


def check_table(platform: str) -> None:
    table, key1_col, key2_col = PLATFORM_SPEC[platform]

    table_rows = qval(f"SELECT COUNT(*) FROM {table}")
    registry_rows = qval(
        "SELECT COUNT(*) FROM sm.post_registry WHERE platform = %s",
        (platform,),
    )

    dupes = duplicate_registry_count(platform, key2_col)
    orphans = orphaned_registry_count(platform, table, key1_col, key2_col)

    print(f"[{platform}]")
    print(f"  source table rows       : {table_rows:,}")
    print(f"  registry rows           : {registry_rows:,}")
    print(f"  duplicate registry keys : {dupes:,}")
    print(f"  orphaned registry rows  : {orphans:,}")
    print()

# -----------------------------
# Main
# -----------------------------


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m scripts.table_stats")
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Run against PROD (default: dev).",
    )
    args = ap.parse_args()

    init_pool(prefix="prod" if args.prod else "dev")

    try:
        print("=" * 80)
        print("connected on", "PROD" if args.prod else "DEV")
        print("POST REGISTRY DATA QUALITY CHECK")
        print("=" * 80)

        print("\n--- REQUIRED 1:1 TABLES ---\n")

        for platform in REQUIRED_1_TO_1:
            check_table(platform)

        print("\n--- OPTIONAL TABLES ---\n")

        for platform in OPTIONAL:
            check_table(platform)

        # posts_all vs post_registry
        posts_all_rows = qval("SELECT COUNT(*) FROM sm.posts_all")
        registry_total = qval("SELECT COUNT(*) FROM sm.post_registry")

        print("\n--- GLOBAL CHECKS ---\n")
        print(f"sm.posts_all rows     : {posts_all_rows:,}")
        print(f"sm.post_registry rows : {registry_total:,}")

        if posts_all_rows != registry_total:
            print("!! MISMATCH: posts_all does not include all registry rows")
        else:
            print("OK: posts_all row count matches post_registry")

    finally:
        close_pool()


if __name__ == "__main__":
    main()
