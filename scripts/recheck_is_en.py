"""
rechecks is_en for given platforms
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv
from psycopg2.extras import execute_batch

from db.db import init_pool, close_pool, getcursor
from lang.detect_lang import detect_is_en

load_dotenv()

# Only knob (edit this file to change what gets rechecked)
RECHECK_PLATFORMS: list[str] = ["reddit_submission"]

BATCH_SIZE = 2000
MIN_LEN = 24
MIN_CONF = 0.65

# ----------------------------------------------------------------------
# Platform-specific routing (keep in sync with label_en.py)
# ----------------------------------------------------------------------

PLATFORM_UPDATE_SPEC: dict[str, tuple[str, str, str | None]] = {
    "tweet": ("sm.tweet", "id", ""),
    "reddit_submission": ("sm.reddit_submission", "id", ""),
    "reddit_comment": ("sm.reddit_comment", "id", ""),
    "telegram_post": ("sm.telegram_post", "channel_id", "message_id"),
    "youtube_comment": ("youtube.comment", "video_id", "comment_id"),
    "youtube_video": ("youtube.video", "video_id", ""),
    "podcast_episode": ("podcasts.episodes", "id", ""),
    "news_article": ("news.article", "id", ""),
}


def build_update_sql(table: str, key1_col: str, key2_col: str) -> str:
    if key2_col == "":
        return f"""
            UPDATE {table}
               SET is_en = %s
             WHERE {key1_col} = %s
        """
    return f"""
        UPDATE {table}
           SET is_en = %s
         WHERE {key1_col} = %s
           AND {key2_col} = %s
    """


# ----------------------------------------------------------------------
# Recheck logic
# ----------------------------------------------------------------------

class IsEnRechecker:
    def run(self, platforms: list[str]) -> None:
        log = logging.getLogger(__name__)
        platforms = [p for p in platforms if p in PLATFORM_UPDATE_SPEC]

        if not platforms:
            log.info("recheck_is_en: no valid platforms configured; nothing to do")
            return

        log.info("recheck_is_en: starting recheck for platforms=%s", platforms)

        scanned = updated = unknown = 0

        with getcursor() as read_cur, getcursor(commit=True) as write_cur:
            for batch in self._iter_posts_for_platforms(read_cur, platforms=platforms):
                scanned += len(batch)
                u, unk = self._label_batch(batch, write_cur)
                updated += u
                unknown += unk

                log.info(
                    "recheck_is_en: progress scanned=%d updated=%d unknown=%d",
                    scanned,
                    updated,
                    unknown,
                )

        log.info(
            "recheck_is_en: done scanned=%d updated=%d unknown=%d (unknown marked as False)",
            scanned,
            updated,
            unknown,
        )

    def _iter_posts_for_platforms(self, cur, *, platforms: list[str]):
        """
        Iterate all posts in sm.posts_all for the given platforms (text not null),
        ordered by post_id for stable batching.
        """
        cur.execute(
            """
            SELECT
                post_id,
                platform,
                key1,
                key2,
                text
            FROM sm.posts_all
            WHERE platform = ANY(%s)
              AND text IS NOT NULL
            ORDER BY post_id
            """,
            (platforms,),
        )

        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            yield rows

    def _label_batch(self, batch, write_cur) -> tuple[int, int]:
        """
        Apply detect_is_en() to a batch and write updates grouped by platform.
        Returns: (updated_count, unknown_count)
        """
        updates_by_platform: Dict[str,
                                  List[Tuple[Any, ...]]] = defaultdict(list)
        unknown = 0

        for _post_id, platform, key1, key2, text in batch:
            spec = PLATFORM_UPDATE_SPEC.get(platform)
            if not spec:
                continue

            r = detect_is_en(text, min_len=MIN_LEN, min_conf=MIN_CONF)
            if r is None:
                unknown += 1
                r = False

            if spec[2] == "":
                updates_by_platform[platform].append((r, key1))
            else:
                updates_by_platform[platform].append((r, key1, key2))

        updated = 0
        for platform, rows in updates_by_platform.items():
            table, key1_col, key2_col = PLATFORM_UPDATE_SPEC[platform]
            sql = build_update_sql(table, key1_col, key2_col)
            execute_batch(write_cur, sql, rows, page_size=1000)
            updated += write_cur.rowcount or 0

        return updated, unknown


# ----------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------

def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main() -> int:
    _setup_logging()

    ap = argparse.ArgumentParser(
        description="Recheck is_en for selected platforms.")
    ap.add_argument("--prod", action="store_true",
                    help="Use prod DB pool prefix.")
    args = ap.parse_args()

    init_pool(prefix="prod" if args.prod else "dev")
    try:
        IsEnRechecker().run(platforms=RECHECK_PLATFORMS)
        return 0
    finally:
        close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
