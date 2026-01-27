from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv
from psycopg2.extras import execute_batch

from db.db import init_pool, close_pool, getcursor
from lang.detect_lang import detect_is_en

load_dotenv()

BATCH_SIZE = 2000
MIN_LEN = 24
MIN_CONF = 0.65

# ----------------------------------------------------------------------
# Platform-specific routing (Needs to be updated on new platform additions)
# ----------------------------------------------------------------------

PLATFORM_UPDATE_SPEC: dict[str, tuple[str, str, str | None]] = {
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


def build_update_sql(
    table: str,
    key1_col: str,
    key2_col: str,
) -> str:
    if key2_col == "":
        return f"""
            UPDATE {table}
               SET is_en = %s
             WHERE {key1_col} = %s
        """
    else:
        return f"""
            UPDATE {table}
               SET is_en = %s
             WHERE {key1_col} = %s
               AND {key2_col} = %s
        """

# ----------------------------------------------------------------------
# Core service
# ----------------------------------------------------------------------


class LanguageLabeler:
    def run_once(self) -> None:
        log = logging.getLogger(__name__)
        log.info("language labeler: starting run")

        with getcursor() as cur:
            last_checked = self._get_cursor(cur)
            max_id = self._get_max_post_id(cur)

        if max_id <= last_checked:
            log.info("language labeler: no new posts")
            self._update_cursor(last_checked)
            return

        scanned = updated = unknown = 0

        with getcursor() as read_cur, getcursor(commit=True) as write_cur:
            for batch in self._iter_posts(
                read_cur,
                min_id=last_checked,
                max_id=max_id,
            ):
                scanned += len(batch)

                updates_by_platform: Dict[str,
                                          List[Tuple[Any, ...]]] = defaultdict(list)

                for post_id, platform, key1, key2, text in batch:
                    spec = PLATFORM_UPDATE_SPEC.get(platform)
                    if not spec:
                        continue

                    r = detect_is_en(text, min_len=MIN_LEN, min_conf=MIN_CONF)
                    if r is None:
                        unknown += 1
                        continue

                    if spec[2] == "":
                        updates_by_platform[platform].append((r, key1))
                    else:
                        updates_by_platform[platform].append((r, key1, key2))

                for platform, rows in updates_by_platform.items():
                    table, key1_col, key2_col = PLATFORM_UPDATE_SPEC[platform]
                    sql = build_update_sql(table, key1_col, key2_col)
                    execute_batch(write_cur, sql, rows, page_size=1000)
                    updated += write_cur.rowcount or 0

        self._update_cursor(max_id)

        log.info(
            "language labeler: scanned=%d updated=%d unknown=%d",
            scanned,
            updated,
            unknown,
        )

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def _iter_posts(self, cur, *, min_id: int, max_id: int):
        cur.execute(
            """
            SELECT
                post_id,
                platform,
                key1,
                key2,
                text
            FROM sm.posts_all
            WHERE post_id > %s
              AND post_id <= %s
              AND text IS NOT NULL
            ORDER BY post_id
            """,
            (min_id, max_id),
        )

        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            yield rows

    # ------------------------------------------------------------------
    # Cursor helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_cursor(cur) -> int:
        cur.execute(
            """
            SELECT last_checked_post_id
            FROM sm.lang_label_state
            WHERE id = 'global'
            """
        )
        row = cur.fetchone()
        if row:
            return int(row[0])

        cur.execute(
            """
            INSERT INTO sm.lang_label_state (id, last_checked_post_id)
            VALUES ('global', 0)
            ON CONFLICT DO NOTHING
            """
        )
        return 0

    @staticmethod
    def _update_cursor(post_id: int) -> None:
        with getcursor(commit=True) as cur:
            cur.execute(
                """
                UPDATE sm.lang_label_state
                   SET last_checked_post_id = %s,
                       last_run_at = now()
                 WHERE id = 'global'
                """,
                (post_id,),
            )

    @staticmethod
    def _get_max_post_id(cur) -> int:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM sm.post_registry")
        return int(cur.fetchone()[0])


# ----------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    init_pool()
    try:
        LanguageLabeler().run_once()
    finally:
        close_pool()


def test_sample(
    *,
    sample_size: int = 100,
) -> None:
    """
    Pull a small sample of posts and run detect_is_en() on each.
    """
    from db.db import init_pool, close_pool, getcursor

    init_pool()
    try:
        with getcursor() as cur:
            cur.execute(
                """
                SELECT
                    post_id,
                    platform,
                    text
                FROM sm.posts_all
                WHERE text IS NOT NULL
                ORDER BY random()
                LIMIT %s
                """,
                (int(sample_size),),
            )
            rows = cur.fetchall()

        for _post_id, _platform, text in rows:
            detect_is_en(text)
    finally:
        close_pool()


if __name__ == "__main__":
    test_sample()
