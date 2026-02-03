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
            for batch in self._iter_posts_new(read_cur, min_id=last_checked, max_id=max_id):
                scanned += len(batch)
                u, unk = self._label_batch(batch, write_cur)
                updated += u
                unknown += unk

        self._update_cursor(max_id)

        log.info("language labeler: scanned=%d updated=%d unknown=%d (unknown are still marked as False)",
                 scanned, updated, unknown)

    def recheck_old_unlabeled(self) -> None:
        """
        Recheck ALL posts with:
          - post_id < last_checked_post_id
          - is_en IS NULL (as exposed through sm.posts_all)
        """
        log = logging.getLogger(__name__)
        log.info("language labeler: starting recheck of old unlabeled posts")

        with getcursor() as cur:
            last_checked = self._get_cursor(cur)

        scanned = updated = unknown = 0

        with getcursor() as read_cur, getcursor(commit=True) as write_cur:
            for batch in self._iter_posts_old_unlabeled(read_cur, before_id=last_checked):
                scanned += len(batch)
                u, unk = self._label_batch(batch, write_cur)
                updated += u
                unknown += unk

        # Intentionally do NOT move the cursor in recheck mode.
        log.info(
            "language labeler: recheck complete scanned=%d updated=%d unknown=%d (unknown still marked as False)",
            scanned,
            updated,
            unknown,
        )

    # ------------------------------------------------------------------
    # Shared labeling logic
    # ------------------------------------------------------------------

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
                r = False  # still mark as False if unknown

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

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def _iter_posts_new(self, cur, *, min_id: int, max_id: int):
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

    def _iter_posts_old_unlabeled(self, cur, *, before_id: int):
        cur.execute(
            """
            SELECT
                post_id,
                platform,
                key1,
                key2,
                text
            FROM sm.posts_all
            WHERE post_id < %s
              AND is_en IS NULL
              AND text IS NOT NULL
            ORDER BY post_id
            """,
            (before_id,),
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
            select last_checked_post_id
            from sm.lang_label_state
            where id = 'global'
            """
        )
        row = cur.fetchone()
        if row:
            return int(row[0])

        cur.execute(
            """
            insert into sm.lang_label_state (id, last_checked_post_id)
            values ('global', 0)
            on conflict do nothing
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

def main(prod=False, recheck=False) -> None:
    if prod:
        init_pool(prefix="prod")
    else:
        init_pool(prefix="dev")
    try:
        if recheck:
            LanguageLabeler().recheck_old_unlabeled()
        else:
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
