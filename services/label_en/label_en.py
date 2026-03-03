from __future__ import annotations

import logging
import signal
import threading
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

_STOP = threading.Event()  # long loops will check if this is set to enable faster quits on ctrl-c

def _handle_signal(signum, frame):
    logging.getLogger(__name__).warning("Received signal %s, stopping...", signum)
    _STOP.set()


def build_update_sql_from_row(row_cls) -> str:
    table = row_cls.TABLE
    pk = row_cls.PK

    if len(pk) == 1:
        return f"UPDATE {table} SET is_en = %s WHERE {pk[0]} = %s"
    if len(pk) == 2:
        return f"UPDATE {table} SET is_en = %s WHERE {pk[0]} = %s AND {pk[1]} = %s"

    raise ValueError(f"Unsupported PK length for {row_cls}: {pk!r}")



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
                if _STOP.is_set():
                    log.info("language labeler: stop requested, exiting early")
                    break
                scanned += len(batch)
                u, unk = self._label_batch(batch, write_cur)
                updated += u
                unknown += unk

        self._update_cursor(max_id)

        log.info("language labeler: scanned=%d updated=%d unknown=%d (unknown are still marked as False)",
                 scanned, updated, unknown)

    def recheck_old_unlabeled(self, platform: str | None = None) -> None:
        """
        Recheck ALL posts with:
          - post_id < last_checked_post_id
          - is_en IS NULL (as exposed through sm.posts_all)
        """
        log = logging.getLogger(__name__)
        log.info(
            "language labeler: starting recheck of old unlabeled posts platform=%s", platform or "ALL")

        with getcursor() as cur:
            last_checked = self._get_cursor(cur)

        scanned = updated = unknown = 0

        with getcursor() as read_cur, getcursor(commit=True) as write_cur:
            for batch in self._iter_posts_old_unlabeled(
                read_cur, before_id=last_checked, platform=platform
            ):
                if _STOP.is_set():
                    log.info("language labeler: stop requested, exiting early")
                    break
                scanned += len(batch)
                u, unk = self._label_batch(batch, write_cur)
                updated += u
                unknown += unk

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
        from ingestion.platform_registry import PLATFORM_ROW  # platform -> InsertableRow subclass

        updates_by_platform: Dict[str, List[Tuple[Any, ...]]] = defaultdict(list)
        unknown = 0

        # Cache update SQL per platform to avoid rebuilding in the loop
        sql_by_platform: dict[str, str] = {}

        for _post_id, platform, key1, key2, text in batch:
            if _STOP.is_set():
                break
            row_cls = PLATFORM_ROW.get(platform)
            if row_cls is None:
                continue

            r = detect_is_en(text, min_len=MIN_LEN, min_conf=MIN_CONF)
            if r is None:
                unknown += 1
                r = False  # still mark as False if unknown

            pk = row_cls.PK

            # Build SQL once per platform
            if platform not in sql_by_platform:
                sql_by_platform[platform] = build_update_sql_from_row(row_cls)

            # Queue row update args
            if len(pk) == 1:
                updates_by_platform[platform].append((r, key1))
            else:
                updates_by_platform[platform].append((r, key1, key2))

        updated = 0
        for platform, rows in updates_by_platform.items():
            sql = sql_by_platform[platform]
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
            if _STOP.is_set():
                break
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            yield rows

    def _iter_posts_old_unlabeled(self, cur, *, before_id: int, platform: str | None = None):
        sql = """
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
        """
        params: list = [before_id]

        if platform is not None:
            sql += " AND platform = %s"
            params.append(platform)

        sql += " ORDER BY post_id"

        cur.execute(sql, params)

        while True:
            if _STOP.is_set():
                break
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

def main(prod: bool = False, recheck: str | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    init_pool(prefix="prod" if prod else "dev")
    try:
        if recheck is not None:
            platform = None if recheck == "__ALL__" else recheck
            LanguageLabeler().recheck_old_unlabeled(platform=platform)
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
