
from __future__ import annotations

import logging

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from db.db import init_pool, close_pool, getcursor
from db.post_registry_utils import ensure_post_registered

load_dotenv()

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    init_pool(prefix="prod")
    try:
        res = input(
            "are you sure you want to edit prod db? type 'yes' to continue: ")
        if res.lower().strip() != "yes":
            log.info("aborted")
            return

        # Read eligible episodes
        with getcursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id
                FROM podcasts.episodes
                WHERE transcript IS NOT NULL
                  AND btrim(transcript) <> ''
                ORDER BY podcast_id, id;
                """
            )
            eligible_episodes = cur.fetchall()

        log.info("eligible episodes: %d", len(eligible_episodes))

        # Register them (commit)
        registered = 0
        with getcursor(commit=True) as write_cur:
            for i, episode in enumerate(eligible_episodes, start=1):
                ensure_post_registered(
                    write_cur,
                    platform="podcast_episode",
                    key1=episode["id"],
                )
                registered += 1

                if i % 1000 == 0:
                    log.info("processed %d / %d", i, len(eligible_episodes))

        log.info("done. ensure_post_registered called for %d episodes.", registered)

    finally:
        close_pool()


if __name__ == "__main__":
    main()
