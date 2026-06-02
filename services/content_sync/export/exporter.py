from __future__ import annotations

import logging
from datetime import datetime

from dotenv import load_dotenv

from content_sync.export_runner import run_export
from db.db import close_pool, init_pool

load_dotenv()

log = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main(
    *,
    prod: bool = False,
    since_override: datetime | None = None,
    dry_run: bool = False,
) -> None:
    _setup_logging()
    prefix = "prod" if prod else "dev"
    init_pool(prefix=prefix)
    log.info("Initialized DB pool with %s prefix.", prefix.upper())
    try:
        run_export(since_override=since_override, dry_run=dry_run)
    finally:
        close_pool()
