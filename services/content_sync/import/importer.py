from __future__ import annotations

import logging

from dotenv import load_dotenv

from content_sync.import_runner import run_import
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
    bundle_id: str | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    _setup_logging()
    prefix = "prod" if prod else "dev"
    init_pool(prefix=prefix)
    log.info("Initialized DB pool with %s prefix.", prefix.upper())
    try:
        run_import(bundle_id=bundle_id, dry_run=dry_run, force=force)
    finally:
        close_pool()
