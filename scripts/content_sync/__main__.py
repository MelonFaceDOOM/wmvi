from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


def _parse_args():
    ap = argparse.ArgumentParser(
        prog="python -m scripts.content_sync",
        description="Unified content sync export/import (dev GPU -> nitwitch -> prod).",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_export = sub.add_parser("export", help="Export delta bundle to configured storage")
    ap_export.add_argument(
        "--prod",
        action="store_true",
        help="Use PROD DB pool (default: dev).",
    )
    ap_export.add_argument(
        "--since",
        default=None,
        help="Override export watermark (ISO8601).",
    )
    ap_export.add_argument(
        "--dry-run",
        action="store_true",
        help="Log counts only; do not write bundle or upload.",
    )
    ap_export.add_argument(
        "--platform",
        action="append",
        dest="platforms",
        help="Limit to platform(s); repeatable. Default: all v1 platforms.",
    )

    ap_import = sub.add_parser("import", help="Import pending bundles from storage")
    ap_import.add_argument(
        "--prod",
        action="store_true",
        help="Use PROD DB pool (default: dev).",
    )
    ap_import.add_argument(
        "--bundle",
        default=None,
        help="Import a single bundle folder name.",
    )
    ap_import.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse bundles without writing to DB.",
    )
    ap_import.add_argument(
        "--force",
        action="store_true",
        help="Import all bundles on storage, ignoring import watermark.",
    )

    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    from db.db import close_pool, init_pool

    prefix = "prod" if args.prod else "dev"
    init_pool(prefix=prefix)
    logging.info("Initialized DB pool with %s prefix.", prefix.upper())

    try:
        if args.cmd == "export":
            from content_sync.export_runner import run_export

            since = None
            if args.since:
                since = datetime.fromisoformat(args.since.replace("Z", "+00:00"))
            run_export(
                since_override=since,
                dry_run=args.dry_run,
                platforms=args.platforms,
            )
        elif args.cmd == "import":
            from content_sync.import_runner import run_import

            run_import(
                bundle_id=args.bundle,
                dry_run=args.dry_run,
                force=args.force,
            )
        else:
            print(f"unknown command: {args.cmd}", file=sys.stderr)
            raise SystemExit(2)
    finally:
        close_pool()


if __name__ == "__main__":
    main()
