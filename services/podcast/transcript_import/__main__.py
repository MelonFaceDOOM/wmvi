import argparse

from .importer import main


def _parse_args():
    ap = argparse.ArgumentParser(prog="python -m services.podcast.transcript_import")
    ap.add_argument(
        "--dev",
        action="store_true",
        help="Run against DEV DB (default: PROD).",
    )
    ap.add_argument(
        "--date",
        default=None,
        help="Export bundle date (YYYY-MM-DD). Default: latest under PODCAST_SYNC_LOCAL_DIR.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse file and count rows without writing to DB.",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Re-apply even if this export_date was already processed.",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    prod = not args.dev
    main(
        prod=prod,
        export_date=args.date,
        dry_run=args.dry_run,
        force=args.force,
    )
