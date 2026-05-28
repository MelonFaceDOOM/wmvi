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
        "--bundle",
        default=None,
        help="Import a single bundle folder name (e.g. 2026-05-22T14-30-45Z). "
        "Default: import all bundles newer than last_imported_at in DB.",
    )
    ap.add_argument(
        "--date",
        default=None,
        help="Deprecated alias for --bundle.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse file and count rows without writing to DB.",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Import all bundles on nitwitch, ignoring last_imported_at.",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    prod = not args.dev
    bundle_id = args.bundle or args.date
    main(
        prod=prod,
        bundle_id=bundle_id,
        dry_run=args.dry_run,
        force=args.force,
    )
