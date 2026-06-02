import argparse

from .importer import main


def _parse_args():
    ap = argparse.ArgumentParser(
        prog="python -m services.content_sync.import",
    )
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Use PROD DB pool (default: dev).",
    )
    ap.add_argument(
        "--bundle",
        default=None,
        help="Import a single bundle folder name.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse bundles without writing to DB.",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Import all bundles on storage, ignoring import watermark.",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(
        prod=args.prod,
        bundle_id=args.bundle,
        dry_run=args.dry_run,
        force=args.force,
    )
