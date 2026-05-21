import argparse
from datetime import datetime

from .exporter import main


def _parse_args():
    ap = argparse.ArgumentParser(prog="python -m services.podcast.transcript_export")
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Run against PROD pool vars (on local PCs, point PROD_* at local DB).",
    )
    ap.add_argument(
        "--since",
        default=None,
        help="Override export watermark (ISO8601 timestamp).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Count rows only; do not write files, upload, or advance watermark.",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    since = None
    if args.since:
        since = datetime.fromisoformat(args.since.replace("Z", "+00:00"))
    main(prod=args.prod, since_override=since, dry_run=args.dry_run)
