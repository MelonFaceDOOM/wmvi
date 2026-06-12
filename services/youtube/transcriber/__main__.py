import argparse
from .transcriber import main

"""
TO RUN ON DEV:
python -m services.youtube.transcriber

TO RUN ON PROD:
python -m services.youtube.transcriber --prod

Run from repo root. Cookies/proxy/xrdp: transcription/youtube.md
"""


def _parse_args():
    ap = argparse.ArgumentParser(prog="python -m services.youtube.transcriber")
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Run against PROD (default: dev).",
    )
    ap.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="One-shot mode: transcribe up to N videos, then exit (skips session scheduler).",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be >= 1")
    main(prod=args.prod, limit=args.limit)
