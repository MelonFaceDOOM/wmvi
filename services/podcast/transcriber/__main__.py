import argparse
from .transcriber import main


"""
TO RUN ON DEV:
python -m services.podcast.transcriber

TO RUN ON PROD:
python -m services.podcast.transcriber --prod

Run from repo root. GPU setup: transcription/README.md
"""


def _parse_args():
    ap = argparse.ArgumentParser(prog="python -m services.transcriber")
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Run against PROD (default: dev).",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(prod=args.prod)
