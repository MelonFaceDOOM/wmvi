import argparse
from .youtube_transciber import main

"""
TO RUN ON DEV:
python -m services.youtube_transciber

TO RUN ON PROD:
python -m services.youtube_transciber --prod

calls must come from root dir (wmvi):
"""


def _parse_args():
    ap = argparse.ArgumentParser(prog="python -m services.youtube_transciber")
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Run against PROD (default: dev).",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(prod=args.prod)
