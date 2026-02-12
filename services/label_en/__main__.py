import argparse

from .label_en import main

"""
TO RUN ON DEV:
python -m services.label_en

TO RUN ON PROD:
python -m services.label_en --prod

calls must come from root dir (wmvi):
"""


def _parse_args():
    ap = argparse.ArgumentParser(prog="python -m services.label_en")
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Run against PROD (default: dev).",
    )
    ap.add_argument(
        "--recheck",
        nargs="?",              # optional value
        const="__ALL__",        # value when flag is present with no arg
        default=None,           # flag not present
        metavar="PLATFORM",
        help=(
            "Recheck old posts where post_id < last_checked_post_id and is_en IS NULL. "
            "Optionally restrict to one platform: --recheck podcast_episode"
        ),
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(prod=args.prod, recheck=args.recheck)
