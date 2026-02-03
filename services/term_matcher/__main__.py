import argparse
from .term_matcher import main

"""
TO RUN ON DEV:
python -m services.term_matcher

TO RUN ON PROD:
python -m services.term_matcher --prod

calls must come from root dir (wmvi):
"""

def _parse_args():
    ap = argparse.ArgumentParser(prog="python -m services.term_matcher")
    ap.add_argument("--prod", action="store_true", help="Run against PROD (default: dev).")
    ap.add_argument(
        "--terms",
        action="append",
        help="Restrict run to these exact term names (repeatable).",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(prod=args.prod, terms=args.terms)
