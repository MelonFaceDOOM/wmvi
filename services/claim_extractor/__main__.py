import argparse

from .claim_extractor import main

"""
TO RUN ON DEV:
python -m services.claim_extractor -n=2000 -o="outfile.jsonl"

TO RUN ON PROD:
python -m services.claim_extractor -n=2000 -o="outfile.jsonl" --prod

Calls must come from root dir (wmvi).
"""


def _parse_args():
    ap = argparse.ArgumentParser(prog="python -m services.claim_extractor")
    ap.add_argument(
        "-n",
        type=int,
        required=True,
        help="Number of posts to sample from the database.",
    )
    ap.add_argument(
        "-o",
        "--out",
        required=True,
        help="Output JSONL path.",
    )
    ap.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=6,
        help="Max concurrent prompt requests (default: 6).",
    )
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Run against PROD (default: DEV).",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(
        n=args.n,
        out_jsonl=args.out,
        concurrency=args.concurrency,
        prod=args.prod,
    )