from __future__ import annotations

import argparse
import logging
from typing import List, Sequence

from dotenv import load_dotenv

from db.db import init_pool, close_pool, getcursor

from .term_matcher import MATCHER_VERSION, TermMatcher, setup_logging
from .queries import (
    get_term_stats,
    get_vaccine_terms,
    get_vaccine_terms_like,
    get_terms_by_ids,
    get_terms_by_names,
)

load_dotenv()


def _cmd_run_loop(args: argparse.Namespace) -> None:
    matcher = TermMatcher(
        matcher_version=args.matcher_version,
        per_term_sleep_seconds=args.per_term_sleep,
        loop_sleep_seconds=args.loop_sleep,
    )
    matcher.run_loop()


def _cmd_run_once(args: argparse.Namespace) -> None:
    matcher = TermMatcher(
        matcher_version=args.matcher_version,
        per_term_sleep_seconds=args.per_term_sleep,
        loop_sleep_seconds=args.loop_sleep,
    )
    matcher.run_once()


def _cmd_run_ids(args: argparse.Namespace) -> None:
    matcher = TermMatcher(
        matcher_version=args.matcher_version,
        per_term_sleep_seconds=args.per_term_sleep,
        loop_sleep_seconds=args.loop_sleep,
    )
    matcher.run_for_term_ids(args.term_id or [])


def _cmd_run_names(args: argparse.Namespace) -> None:
    matcher = TermMatcher(
        matcher_version=args.matcher_version,
        per_term_sleep_seconds=args.per_term_sleep,
        loop_sleep_seconds=args.loop_sleep,
    )
    matcher.run_for_term_names(args.term_name or [])


def _cmd_print_terms(args: argparse.Namespace) -> None:
    with getcursor() as cur:
        if args.filter:
            terms = get_vaccine_terms_like(cur, args.filter)
        else:
            terms = get_vaccine_terms(cur)

    if not terms:
        print("No terms found.")
        return

    for term_id, name in terms:
        print(f"{term_id:6d}  {name}")


def _resolve_term_ids_from_args(args: argparse.Namespace) -> List[int]:
    """
    For stats-related commands: resolve term_ids from --term-id and --term-name.
    """
    term_ids: List[int] = []
    if args.term_id:
        term_ids.extend(int(tid) for tid in args.term_id)

    if args.term_name:
        with getcursor() as cur:
            rows = get_terms_by_names(cur, args.term_name)
        term_ids.extend(tid for (tid, _name) in rows)

    # dedupe while preserving order
    seen = set()
    deduped: List[int] = []
    for tid in term_ids:
        if tid not in seen:
            seen.add(tid)
            deduped.append(tid)
    return deduped


def _cmd_stats(args: argparse.Namespace) -> None:
    matcher_version = args.matcher_version or MATCHER_VERSION
    term_ids = _resolve_term_ids_from_args(args)

    with getcursor() as cur:
        stats = get_term_stats(cur, matcher_version=matcher_version, term_ids=term_ids or None)

    if not stats:
        print("No stats available (no terms or no state).")
        return

    # Header
    print(
        f"{'term_id':7s}  {'name':30s}  {'matches':8s}  "
        f"{'last_checked':12s}  {'coverage%':10s}"
    )

    for row in stats:
        term_id = row["term_id"]
        name = row["name"]
        match_count = row["match_count"]
        last_checked = row["last_checked_post_id"]
        coverage_pct = row["coverage"] * 100.0
        print(
            f"{term_id:7d}  {name[:30]:30s}  {match_count:8d}  "
            f"{(last_checked or 0):12d}  {coverage_pct:9.1f}"
        )


def _cmd_stats_top(args: argparse.Namespace) -> None:
    matcher_version = args.matcher_version or MATCHER_VERSION
    limit = int(args.limit)

    with getcursor() as cur:
        stats = get_term_stats(cur, matcher_version=matcher_version, term_ids=None)

    if not stats:
        print("No stats available.")
        return

    # Sort by match_count desc, then term_id asc
    stats.sort(key=lambda r: (-r["match_count"], r["term_id"]))
    top = stats[:limit]

    print(
        f"{'rank':4s}  {'term_id':7s}  {'name':30s}  {'matches':8s}  "
        f"{'coverage%':10s}"
    )
    for idx, row in enumerate(top, start=1):
        term_id = row["term_id"]
        name = row["name"]
        match_count = row["match_count"]
        coverage_pct = row["coverage"] * 100.0
        print(
            f"{idx:4d}  {term_id:7d}  {name[:30]:30s}  "
            f"{match_count:8d}  {coverage_pct:9.1f}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="term_matcher",
        description="Term matcher and stats CLI for vaccine terms.",
    )
    parser.add_argument(
        "--matcher-version",
        default=MATCHER_VERSION,
        help=f"Matcher version to use (default: {MATCHER_VERSION})",
    )
    parser.add_argument(
        "--per-term-sleep",
        type=float,
        default=0.05,
        help="Pause in seconds between processing terms (default: 0.05).",
    )
    parser.add_argument(
        "--loop-sleep",
        type=float,
        default=60.0,
        help="Pause in seconds between full passes in run-loop (default: 60).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # run-loop
    p_loop = subparsers.add_parser("run-loop", help="Run matcher in infinite loop.")
    p_loop.set_defaults(func=_cmd_run_loop)

    # run-once
    p_once = subparsers.add_parser("run-once", help="Run matcher over all terms once and exit.")
    p_once.set_defaults(func=_cmd_run_once)

    # run-ids
    p_ids = subparsers.add_parser(
        "run-ids",
        help="Run matcher once for specific term IDs.",
    )
    p_ids.add_argument(
        "--term-id",
        nargs="+",
        type=int,
        required=True,
        help="One or more term IDs.",
    )
    p_ids.set_defaults(func=_cmd_run_ids)

    # run-names
    p_names = subparsers.add_parser(
        "run-names",
        help="Run matcher once for specific term names (exact matches).",
    )
    p_names.add_argument(
        "--term-name",
        nargs="+",
        required=True,
        help="One or more term names (exact).",
    )
    p_names.set_defaults(func=_cmd_run_names)

    # print-terms
    p_print = subparsers.add_parser(
        "print-terms",
        help="Print all terms (optionally filter by substring).",
    )
    p_print.add_argument(
        "--filter",
        help="Substring to filter term names (ILIKE).",
    )
    p_print.set_defaults(func=_cmd_print_terms)

    # stats
    p_stats = subparsers.add_parser(
        "stats",
        help="Show stats per term: matches and coverage for this matcher version.",
    )
    p_stats.add_argument(
        "--term-id",
        nargs="+",
        type=int,
        help="Restrict stats to these term IDs.",
    )
    p_stats.add_argument(
        "--term-name",
        nargs="+",
        help="Restrict stats to these term names (exact).",
    )
    p_stats.set_defaults(func=_cmd_stats)

    # stats-top
    p_top = subparsers.add_parser(
        "stats-top",
        help="Show top N terms by number of matches.",
    )
    p_top.add_argument(
        "--limit",
        type=int,
        default=20,
        help="How many top terms to show (default: 20).",
    )
    p_top.set_defaults(func=_cmd_stats_top)

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    setup_logging()
    init_pool()  # Uses env DEFAULT_DB / prefix
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        args.func(args)
    finally:
        close_pool()


if __name__ == "__main__":
    main()
