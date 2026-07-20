"""List taxonomy.vaccine_term names for building --terms-file lists.

CLI::

    # All terms (dev), one name per line on stdout:
    python -m scripts.list_vaccine_terms

    # Prod + write a terms file:
    python -m scripts.list_vaccine_terms --prod --out my_terms.txt

    # Only terms in a named subset (e.g. core_search_terms):
    python -m scripts.list_vaccine_terms --prod --subset core_search_terms

    # List subset names:
    python -m scripts.list_vaccine_terms --list-subsets --prod
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from db.db import close_pool, getcursor, init_pool


def list_term_names(*, subset: str | None = None) -> list[str]:
    """Return sorted vaccine term names (optionally filtered to a subset)."""
    if subset:
        sql = """
            SELECT t.name
            FROM taxonomy.vaccine_term_subset s
            JOIN taxonomy.vaccine_term_subset_member m
              ON m.subset_id = s.id
            JOIN taxonomy.vaccine_term t
              ON t.id = m.term_id
            WHERE s.name = %s
            ORDER BY t.name
        """
        params: tuple[Any, ...] = (subset,)
    else:
        sql = """
            SELECT name
            FROM taxonomy.vaccine_term
            ORDER BY name
        """
        params = ()

    with getcursor() as cur:
        cur.execute(sql, params)
        return [str(row[0]) for row in cur.fetchall() if row and row[0] is not None]


def list_subset_names() -> list[dict[str, Any]]:
    """Return subset metadata sorted by name."""
    with getcursor() as cur:
        cur.execute(
            """
            SELECT
                s.name,
                s.description,
                COUNT(m.term_id)::int AS term_count
            FROM taxonomy.vaccine_term_subset s
            LEFT JOIN taxonomy.vaccine_term_subset_member m
              ON m.subset_id = s.id
            GROUP BY s.id, s.name, s.description
            ORDER BY s.name
            """
        )
        rows = cur.fetchall()
    return [
        {
            "name": str(name),
            "description": description,
            "term_count": int(term_count or 0),
        }
        for name, description, term_count in rows
    ]


def write_terms_file(path: Path, names: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(names) + ("\n" if names else ""), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(
        description="List taxonomy.vaccine_term names (for --terms-file)"
    )
    ap.add_argument("--prod", action="store_true", help="Use prod DB pool")
    ap.add_argument(
        "--subset",
        type=str,
        default=None,
        help="Only terms in taxonomy.vaccine_term_subset.name (e.g. core_search_terms)",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Write one term per line (default: print to stdout)",
    )
    ap.add_argument(
        "--list-subsets",
        action="store_true",
        help="Print subset names/counts as JSON and exit",
    )
    ap.add_argument(
        "--json",
        action="store_true",
        help="Print terms as a JSON array instead of one-per-line",
    )
    args = ap.parse_args(argv)

    init_pool(prefix="prod" if args.prod else "dev")
    try:
        if args.list_subsets:
            subsets = list_subset_names()
            print(json.dumps(subsets, ensure_ascii=False, indent=2), flush=True)
            return 0

        names = list_term_names(subset=args.subset)
        if args.subset is not None and not names:
            print(
                f"warning: subset {args.subset!r} matched 0 terms "
                "(unknown subset name, or empty membership)",
                file=sys.stderr,
                flush=True,
            )

        if args.out is not None:
            write_terms_file(args.out, names)
            print(
                json.dumps(
                    {
                        "out": str(args.out),
                        "term_count": len(names),
                        "subset": args.subset,
                        "use_prod": bool(args.prod),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            return 0

        if args.json:
            print(json.dumps(names, ensure_ascii=False, indent=2), flush=True)
        else:
            for name in names:
                print(name, flush=True)
        return 0
    finally:
        close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
