from __future__ import annotations

import argparse
import logging
import re
from typing import Iterable, List, Tuple

from dotenv import load_dotenv

from db.db import getcursor, init_pool, close_pool
from .queries import (
    get_all_terms,
    get_terms_by_names,
    get_latest_post_id,
    get_or_init_term_state,
    update_term_state,
    fetch_candidate_posts,
    insert_term_hits,
)

load_dotenv()

MATCHER_VERSION = "tsv_en_spans_v2"


# --------------------
# core matcher
# --------------------

def run(term_names: Iterable[str] | None = None) -> None:
    log = logging.getLogger("term_matcher")

    with getcursor() as cur:
        if term_names:
            terms = get_terms_by_names(cur, term_names)
        else:
            terms = get_all_terms(cur)

    if not terms:
        log.info("No terms to process.")
        return

    log.info("Processing %d terms.", len(terms))

    for term_id, term_name in terms:
        _process_term(term_id, term_name)


def _process_term(term_id: int, term: str) -> None:
    log = logging.getLogger("term_matcher")

    with getcursor() as cur:
        last_post_id = get_or_init_term_state(cur, term_id, MATCHER_VERSION)
        max_post_id = get_latest_post_id(cur)

        if max_post_id <= last_post_id:
            update_term_state(cur, term_id, MATCHER_VERSION, last_post_id)
            return

        candidates = fetch_candidate_posts(
            cur,
            term=term,
            min_post_id=last_post_id,
            max_post_id=max_post_id,
        )

        hits: List[Tuple[int, int, int, int, str]] = []

        # simple, explicit span extraction
        pattern = re.compile(re.escape(term), re.IGNORECASE)

        for post_id, text in candidates:
            for m in pattern.finditer(text):
                hits.append(
                    (
                        post_id,
                        term_id,
                        m.start(),
                        m.end(),
                        MATCHER_VERSION,
                    )
                )

        inserted = insert_term_hits(cur, hits)
        update_term_state(cur, term_id, MATCHER_VERSION, max_post_id)

    log.info(
        "term=%r scanned (%d, %d] candidates=%d hits=%d inserted=%d",
        term,
        last_post_id,
        max_post_id,
        len(candidates),
        len(hits),
        inserted,
    )


# --------------------
# CLI (minimal, obvious)
# --------------------

def main(prod=False) -> None:
    parser = argparse.ArgumentParser(
        description="Run term matcher over post_search_en.",
    )
    parser.add_argument(
        "--term",
        action="append",
        help="Restrict run to these exact term names (repeatable).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    if prod:
        init_pool(prefix="prod")
    else:
        init_pool(prefix="dev")
    try:
        run(args.term)
    finally:
        close_pool()


if __name__ == "__main__":
    main()
