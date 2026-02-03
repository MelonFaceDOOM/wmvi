from db.db import init_pool, getcursor, close_pool
import argparse

SUBSET_NAME = "core_search_terms"
SUBSET_DESCRIPTION = "core search terms initially selected for economical youtube monitoring"


subset_terms = [
    "vaccines autism",
    "mmr autism",
    "autism vaccine",
    "childhood vaccines autism",
    "vaccine",
    "vaccines",
    "vaccination",
    "covid vaccine",
    "cdc",
    "acip",
    "hhs",
    "vaccine panel",
    "mmr vaccine",
    "measles vaccine",
    "mumps vaccine",
    "rubella vaccine",
    "measles",
    "rfk",
    "national vaccine information center",
    "martin kulldorff",
]


def do_changes(prefix):
    init_pool(prefix=prefix)
    with getcursor(commit=True) as cur:

        # ------------------------------------------------------------
        # 1) Insert new vaccine terms (if missing)
        # ------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO taxonomy.vaccine_term (name, type)
            SELECT v.term, 'search'
            FROM unnest(%s::text[]) AS v(term)
            ON CONFLICT (name) DO NOTHING
            """,
            (subset_terms,),
        )

        # ------------------------------------------------------------
        # 2) Create subset (if missing)
        # ------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO taxonomy.vaccine_term_subset (name, description)
            VALUES (%s, %s)
            ON CONFLICT (name) DO NOTHING
            """,
            (SUBSET_NAME, SUBSET_DESCRIPTION),
        )

        # ------------------------------------------------------------
        # 3) Link subset to terms
        # ------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO taxonomy.vaccine_term_subset_member (subset_id, term_id)
            SELECT
                s.id AS subset_id,
                t.id AS term_id
            FROM taxonomy.vaccine_term_subset s
            JOIN taxonomy.vaccine_term t
              ON t.name = ANY(%s::text[])
            WHERE s.name = %s
            ON CONFLICT DO NOTHING
            """,
            (subset_terms, SUBSET_NAME),
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Apply changes to PROD (will prompt for confirmation).",
    )
    args = ap.parse_args()

    prefix = "dev"
    if args.prod:
        resp = input(
            "WARNING -- apply migrations to PROD? (y/n): ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return
        prefix = "prod"
    try:
        do_changes(prefix=prefix)
        print("changes applied")
    finally:
        close_pool()


if __name__ == "__main__":
    main()
