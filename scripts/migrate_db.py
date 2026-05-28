import argparse

from db.db import close_pool, init_pool
from db.migrations_runner import run_migrations, stamp_migrations


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Apply or stamp SQL migrations.",
        epilog=(
            "If the DB schema was copied from prod but schema_migrations is empty, "
            "stamp first then migrate:\n"
            "  python -m scripts.migrate_db --test --stamp --stamp-up-to 21\n"
            "  python -m scripts.migrate_db --test"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = ap.add_mutually_exclusive_group()
    group.add_argument(
        "--test",
        action="store_true",
        help="Apply migrations to TEST.",
    )
    group.add_argument(
        "--prod",
        action="store_true",
        help="Apply migrations to PROD (will prompt for confirmation).",
    )
    ap.add_argument(
        "--stamp",
        action="store_true",
        help="Record migrations as applied without running SQL (for restored DBs).",
    )
    ap.add_argument(
        "--stamp-up-to",
        type=int,
        metavar="N",
        help="With --stamp, only migrations numbered <= N (e.g. 21 for 021_*.sql).",
    )
    args = ap.parse_args()

    if args.prod:
        resp = input("WARNING -- apply migrations to PROD? (y/n): ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return
        init_pool(prefix="PROD")
    elif args.test:
        init_pool(prefix="TEST")
    else:
        init_pool()

    try:
        if args.stamp:
            if args.prod:
                resp = input(
                    "WARNING -- stamp schema_migrations on PROD without running SQL? (y/n): "
                ).strip().lower()
                if resp not in {"y", "yes"}:
                    print("Aborted.")
                    return
            stamped = stamp_migrations(
                migrations_dir="db/migrations",
                up_to=args.stamp_up_to,
            )
            print("Stamped migrations:", stamped or "(none — already up to date)")
            return

        applied = run_migrations(migrations_dir="db/migrations")
        print("Applied migrations:", applied)
    finally:
        close_pool()


if __name__ == "__main__":
    main()
