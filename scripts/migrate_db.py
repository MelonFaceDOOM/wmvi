import argparse
from db.db import init_pool, close_pool
from db.migrations_runner import run_migrations


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Apply migrations to PROD (will prompt for confirmation).",
    )
    args = ap.parse_args()

    if args.prod:
        resp = input(
            "WARNING -- apply migrations to PROD? (y/n): ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return

        init_pool(prefix="PROD")
    else:
        init_pool()

    try:
        applied = run_migrations(migrations_dir="db/migrations")
        print("Applied migrations:", applied)
    finally:
        close_pool()


if __name__ == "__main__":
    main()
