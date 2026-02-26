from __future__ import annotations

"""
Patch checksum(s) for already-applied migrations.

Examples:
  # Patch a single migration (DEV)
  python -m scripts.patch_migration_checksum 13

  # Patch ALL migrations (DEV)
  python -m scripts.patch_migration_checksum all

  # Patch a single migration (PROD)
  python -m scripts.patch_migration_checksum 13 --prod
"""

import argparse
import hashlib
from pathlib import Path
import sys
from typing import Iterable, List

from db.db import init_pool, getcursor, close_pool
from db.migrations_runner import _sha256_canonical_sql

MIGRATIONS_DIR = Path("db/migrations")


def find_migration_file(migration_num: int) -> Path:
    prefix = f"{migration_num:03d}_"
    matches = list(MIGRATIONS_DIR.glob(f"{prefix}*.sql"))
    if not matches:
        raise FileNotFoundError(f"No migration file found with prefix {prefix}")
    if len(matches) > 1:
        raise RuntimeError(f"Multiple migration files found for prefix {prefix}: {matches}")
    return matches[0]


def list_migration_files() -> List[Path]:
    # Expect names like 001_foo.sql, 013_bar.sql
    # Sort lexicographically so the numeric prefix orders correctly.
    return sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql"))


def patch_one(version: str, path: Path) -> tuple[bool, str, str]:
    """
    Returns: (changed, old_checksum, new_checksum)
    Exits with an error if migration version not found in schema_migrations.
    """
    new_checksum = _sha256_canonical_sql(path)

    with getcursor() as cur:
        cur.execute(
            """
            SELECT checksum
            FROM schema_migrations
            WHERE version = %s
            """,
            (version,),
        )
        row = cur.fetchone()

    if row is None:
        print(
            f"ERROR: migration {version} not found in schema_migrations",
            file=sys.stderr,
        )
        sys.exit(1)

    old_checksum = row[0]

    if old_checksum == new_checksum:
        return (False, old_checksum, new_checksum)

    with getcursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE schema_migrations
            SET checksum = %s
            WHERE version = %s
            """,
            (new_checksum, version),
        )

    return (True, old_checksum, new_checksum)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Patch checksum for already-applied migrations (use with care)."
    )
    ap.add_argument(
        "migration",
        help="Migration number (e.g. 13 for 013_*.sql) or 'all'",
    )
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Run against PROD (default: dev).",
    )
    args = ap.parse_args()

    init_pool(prefix="prod" if args.prod else "dev")

    try:
        if args.migration.lower() == "all":
            files = list_migration_files()
            if not files:
                print(f"ERROR: no migration files found in {MIGRATIONS_DIR}", file=sys.stderr)
                sys.exit(1)

            changed = 0
            unchanged = 0

            print(f"Patching ALL migrations ({len(files)}) from {MIGRATIONS_DIR}...")
            for path in files:
                version = path.name
                ok, old_cs, new_cs = patch_one(version, path)
                if ok:
                    changed += 1
                    print(f"CHANGED  {version}  {old_cs} -> {new_cs}")
                else:
                    unchanged += 1
                    print(f"OK       {version}  {new_cs}")

            print()
            print("Done:")
            print(f"  changed   : {changed}")
            print(f"  unchanged : {unchanged}")
            return

        # single migration number
        try:
            num = int(args.migration)
        except ValueError:
            print("ERROR: migration must be an integer or 'all'", file=sys.stderr)
            sys.exit(1)

        try:
            path = find_migration_file(num)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)

        version = path.name
        ok, old_checksum, new_checksum = patch_one(version, path)

        if not ok:
            print(f"No change: checksum for {version} is unchanged.")
            return

        print("Checksum patched successfully:")
        print(f"  migration : {version}")
        print(f"  old       : {old_checksum}")
        print(f"  new       : {new_checksum}")

    finally:
        close_pool()


if __name__ == "__main__":
    main()