from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import sys

from db.db import init_pool, getcursor, close_pool

MIGRATIONS_DIR = Path("db/migrations")


def compute_checksum(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def find_migration_file(migration_num: int) -> Path:
    prefix = f"{migration_num:03d}_"
    matches = list(MIGRATIONS_DIR.glob(f"{prefix}*.sql"))
    if not matches:
        raise FileNotFoundError(
            f"No migration file found with prefix {prefix}")
    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple migration files found for prefix {prefix}: {matches}"
        )
    return matches[0]


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Patch checksum for an already-applied migration (DEV ONLY)."
    )
    ap.add_argument(
        "migration_num",
        type=int,
        help="Migration number (e.g. 13 for 013_*.sql)",
    )
    args = ap.parse_args()

    init_pool(prefix="dev")

    try:
        try:
            path = find_migration_file(args.migration_num)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)

        version = path.name
        new_checksum = compute_checksum(path)

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
            print(f"No change: checksum for {version} is unchanged.")
            return

        with getcursor(commit=True) as cur:
            cur.execute(
                """
                UPDATE schema_migrations
                SET checksum = %s
                WHERE version = %s
                """,
                (new_checksum, version),
            )

        print("Checksum patched successfully:")
        print(f"  migration : {version}")
        print(f"  old       : {old_checksum}")
        print(f"  new       : {new_checksum}")

    finally:
        close_pool()


if __name__ == "__main__":
    main()
