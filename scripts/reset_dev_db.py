from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()

# run from project room with:
#  rely on existing backup file:
#     python -m scripts.reset_dev_db --no-backup 
#  overwrite existing backup file:
#     python -m scripts.reset_dev_db --force

# file to use
BASE_DUMP = "db/snapshots/base_after_001.dump"

# get credentials from env
PGHOST = os.getenv("DEV_PGHOST")
PGDATABASE = os.getenv("DEV_PGDATABASE")
PGUSER = os.getenv("DEV_PGUSER")
PGPASSWORD = os.getenv("DEV_PGPASSWORD")

for name, value in [("DEV_PGHOST", PGHOST), ("DEV_PGDATABASE", PGDATABASE), ("DEV_PGUSER", PGUSER)]:
    if not value:
        print(f"[error] Environment variable {name} is not set.", file=sys.stderr)
        sys.exit(1)

def run_cmd(cmd: list[str]) -> None:
    """Run a command and exit on failure."""
    print("+", " ".join(cmd))

    env = os.environ.copy()
    if PGPASSWORD:
        env["PGPASSWORD"] = PGPASSWORD  # Postgres will use this automatically

    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        print(f"[error] Command failed with exit code {result.returncode}", file=sys.stderr)
        sys.exit(result.returncode)

def backup_current_db(
    base_dump_path: str = BASE_DUMP,
    force: bool = False
) -> Path:
    """Take a compressed pg_dump of the current DB."""
    if not force:
        if os.path.isfile(base_dump_path):
            print(f"the file {base_dump_path} already exists.", file=sys.stderr)
            sys.exit(1)

    cmd = [
        "pg_dump",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", PGDATABASE,
        "-Fc",
        "-f", base_dump_path,
    ]
    run_cmd(cmd)

    if not os.path.isfile(base_dump_path):
        print(f"[error] Backup file {base_dump_path} was not created.", file=sys.stderr)
        sys.exit(1)

    print(f"[ok] Backup written to {base_dump_path}")
    return base_dump_path

def drop_and_create_db() -> None:
    """Terminate connections, drop the DB, and recreate it."""
    # 1) Terminate connections
    terminate_sql = f"""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = '{PGDATABASE}'
          AND pid <> pg_backend_pid();
    """

    cmd_terminate = [
        "psql",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", "postgres",
        "-c", terminate_sql,
    ]
    run_cmd(cmd_terminate)

    # 2) Drop DB (if exists)
    cmd_drop = [
        "psql",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", "postgres",
        "-c", f"DROP DATABASE IF EXISTS {PGDATABASE};",
    ]
    run_cmd(cmd_drop)

    # 3) Create DB
    cmd_create = [
        "psql",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", "postgres",
        "-c", f"CREATE DATABASE {PGDATABASE} OWNER {PGUSER};",
    ]
    run_cmd(cmd_create)

    print(f"[ok] Database {PGDATABASE} dropped and recreated.")


def restore_base_dump(base_dump_path: str = BASE_DUMP) -> None:
    """Restore the given dump into the fresh DB."""
    if not os.path.isfile(base_dump_path):
        print(f"the file {base_dump_path} does not exist and can't be restored.", file=sys.stderr)
        sys.exit(1)

    cmd = [
        "pg_restore",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", PGDATABASE,
        "-v",
        base_dump_path,
    ]
    run_cmd(cmd)
    print(f"[ok] Restored base dump from {base_dump_path}")


def ensure_extensions() -> None:
    """Create any extensions. Extend if more are added later."""
    cmd = [
        "psql",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", PGDATABASE,
        "-c", "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
    ]
    run_cmd(cmd)
    print("[ok] Ensured pg_trgm extension exists.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset dev DB: optional backup, drop/create, restore base dump, create extensions."
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip pg_dump backup of current DB before reset.",
    )
    parser.add_argument(
        "--base-dump",
        type=str,
        default=BASE_DUMP,
        help=f"Path to base dump to restore (default: {BASE_DUMP!r})",
    )
    parser.add_argument(
        "--force",
        action='store_true',
        help=f"Overwrite existing dump file if it exists.",
    )
    args = parser.parse_args()

    base_dump_path = args.base_dump

    if not args.no_backup:
        backup_current_db(base_dump_path, args.force)
    else:
        print("[info] Skipping backup step (--no-backup provided)")

    drop_and_create_db()
    restore_base_dump(base_dump_path)
    ensure_extensions()

    print("[done] Reset sequence complete.")


if __name__ == "__main__":
    main()
