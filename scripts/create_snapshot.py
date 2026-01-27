from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
from db.db import init_pool, close_pool, getcursor

load_dotenv()


def die(msg: str) -> None:
    print(f"[error] {msg}", file=sys.stderr)
    sys.exit(1)


def run_cmd(cmd: list[str], password: str | None) -> None:
    print("+", " ".join(cmd))
    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        sys.exit(result.returncode)


def get_latest_migration_version(env: str) -> str:
    init_pool(env)
    try:
        with getcursor() as cur:
            cur.execute(
                """
                SELECT version
                FROM public.schema_migrations
                ORDER BY applied_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("No migrations found in schema_migrations")
            return row[0]
    finally:
        close_pool()


def load_pg_env(prefix: str) -> tuple[str, str, str, str | None]:
    host = os.getenv(f"{prefix}_PGHOST")
    db = os.getenv(f"{prefix}_PGDATABASE")
    user = os.getenv(f"{prefix}_PGUSER")
    password = os.getenv(f"{prefix}_PGPASSWORD")

    for name, value in [
        (f"{prefix}_PGHOST", host),
        (f"{prefix}_PGDATABASE", db),
        (f"{prefix}_PGUSER", user),
    ]:
        if not value:
            die(f"Environment variable {name} is not set")

    return host, db, user, password


def main() -> None:
    ap = argparse.ArgumentParser(description="Create a full DB snapshot.")
    ap.add_argument("--prod", action="store_true", help="Use PROD_* env vars")
    ap.add_argument(
        "--file",
        help="Snapshot file path (default: db/snapshots/base_after_<version>[_prod].dump)",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing snapshot if it exists",
    )
    args = ap.parse_args()

    prefix = "PROD" if args.prod else "DEV"
    suffix = "_prod" if args.prod else ""

    PGHOST, PGDATABASE, PGUSER, PGPASSWORD = load_pg_env(prefix)

    snapshots_dir = Path("db/snapshots")
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    if args.file:
        snapshot_path = Path(args.file)
    else:
        env = "prod" if args.prod else "dev"
        version = get_latest_migration_version(env)
        snapshot_path = snapshots_dir / f"base_after_{version}{suffix}.dump"

    if snapshot_path.exists() and not args.force:
        resp = input(
            f"{snapshot_path} exists. Overwrite? (y/n): ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return

    cmd = [
        "pg_dump",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", PGDATABASE,
        "-Fc",
        "-f", str(snapshot_path),
    ]
    run_cmd(cmd, PGPASSWORD)

    if not snapshot_path.exists():
        die(f"Snapshot {snapshot_path} was not created")

    print(f"[ok] Snapshot written to {snapshot_path}")


if __name__ == "__main__":
    main()
