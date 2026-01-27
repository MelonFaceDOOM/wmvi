from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# --- env ---
PGHOST = os.getenv("DEV_PGHOST")
PGDATABASE = os.getenv("DEV_PGDATABASE")
PGUSER = os.getenv("DEV_PGUSER")
PGPASSWORD = os.getenv("DEV_PGPASSWORD")

for name, value in [
    ("DEV_PGHOST", PGHOST),
    ("DEV_PGDATABASE", PGDATABASE),
    ("DEV_PGUSER", PGUSER),
]:
    if not value:
        print(f"[error] Environment variable {
              name} is not set.", file=sys.stderr)
        sys.exit(1)


def run_cmd(cmd: list[str]) -> None:
    print("+", " ".join(cmd))
    env = os.environ.copy()
    if PGPASSWORD:
        env["PGPASSWORD"] = PGPASSWORD
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Drop DB and restore from snapshot.")
    ap.add_argument(
        "snapshot",
        nargs="?",
        help="Snapshot file (default: latest in db/snapshots)",
    )
    args = ap.parse_args()

    snapshots_dir = Path("db/snapshots")

    if args.snapshot:
        snapshot_path = Path(args.snapshot)
    else:
        dumps = sorted(snapshots_dir.glob("*.dump"))
        if not dumps:
            print("[error] No snapshots found.", file=sys.stderr)
            sys.exit(1)
        snapshot_path = dumps[-1]

    if not snapshot_path.exists():
        print(f"[error] Snapshot not found: {snapshot_path}", file=sys.stderr)
        sys.exit(1)

    print(f"[info] Restoring snapshot {snapshot_path} into {PGDATABASE}")

    # terminate connections
    run_cmd([
        "psql",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", "postgres",
        "-c", f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{PGDATABASE}'
              AND pid <> pg_backend_pid();
        """,
    ])

    # drop db
    run_cmd([
        "psql",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", "postgres",
        "-c", f"DROP DATABASE IF EXISTS {PGDATABASE};",
    ])

    # create db
    run_cmd([
        "psql",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", "postgres",
        "-c", f"CREATE DATABASE {PGDATABASE} OWNER {PGUSER};",
    ])

    # restore
    run_cmd([
        "pg_restore",
        "-h", PGHOST,
        "-U", PGUSER,
        "-d", PGDATABASE,
        "-v",
        str(snapshot_path),
    ])

    print("[ok] Restore complete.")


if __name__ == "__main__":
    main()
