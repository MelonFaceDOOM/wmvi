from __future__ import annotations

import os
import sys
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Dict, List, Optional

import psycopg2
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class PgCreds:
    host: str
    port: str
    user: str
    password: str
    database: str
    sslmode: str = "require"


def _get_creds(prefix: str, db_override: Optional[str] = None) -> PgCreds:
    host = os.environ[f"{prefix}_PGHOST"]
    port = os.environ.get(f"{prefix}_PGPORT", "5432")
    user = os.environ[f"{prefix}_PGUSER"]
    password = os.environ[f"{prefix}_PGPASSWORD"]
    database = db_override or os.environ[f"{prefix}_PGDATABASE"]
    sslmode = os.environ.get(f"{prefix}_PGSSLMODE", "require")
    return PgCreds(host=host, port=port, user=user, password=password, database=database, sslmode=sslmode)


def _subproc_env(creds: PgCreds) -> Dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "PGHOST": creds.host,
            "PGPORT": creds.port,
            "PGUSER": creds.user,
            "PGPASSWORD": creds.password,
            "PGDATABASE": creds.database,
            "PGSSLMODE": creds.sslmode,
        }
    )
    return env


def _run(cmd: List[str], env: Dict[str, str]) -> None:
    print(f"+ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, env=env, check=True)


def _drop_and_recreate_db(dev: PgCreds, maint_db: str = "postgres") -> None:
    """
    Drops and recreates the DEV database (hard reset).
    Requires the DEV user to have permission to DROP/CREATE DATABASE.
    """
    # Connect to maintenance DB (not the DB we're dropping)
    conn = psycopg2.connect(
        host=dev.host,
        port=dev.port,
        user=dev.user,
        password=dev.password,
        dbname=maint_db,
        sslmode=dev.sslmode,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Terminate any active connections to the target DB
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (dev.database,),
            )

            # Drop + recreate
            cur.execute(f'DROP DATABASE IF EXISTS "{dev.database}"')
            cur.execute(f'CREATE DATABASE "{dev.database}" OWNER "{dev.user}"')
    finally:
        conn.close()


def clone_prod_to_dev() -> None:
    prod = _get_creds("PROD")
    dev = _get_creds("DEV")

    # Safety: refuse obvious foot-guns
    if dev.host == prod.host and dev.database == prod.database:
        raise RuntimeError(
            "DEV appears to point at PROD (same host+db). Refusing to continue.")

    print("[clone] Dumping PROD, then hard-resetting DEV DB, then restoring dump into DEV.")
    print(f"[clone] PROD: {prod.host}:{prod.port}/{prod.database}")
    print(f"[clone] DEV : {dev.host}:{dev.port}/{dev.database}")

    with tempfile.TemporaryDirectory() as td:
        dump_path = os.path.join(td, "prod.dump")

        # 1) Dump PROD to a temp file (custom format, good for pg_restore)
        _run(
            [
                "pg_dump",
                "-Fc",  # custom format
                "--no-owner",
                "--no-privileges",
                "-f",
                dump_path,
            ],
            env=_subproc_env(prod),
        )

        # 2) Drop + recreate DEV database (guaranteed clean slate)
        print("[clone] Dropping + recreating DEV database...", flush=True)
        _drop_and_recreate_db(dev, maint_db=os.environ.get(
            "DEV_PG_MAINT_DB", "postgres"))

        # 3) Restore dump into DEV
        # Note: DB is empty now, so --clean isn’t necessary (but harmless).
        _run(
            [
                "pg_restore",
                "--no-owner",
                "--no-privileges",
                "--dbname",
                dev.database,
                dump_path,
            ],
            env=_subproc_env(dev),
        )

    print("[clone] Done.")


if __name__ == "__main__":
    try:
        clone_prod_to_dev()
    except subprocess.CalledProcessError as e:
        print(f"\nERROR: command failed with exit code {
              e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
