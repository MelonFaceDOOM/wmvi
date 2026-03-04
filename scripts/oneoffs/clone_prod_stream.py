from __future__ import annotations

import os
import sys
import subprocess

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ----------------------------------------------------------------------
# One-off: clone OLDPROD -> NEWPROD by streaming pg_dump | pg_restore
#
# Required .env keys:
#   OLDPROD_PGHOST, OLDPROD_PGPORT (optional; defaults 5432), OLDPROD_PGUSER,
#   OLDPROD_PGPASSWORD, OLDPROD_PGDATABASE, OLDPROD_PGSSLMODE (optional; defaults require)
#
#   NEWPROD_PGHOST, NEWPROD_PGPORT (optional; defaults 5432), NEWPROD_PGUSER,
#   NEWPROD_PGPASSWORD, NEWPROD_PGDATABASE, NEWPROD_PGSSLMODE (optional; defaults require)
#
# Assumptions:
# - Target server + user exist.
# - This script will hard-reset the target DB (DROP + CREATE), then restore into it.
# ----------------------------------------------------------------------

SOURCE_PREFIX = "OLDPROD"
TARGET_PREFIX = "PROD"
TARGET_MAINT_DB = "postgres"  # maintenance DB on target server


def _env(prefix: str, key: str, default: str | None = None) -> str:
    k = f"{prefix}_{key}"
    v = os.environ.get(k, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {k}")
    return v


def _get_conn(prefix: str, db_override: str | None = None):
    host = _env(prefix, "PGHOST")
    port = _env(prefix, "PGPORT", "5432")
    user = _env(prefix, "PGUSER")
    password = _env(prefix, "PGPASSWORD")
    database = db_override or _env(prefix, "PGDATABASE")
    sslmode = _env(prefix, "PGSSLMODE", "require")

    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database,
        sslmode=sslmode,
    )


def _reset_target_db() -> None:
    target_host = _env(TARGET_PREFIX, "PGHOST")
    target_port = _env(TARGET_PREFIX, "PGPORT", "5432")
    target_user = _env(TARGET_PREFIX, "PGUSER")
    target_db = _env(TARGET_PREFIX, "PGDATABASE")

    # Safety: refuse obvious foot-guns
    src_host = _env(SOURCE_PREFIX, "PGHOST")
    src_port = _env(SOURCE_PREFIX, "PGPORT", "5432")
    src_db = _env(SOURCE_PREFIX, "PGDATABASE")
    if (src_host, src_port, src_db) == (target_host, target_port, target_db):
        raise RuntimeError("Refusing: source and target appear identical (same host/port/db).")

    print(f"[clone] SOURCE: {src_host}:{src_port}/{src_db}")
    print(f"[clone] TARGET: {target_host}:{target_port}/{target_db}")
    print("[clone] Dropping + recreating target DB...", flush=True)

    conn = _get_conn(TARGET_PREFIX, db_override=TARGET_MAINT_DB)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            # terminate active sessions
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (target_db,),
            )

            # drop + create
            cur.execute(f'DROP DATABASE IF EXISTS "{target_db}"')
            cur.execute(f'CREATE DATABASE "{target_db}" OWNER "{target_user}"')
    finally:
        conn.close()


def _stream_dump_restore() -> None:
    # Build env dicts for subprocesses
    src_env = os.environ.copy()
    src_env.update(
        {
            "PGHOST": _env(SOURCE_PREFIX, "PGHOST"),
            "PGPORT": _env(SOURCE_PREFIX, "PGPORT", "5432"),
            "PGUSER": _env(SOURCE_PREFIX, "PGUSER"),
            "PGPASSWORD": _env(SOURCE_PREFIX, "PGPASSWORD"),
            "PGDATABASE": _env(SOURCE_PREFIX, "PGDATABASE"),
            "PGSSLMODE": _env(SOURCE_PREFIX, "PGSSLMODE", "require"),
        }
    )

    tgt_env = os.environ.copy()
    tgt_env.update(
        {
            "PGHOST": _env(TARGET_PREFIX, "PGHOST"),
            "PGPORT": _env(TARGET_PREFIX, "PGPORT", "5432"),
            "PGUSER": _env(TARGET_PREFIX, "PGUSER"),
            "PGPASSWORD": _env(TARGET_PREFIX, "PGPASSWORD"),
            "PGDATABASE": _env(TARGET_PREFIX, "PGDATABASE"),
            "PGSSLMODE": _env(TARGET_PREFIX, "PGSSLMODE", "require"),
        }
    )

    dump_cmd = [
        "pg_dump",
        "-Fc",
        "--no-owner",
        "--no-privileges",
    ]

    restore_cmd = [
        "pg_restore",
        "--no-owner",
        "--no-privileges",
        "--dbname",
        _env(TARGET_PREFIX, "PGDATABASE"),
        "--exit-on-error",
    ]

    print("[clone] Streaming pg_dump → pg_restore ...", flush=True)
    print("+ " + " ".join(dump_cmd), flush=True)
    print("+ " + " ".join(restore_cmd), flush=True)

    dump_p = subprocess.Popen(dump_cmd, env=src_env, stdout=subprocess.PIPE)
    assert dump_p.stdout is not None

    restore_p = subprocess.Popen(restore_cmd, env=tgt_env, stdin=dump_p.stdout)

    # allow SIGPIPE to pg_dump if restore dies early
    dump_p.stdout.close()

    restore_rc = restore_p.wait()
    dump_rc = dump_p.wait()

    if dump_rc != 0:
        raise subprocess.CalledProcessError(dump_rc, dump_cmd)
    if restore_rc != 0:
        raise subprocess.CalledProcessError(restore_rc, restore_cmd)

def _print_table_counts_sanity_check() -> None:
    """
    Sanity check: print how many user tables exist in SOURCE and TARGET.

    - SOURCE must exist; if it doesn't, we error.
    - TARGET may not exist yet. Print message and continue.
    """
    def count_tables(prefix: str) -> int:
        conn = _get_conn(prefix)  # uses <prefix>_PGDATABASE
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND table_type = 'BASE TABLE'
                    """
                )
                (n,) = cur.fetchone()
                return int(n)
        finally:
            conn.close()

    src_host = _env(SOURCE_PREFIX, "PGHOST")
    src_port = _env(SOURCE_PREFIX, "PGPORT", "5432")
    src_db = _env(SOURCE_PREFIX, "PGDATABASE")

    tgt_host = _env(TARGET_PREFIX, "PGHOST")
    tgt_port = _env(TARGET_PREFIX, "PGPORT", "5432")
    tgt_db = _env(TARGET_PREFIX, "PGDATABASE")

    # SOURCE: must exist
    src_n = count_tables(SOURCE_PREFIX)
    print(f"[sanity] SOURCE ({SOURCE_PREFIX}) {src_host}:{src_port}/{src_db}: {src_n} tables")

    # TARGET: may not exist yet
    try:
        tgt_n = count_tables(TARGET_PREFIX)
        print(f"[sanity] TARGET ({TARGET_PREFIX}) {tgt_host}:{tgt_port}/{tgt_db}: {tgt_n} tables")
    except psycopg2.OperationalError as e:
        # Typical when DB doesn't exist (or auth/network).
        msg = str(e).strip().splitlines()[0] if str(e).strip() else "OperationalError"
        print(
            f"[sanity] TARGET ({TARGET_PREFIX}) {tgt_host}:{tgt_port}/{tgt_db}: "
            f"DB not present / not reachable yet (expected). ({msg})"
        )


def main() -> None:
    _print_table_counts_sanity_check()
    # _reset_target_db()
    # _stream_dump_restore()
    # print("[clone] Done.")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"\nERROR: command failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)