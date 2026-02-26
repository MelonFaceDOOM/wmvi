from __future__ import annotations
import hashlib
import os
import glob
from typing import Dict, List
from db.db import getcursor


def _sha256_canonical_sql(path: str) -> str:
    h = hashlib.sha256()

    raw = open(path, "rb").read()

    # Decode (strip UTF-8 BOM if present)
    text = raw.decode("utf-8-sig")

    # Normalize newlines: CRLF / CR -> LF
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Hash canonical form
    h.update(text.encode("utf-8"))
    return h.hexdigest()


def ensure_migrations_table() -> None:
    with getcursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version     TEXT PRIMARY KEY,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                checksum    TEXT NOT NULL
            );
        """)


def applied_migrations() -> Dict[str, str]:
    with getcursor() as cur:
        cur.execute("SELECT version, checksum FROM schema_migrations")
        return {v: c for (v, c) in cur.fetchall()}


def read_sql_canonical(path: str) -> str:
    raw = open(path, "rb").read()
    text = raw.decode("utf-8-sig")
    return text.replace("\r\n", "\n").replace("\r", "\n")


def apply_sql_file(path: str) -> None:
    sql_text = read_sql_canonical(path)
    with getcursor(commit=True) as cur:
        cur.execute(sql_text)


def record_migration(version: str, checksum: str) -> None:
    with getcursor() as cur:
        cur.execute(
            "INSERT INTO schema_migrations(version, checksum) VALUES (%s, %s)",
            (version, checksum),
        )


def run_migrations(migrations_dir: str = "db/migrations") -> List[str]:
    """
    Applies pending *.sql files in lexical order using getcursor() transactions.
    Returns a list of versions applied this run.
    Assumes init_pool has already been run
    """
    ensure_migrations_table()
    done = applied_migrations()
    paths = sorted(glob.glob(os.path.join(migrations_dir, "*.sql")))
    applied_now: List[str] = []

    for path in paths:
        version = os.path.basename(path)
        checksum = _sha256_canonical_sql(path)

        if version in done:
            if done[version] != checksum:
                raise RuntimeError(
                    f"Checksum mismatch for migration {version}. "
                    f"Previously: {done[version]} Now: {checksum}"
                )
            continue  # already applied, identical

        apply_sql_file(path)
        record_migration(version, checksum)
        applied_now.append(version)

    return applied_now
