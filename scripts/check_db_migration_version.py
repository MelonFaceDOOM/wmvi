from dataclasses import dataclass
from typing import Optional, List
import psycopg2
import os
from dotenv import load_dotenv
import argparse
import sys

load_dotenv()


@dataclass(frozen=True)
class AppliedMigration:
    version: str
    applied_at: str  # keep as string for simple printing
    checksum: str


def db_creds_from_env(prefix: str, db_override: str | None = None) -> str:
    """
    Build a DSN from env vars like DEV_PGHOST / PROD_PGHOST, etc.

    Required:
        {prefix}PGHOST
        {prefix}PGUSER
        {prefix}PGPASSWORD

    Optional:
        {prefix}PGPORT (default '5432')
        {prefix}PGDATABASE (default from env unless db_override provided)
        {prefix}PGSSLMODE (default 'require')
    """
    host = os.environ[f"{prefix}_PGHOST"]
    port = os.environ.get(f"{prefix}_PGPORT", "5432")
    user = os.environ[f"{prefix}_PGUSER"]
    pwd = os.environ[f"{prefix}_PGPASSWORD"]
    db = db_override or os.environ[f"{prefix}_PGDATABASE"]
    ssl = os.environ.get(f"{prefix}_PGSSLMODE", "require")
    return f"host={host} port={port} dbname={db} user={user} password={pwd} sslmode={ssl}"


def list_applied_migrations(
    prefix: str,
    *,
    limit: Optional[int] = None,
    db_override: Optional[str] = None,
) -> List[AppliedMigration]:
    """
    Return migrations applied to the DB selected by env prefix.

    Looks for a table named exactly `schema_migrations` in the first location that matches:
      1) public.schema_migrations
      2) any_schema.schema_migrations (picked by most recently applied_at)
      3) schema_migrations visible on search_path

    Raises RuntimeError if the table cannot be found.
    """
    dsn = db_creds_from_env(prefix, db_override=db_override)
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT to_regclass('public.schema_migrations') IS NOT NULL;
                """
            )
            (has_public,) = cur.fetchone()

            table_ref: Optional[str] = None
            if has_public:
                table_ref = "public.schema_migrations"
            else:
                # 2) Find any schema_migrations table on the DB and pick the one
                # with the greatest MAX(applied_at) (best guess if multiple exist).
                cur.execute(
                    """
                    WITH candidates AS (
                      SELECT n.nspname AS schema_name
                      FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE c.relname = 'schema_migrations'
                        AND c.relkind = 'r'
                    ),
                    scored AS (
                      SELECT
                        schema_name,
                        (
                          SELECT MAX(applied_at)
                          FROM pg_catalog.pg_class c
                          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                          -- dynamic SQL is annoying; instead we build a safe query below
                        ) AS max_applied_at
                      FROM candidates
                    )
                    SELECT schema_name
                    FROM candidates
                    ORDER BY schema_name;
                    """
                )
                schemas = [r[0] for r in cur.fetchall()]

                # If multiple schemas exist, choose the one with latest applied_at
                # by probing each (schemas list is tiny).
                best_schema = None
                best_ts = None
                for sch in schemas:
                    cur.execute(
                        f"SELECT MAX(applied_at) FROM {sch}.schema_migrations;"
                    )
                    (mx,) = cur.fetchone()
                    if mx is not None and (best_ts is None or mx > best_ts):
                        best_ts = mx
                        best_schema = sch

                if best_schema is not None:
                    table_ref = f"{best_schema}.schema_migrations"
                else:
                    # 3) Try unqualified name on search_path
                    cur.execute(
                        "SELECT to_regclass('schema_migrations') IS NOT NULL;")
                    (has_search_path,) = cur.fetchone()
                    if has_search_path:
                        table_ref = "schema_migrations"

            if not table_ref:
                raise RuntimeError(
                    "Could not find schema_migrations table (checked public, all schemas, and search_path)."
                )

            sql = f"""
                SELECT version, applied_at, checksum
                FROM {table_ref}
                ORDER BY applied_at ASC, version ASC
            """
            if limit is not None:
                sql += " LIMIT %s"
                cur.execute(sql, (int(limit),))
            else:
                cur.execute(sql)

            rows = cur.fetchall()

        # stringify applied_at for simple printing / JSON, avoid tz formatting surprises
        return [
            AppliedMigration(version=str(
                v), applied_at=str(ts), checksum=str(cs))
            for (v, ts, cs) in rows
        ]
    finally:
        try:
            conn.close()
        except Exception:
            pass


def print_applied_migrations(prefix: str, *, limit: Optional[int] = None) -> None:
    migs = list_applied_migrations(prefix, limit=limit)
    print(f"[migrations] {prefix}: {len(migs)} applied")
    for m in migs:
        print(f"  - {m.version}  {m.applied_at}  {m.checksum}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print applied DB migrations from schema_migrations for a given env prefix (DEV/PROD/etc)."
    )
    parser.add_argument(
        "prefix",
        help="Env prefix for DB creds (e.g. DEV, PROD). Uses {PREFIX}_PGHOST/_PGUSER/_PGPASSWORD/_PGDATABASE.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of migrations printed (oldest-first).",
    )
    parser.add_argument(
        "--db",
        dest="db_override",
        default=None,
        help="Override database name (otherwise uses {PREFIX}_PGDATABASE).",
    )

    args = parser.parse_args(argv)

    prefix = args.prefix.strip().upper()
    try:
        print_applied_migrations(prefix, limit=args.limit)
    except KeyError as e:
        print(f"Missing environment variable: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
