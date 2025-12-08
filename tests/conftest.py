import os
import psycopg2
import pytest
from psycopg2 import sql

from db.db import init_pool, close_pool, getcursor
from db.migrations_runner import run_migrations

from dotenv import load_dotenv
load_dotenv()

"""
pytest will auto-import these fixtures into other tests
so stuff defined here can be used without an explicit import
"""


REQUIRED_ENV = [
    "TEST_PGHOST", "TEST_PGUSER", "TEST_PGPASSWORD", "TEST_PGPORT", "TEST_PGDATABASE"
]


def _admin_creds(admin_prefix: str = "TEST", admin_db: str = "postgres") -> str:
    """Connect to the admin DB (usually 'postgres') so we can drop/create the test DB."""
    return (
        f"host={os.environ[f'{admin_prefix}_PGHOST']} "
        f"port={os.environ.get(f'{admin_prefix}_PGPORT','5432')} "
        f"dbname={admin_db} user={os.environ[f'{admin_prefix}_PGUSER']} "
        f"password={os.environ[f'{admin_prefix}_PGPASSWORD']} sslmode=require"
    )


def _drop_and_recreate_database(dbname: str) -> None:
    """Force drop the test DB and recreate it fresh."""
    conn = psycopg2.connect(_admin_creds())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)")
                .format(sql.Identifier(dbname))
            )
            cur.execute(
                sql.SQL("CREATE DATABASE {}")
                .format(sql.Identifier(dbname))
            )
    finally:
        conn.close()


@pytest.fixture(scope="session", autouse=True)
def prepared_fresh_db():
    """Fresh test DB for this test session; runs migrations and basic sanity checks."""
    missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        pytest.skip(
            "Set TEST_PGHOST/TEST_PGUSER/TEST_PGPASSWORD/TEST_PGPORT/TEST_PGDATABASE to run DB tests."
        )

    test_db = os.environ["TEST_PGDATABASE"]

    _drop_and_recreate_database(test_db)
    close_pool()  # no-op if pool doesn't exist
    init_pool(prefix="TEST", minconn=1, maxconn=4, force_tunnel=False)

    applied = run_migrations(migrations_dir="db/migrations")
    assert applied, "Expected at least one migration to apply on a fresh DB."

    # Core sanity checks
    with getcursor() as cur:
        cur.execute("SELECT to_regclass('sm.tweet')")
        assert cur.fetchone()[0] == 'sm.tweet'
        cur.execute("SELECT to_regclass('sm.post_registry')")
        assert cur.fetchone()[0] == 'sm.post_registry'
        cur.execute("SELECT to_regclass('scrape.post_scrape')")
        assert cur.fetchone()[0] == 'scrape.post_scrape'
        cur.execute(
            "SELECT 1 FROM pg_views WHERE schemaname='sm' AND viewname='posts_unified'"
        )
        assert cur.fetchone() is not None

    return test_db
