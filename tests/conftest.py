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

def pytest_configure(config):
    if not config.option.markexpr:
        config.option.markexpr = "not transcription"


def pytest_addoption(parser):
    parser.addoption(
        "--with-db",
        action="store_true",
        default=False,
        help="Enable DB-marked tests and run fresh test DB setup.",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--with-db"):
        return
    skip_db = pytest.mark.skip(reason="DB tests disabled; pass --with-db to enable.")
    for item in items:
        if "db" in item.keywords:
            item.add_marker(skip_db)

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
def prepared_fresh_db(request):
    """Fresh test DB for this test session; runs migrations and basic sanity checks."""
    # No-op unless user explicitly enables DB tests.
    if not request.config.getoption("--with-db"):
        return

    missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        pytest.fail(
            "Missing TEST_* DB env vars while --with-db is enabled. "
            "Set TEST_PGHOST/TEST_PGUSER/TEST_PGPASSWORD/TEST_PGPORT/TEST_PGDATABASE."
        )

    test_db = os.environ["TEST_PGDATABASE"]

    _drop_and_recreate_database(test_db)
    close_pool()  # no-op if pool doesn't exist
    init_pool(prefix="TEST", minconn=1, maxconn=4, force_tunnel=False)

    applied = run_migrations(migrations_dir="db/migrations")
    assert applied, "Expected at least one migration to apply on a fresh DB."

    # Core sanity checks
    with getcursor() as cur:
        def _dbg(msg: str, **kv):
            pytest.fail(f"DB sanity check failed: {msg} | {kv}")

        # Tables
        cur.execute("SELECT to_regclass('sm.tweet')")
        got = cur.fetchone()[0]
        if got != "sm.tweet":
            _dbg("missing table", obj="sm.tweet", to_regclass=got)
        assert got == "sm.tweet"

        cur.execute("SELECT to_regclass('sm.post_registry')")
        got = cur.fetchone()[0]
        if got != "sm.post_registry":
            _dbg("missing table", obj="sm.post_registry", to_regclass=got)
        assert got == "sm.post_registry"

        cur.execute("SELECT to_regclass('scrape.post_scrape')")
        got = cur.fetchone()[0]
        if got != "scrape.post_scrape":
            _dbg("missing table", obj="scrape.post_scrape", to_regclass=got)
        assert got == "scrape.post_scrape"

        # View: use to_regclass first (most direct “does something named this exist?”)
        cur.execute("SELECT to_regclass('sm.posts_all')")
        v = cur.fetchone()[0]
        if v is None:
            # dump useful catalog info
            cur.execute("""
                SELECT schemaname, viewname
                FROM pg_views
                WHERE schemaname = 'sm'
                ORDER BY viewname
            """)
            views = cur.fetchall()

            cur.execute("""
                SELECT schemaname, matviewname
                FROM pg_matviews
                WHERE schemaname = 'sm'
                ORDER BY matviewname
            """)
            matviews = cur.fetchall()

            cur.execute("""
                SELECT n.nspname AS schema, c.relname AS name, c.relkind AS kind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'sm'
                  AND c.relname = 'posts_all'
            """)
            rel = cur.fetchall()

            _dbg(
                "missing view sm.posts_all",
                to_regclass=v,
                pg_views_sm=views,
                pg_matviews_sm=matviews,
                pg_class_hit=rel,
            )
        assert v == "sm.posts_all"