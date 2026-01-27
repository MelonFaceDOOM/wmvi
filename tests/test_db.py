import uuid
import pytest
import psycopg2
from psycopg2 import sql
from db.db import init_pool, getcursor


@pytest.fixture
def temp_schema(prepared_fresh_db):
    schema = f"t_{uuid.uuid4().hex[:8]}"
    with getcursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield schema
    finally:
        with getcursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def test_basic_roundtrip(temp_schema):
    table = "items"
    with getcursor() as cur:
        cur.execute(
            sql.SQL("CREATE TABLE {}.{} (id serial primary key, name text)")
            .format(sql.Identifier(temp_schema), sql.Identifier(table))
        )
        cur.execute(
            sql.SQL("INSERT INTO {}.{} (name) VALUES (%s),(%s)")
            .format(sql.Identifier(temp_schema), sql.Identifier(table)),
            ("a", "b"),
        )
    with getcursor() as cur:
        cur.execute(
            sql.SQL("SELECT count(*) FROM {}.{}")
            .format(sql.Identifier(temp_schema), sql.Identifier(table))
        )
        (n,) = cur.fetchone()
    assert n == 2

def test_commit_and_rollback(temp_schema):
    table = "events"
    with getcursor() as cur:
        cur.execute(
            sql.SQL("CREATE TABLE {}.{} (k int primary key, v text)")
            .format(sql.Identifier(temp_schema), sql.Identifier(table))
        )

    # Commit path
    with getcursor() as cur:
        cur.execute(
            sql.SQL("INSERT INTO {}.{} (k,v) VALUES (1,'ok')")
            .format(sql.Identifier(temp_schema), sql.Identifier(table))
        )

    # Rollback path: trigger a UNIQUE violation to force an exception and rollback
    with pytest.raises(psycopg2.Error):
        with getcursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO {}.{} (k,v) VALUES (1,'dupe')")
                .format(sql.Identifier(temp_schema), sql.Identifier(table))
            )

    # Verify only the committed row exists
    with getcursor() as cur:
        cur.execute(
            sql.SQL("SELECT k,v FROM {}.{} ORDER BY k")
            .format(sql.Identifier(temp_schema), sql.Identifier(table))
        )
        rows = cur.fetchall()
    assert rows == [(1, "ok")]

def test_init_pool_idempotent(prepared_fresh_db):
    # Calls a 2nd init pool, which shouldn't inhibit functionality
    init_pool(prefix="TEST", minconn=1, maxconn=4, force_tunnel=False)
    with getcursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
