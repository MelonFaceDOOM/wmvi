from __future__ import annotations

import os
import atexit
import logging
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_POOL: Optional[ThreadedConnectionPool] = None
_DEFAULT_DB: str = os.environ.get("DEFAULT_DB", "DEV")


def _base_creds(prefix: str = "") -> dict:
    return dict(
        host=os.environ[f"{prefix}_PGHOST"],
        user=os.environ[f"{prefix}_PGUSER"],
        password=os.environ[f"{prefix}_PGPASSWORD"],
        port=int(os.environ.get(f"{prefix}_PGPORT", "5432")),
        database=os.environ.get(f"{prefix}_PGDATABASE", "postgres"),
        sslmode=os.environ.get(f"{prefix}_PGSSLMODE", "require"),
        connect_timeout=int(os.environ.get("PGCONNECT_TIMEOUT", "10")),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def close_pool() -> None:
    global _POOL
    if _POOL:
        logger.info("Closing DB connection pool.")
        _POOL.closeall()
        _POOL = None


def close_tunnel() -> None:
    """
    No-op kept for compatibility with older code paths that may still call it.
    SSH tunneling is no longer used.
    """
    return


def init_pool(
    prefix: str = _DEFAULT_DB,
    minconn: int = 1,
    maxconn: int = 10,
    force_tunnel: bool = False,
    recreate: bool = False,
):
    """
    Initialize (or reinitialize) the global DB pool.

    `force_tunnel` is retained for backward compatibility but is ignored.
    """
    prefix = prefix.upper()
    global _POOL

    if force_tunnel or os.environ.get("USE_SSH_TUNNEL") == "1":
        logger.warning(
            "SSH tunneling is no longer supported by db.db; ignoring tunnel request "
            "(prefix=%s).",
            prefix,
        )

    if _POOL and not recreate:
        logger.info(
            "DB pool already initialized (prefix=%s); reusing existing pool.",
            prefix,
        )
        return _POOL

    if recreate:
        logger.info("Recreating DB pool (prefix=%s).", prefix)
        close_pool()

    creds = _base_creds(prefix)

    logger.info(
        "Initializing DB pool (prefix=%s, db=%s, host=%s, minconn=%d, maxconn=%d).",
        prefix,
        creds["database"],
        creds["host"],
        minconn,
        maxconn,
    )

    _POOL = ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, **creds)
    return _POOL


def getconn():
    assert _POOL is not None, "Pool not initialized"
    return _POOL.getconn()


def putconn(conn) -> None:
    if _POOL is not None and conn is not None:
        _POOL.putconn(conn)


@contextmanager
def getcursor(commit: bool = True, cursor_factory=None):
    """
    Borrow a conn from pool, yield a cursor.
    On success -> commit (if commit=True).
    On error   -> rollback (if conn still open), re-raise.
    Always     -> return conn to pool.
    """
    conn = getconn()
    try:
        try:
            cur = conn.cursor(cursor_factory=cursor_factory)
        except psycopg2.OperationalError:
            logger.warning("Got stale DB connection; retrying with a new one.")
            putconn(conn)
            conn = getconn()
            cur = conn.cursor(cursor_factory=cursor_factory)

        try:
            yield cur
            if commit and not conn.closed:
                conn.commit()
        except Exception:
            if not conn.closed:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass
    finally:
        putconn(conn)


@atexit.register
def _cleanup() -> None:
    close_pool()
    close_tunnel()