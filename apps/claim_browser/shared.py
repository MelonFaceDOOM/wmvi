from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Tuple

import streamlit as st
import pandas as pd

from db.db import init_pool, getcursor


@dataclass(frozen=True)
class QueryResult:
    ok: bool
    value: Any = None
    error: Optional[str] = None


@st.cache_resource(show_spinner=False)
def ensure_db_pool(prefix: str) -> None:
    init_pool(prefix=prefix)


def _split_qualified_name(qname: str) -> Tuple[str, str]:
    s = (qname or "").strip()
    if "." not in s:
        raise ValueError(
            f"Expected schema-qualified name like 'sm.post_registry', got: {qname!r}")
    schema, name = s.split(".", 1)
    schema = schema.strip()
    name = name.strip()
    if not schema or not name:
        raise ValueError(f"Invalid qualified name: {qname!r}")
    return schema, name


def query_scalar(sql: str, params: tuple[Any, ...] = ()) -> QueryResult:
    try:
        with getcursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        if not row:
            return QueryResult(ok=True, value=None)
        value = row[0]
        try:
            value = int(value)
            if value < 0:
                value = 0
        except Exception:
            pass
        return QueryResult(ok=True, value=value)
    except Exception as e:
        return QueryResult(ok=False, error=f"{type(e).__name__}: {e}")


def query_exists(sql: str, params: tuple[Any, ...] = ()) -> QueryResult:
    try:
        with getcursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        return QueryResult(ok=True, value=bool(row))
    except Exception as e:
        return QueryResult(ok=False, error=f"{type(e).__name__}: {e}")


def query_table_count(qualified_table: str, *, exact: bool = False) -> QueryResult:
    """
    exact=False: fast estimate using pg_class.reltuples (can be stale but cheap).
    exact=True : SELECT COUNT(*) (can be slow on big tables).
    """
    schema, name = _split_qualified_name(qualified_table)

    if exact:
        return query_scalar(f"SELECT COUNT(*) FROM {schema}.{name};")

    # Estimated row count (cheap). Note: reltuples is updated by ANALYZE/VACUUM.
    return query_scalar(
        """
        SELECT COALESCE(c.reltuples, 0)::bigint
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s;
        """,
        (schema, name),
    )


def query_df(sql: str, params: tuple[Any, ...] = ()) -> QueryResult:
    """
    Run a query and return a pandas DataFrame in QueryResult.value.
    Intended for low/medium-cost dashboard queries.
    """
    try:
        with getcursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall() if cols else []
        df = pd.DataFrame(rows, columns=cols)
        return QueryResult(ok=True, value=df)
    except Exception as e:
        return QueryResult(ok=False, error=f"{type(e).__name__}: {e}")
