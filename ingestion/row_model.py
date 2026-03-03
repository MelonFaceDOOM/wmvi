from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any, ClassVar, Optional, TypeVar

from psycopg2.extras import Json, execute_values


T = TypeVar("T", bound="InsertableRow")


class InsertableRow:
    """
    Base class for dataclass row types that can be bulk-inserted.

    Subclasses MUST be dataclasses.

    Required class vars:
      - TABLE: 'schema.table'
      - PK: tuple of column names used for conflict + returning (usually primary key)
        (You can override with CONFLICT if you want something else.)

    Optional class vars:
      - CONFLICT: explicit '(<cols...>)' text, if not PK-based
      - RETURNING: explicit tuple of returning cols, if not PK-based
    """

    TABLE: ClassVar[str]
    PK: ClassVar[tuple[str, ...]]  # e.g. ('video_id','comment_id') or ('id',)

    # Optional overrides
    CONFLICT: ClassVar[Optional[str]] = None          # e.g. "(video_id, comment_id)"
    RETURNING: ClassVar[Optional[tuple[str, ...]]] = None

    @classmethod
    def cols(cls) -> tuple[str, ...]:
        # Dataclass field order is definition order
        return tuple(f.name for f in fields(cls))

    @classmethod
    def json_cols(cls) -> set[str]:
        return {f.name for f in fields(cls) if f.metadata.get("json") is True}

    @classmethod
    def conflict_clause(cls) -> str:
        if cls.CONFLICT is not None:
            return cls.CONFLICT
        return "(" + ", ".join(cls.PK) + ")"

    @classmethod
    def returning_cols(cls) -> tuple[str, ...]:
        if cls.RETURNING is not None:
            return cls.RETURNING
        return cls.PK

    @classmethod
    def insert_sql(cls) -> str:
        cols = cls.cols()
        returning = cls.returning_cols()
        return (
            f"INSERT INTO {cls.TABLE} ({', '.join(cols)}) "
            f"VALUES %s "
            f"ON CONFLICT {cls.conflict_clause()} DO NOTHING "
            f"RETURNING {', '.join(returning)}"
        )

    def as_insert_tuple(self) -> tuple[Any, ...]:
        # fast ordered extraction
        return tuple(getattr(self, c) for c in self.cols())

    def as_insert_tuple_with_json(self) -> tuple[Any, ...]:
        jcols = self.json_cols()
        out: list[Any] = []
        for c in self.cols():
            v = getattr(self, c)
            if c in jcols and v is not None:
                out.append(Json(v))
            else:
                out.append(v)
        return tuple(out)


def insert_rows_returning(
    *,
    rows: list[T],
    cur,
    page_size: int = 500,
) -> tuple[int, int, set[tuple[str, ...]]]:
    """
    Canonical bulk insert for InsertableRow dataclasses.
    Generates SQL from the row class, uses execute_values, returns inserted PK tuples.
    """
    if not rows:
        return 0, 0, set()

    row_type = type(rows[0])
    if not is_dataclass(rows[0]) or not issubclass(row_type, InsertableRow):
        raise TypeError("rows must be a list of dataclass instances inheriting InsertableRow")

    # Ensure homogeneous list
    for r in rows:
        if type(r) is not row_type:
            raise TypeError("rows must all be the same row type")

    sql = row_type.insert_sql()
    cols = row_type.cols()

    values = [r.as_insert_tuple_with_json() for r in rows]
    template = "(" + ",".join(["%s"] * len(cols)) + ")"

    execute_values(cur, sql, values, template=template, page_size=page_size)
    returned = cur.fetchall()

    inserted_keys: set[tuple[str, ...]] = set(
        tuple("" if x is None else str(x) for x in row) for row in returned
    )
    inserted = len(returned)
    skipped = len(rows) - inserted
    return inserted, skipped, inserted_keys