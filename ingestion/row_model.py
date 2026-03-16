from __future__ import annotations

import json
from dataclasses import MISSING, fields, is_dataclass
from typing import Any, Callable, ClassVar, Optional, TypeVar, get_origin, get_args

from psycopg2.extras import Json, execute_values

T = TypeVar("T", bound="InsertableRow")


class InsertableRow:
    """
    Base class for dataclass row types that can be bulk-inserted.

    Subclasses MUST be dataclasses.

    Required class vars:
      - TABLE: 'schema.table'
      - PK: tuple of column names used for conflict + returning (usually primary key)

    Optional class vars:
      - CONFLICT: explicit '(<cols...>)' text, if not PK-based
      - RETURNING: explicit tuple of returning cols, if not PK-based
      - COERCE: dict[field_name -> callable] to coerce raw dict values to desired python types
    """

    TABLE: ClassVar[str]
    PK: ClassVar[tuple[str, ...]]

    CONFLICT: ClassVar[Optional[str]] = None
    RETURNING: ClassVar[Optional[tuple[str, ...]]] = None

    # New: per-field type coercion rules.
    # Example: {"upvote_ratio": float, "created_at_ts": ensure_utc}
    # Allows subclasses to ensure type converison takes place correctly
    COERCE: ClassVar[dict[str, Callable[[Any], Any]]] = {}

    @classmethod
    def cols(cls) -> tuple[str, ...]:
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

    @classmethod
    def select_cols_sql(cls) -> str:
        """SELECT list matching dataclass field order."""
        return ", ".join(cls.cols())

    @classmethod
    def from_dict(cls: type[T], d: dict[str, Any]) -> T:
        """
        Construct row from a dict.
        - Ignores extra keys not present in dataclass.
        - Uses dataclass defaults for missing keys.
        - Applies COERCE hooks when present.
        """
        if not is_dataclass(cls):
            raise TypeError(f"{cls.__name__} must be a dataclass")

        out: dict[str, Any] = {}
        for f in fields(cls):
            name = f.name
            if name in d:
                v = d[name]
            else:
                if f.default is not MISSING:
                    v = f.default
                elif f.default_factory is not MISSING:  # type: ignore[comparison-overlap]
                    v = f.default_factory()  # type: ignore[misc]
                else:
                    raise KeyError(f"Missing required field {cls.__name__}.{name}")

            co = cls.COERCE.get(name)
            if co is not None:
                v = co(v)

            out[name] = v

        return cls(**out)  # type: ignore[arg-type]

    def as_insert_tuple(self) -> tuple[Any, ...]:
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


def _annotation_is_floatish(ann: Any) -> bool:
    # float
    if ann is float:
        return True
    # Optional[float] / Union[float, None]
    origin = get_origin(ann)
    if origin is None:
        return False
    if origin is list or origin is dict:
        return False
    args = get_args(ann)
    return float in args


def coerce_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return v
    return v
