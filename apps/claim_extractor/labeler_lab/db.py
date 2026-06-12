"""SQLite persistence for ridge heads and manual labels (isolated from project DB)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def default_db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "lab.sqlite"


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ridge_heads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            input_var_keys TEXT NOT NULL,
            artifact_dir TEXT,
            encoder_model_id TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            head_id INTEGER NOT NULL REFERENCES ridge_heads(id) ON DELETE CASCADE,
            task_id TEXT NOT NULL,
            claim_index INTEGER NOT NULL,
            y REAL NOT NULL,
            split TEXT NOT NULL CHECK (split IN ('train', 'eval')),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(head_id, task_id, claim_index)
        );
        CREATE INDEX IF NOT EXISTS idx_labels_head_split ON labels(head_id, split);
        CREATE TABLE IF NOT EXISTS problem_claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            claim_index INTEGER NOT NULL,
            post_json TEXT NOT NULL,
            claim_json TEXT NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            head_id INTEGER REFERENCES ridge_heads(id) ON DELETE SET NULL,
            flagged_from_head TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(task_id, claim_index)
        );
        CREATE INDEX IF NOT EXISTS idx_problem_claims_created ON problem_claims(created_at);
        """
    )
    conn.commit()
    _migrate_ridge_heads(conn)


def _migrate_ridge_heads(conn: sqlite3.Connection) -> None:
    info = conn.execute("PRAGMA table_info(ridge_heads)").fetchall()
    cols = {row[1] for row in info}
    if "encoder_model_id" not in cols:
        conn.execute("ALTER TABLE ridge_heads ADD COLUMN encoder_model_id TEXT")
        conn.commit()
    info = conn.execute("PRAGMA table_info(ridge_heads)").fetchall()
    cols = {row[1] for row in info}
    if "score_field_name" not in cols:
        conn.execute("ALTER TABLE ridge_heads ADD COLUMN score_field_name TEXT")
        conn.commit()


@dataclass
class RidgeHead:
    id: int
    name: str
    input_var_keys: list[str]
    artifact_dir: str | None
    encoder_model_id: str | None
    score_field_name: str | None = None


def create_head(
    conn: sqlite3.Connection,
    name: str,
    input_var_keys: list[str],
    *,
    score_field_name: str | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO ridge_heads (name, input_var_keys, score_field_name) VALUES (?, ?, ?)",
        (name.strip(), json.dumps(input_var_keys), score_field_name),
    )
    conn.commit()
    return int(cur.lastrowid)


def _row_to_head(r: sqlite3.Row) -> RidgeHead:
    sfn = r["score_field_name"]
    return RidgeHead(
        id=int(r["id"]),
        name=str(r["name"]),
        input_var_keys=json.loads(r["input_var_keys"]),
        artifact_dir=r["artifact_dir"],
        encoder_model_id=r["encoder_model_id"],
        score_field_name=str(sfn) if sfn else None,
    )


def list_heads(conn: sqlite3.Connection) -> list[RidgeHead]:
    rows = conn.execute(
        "SELECT id, name, input_var_keys, artifact_dir, encoder_model_id, score_field_name FROM ridge_heads ORDER BY id"
    ).fetchall()
    return [_row_to_head(r) for r in rows]


def get_head(conn: sqlite3.Connection, head_id: int) -> RidgeHead | None:
    r = conn.execute(
        "SELECT id, name, input_var_keys, artifact_dir, encoder_model_id, score_field_name FROM ridge_heads WHERE id = ?",
        (head_id,),
    ).fetchone()
    if r is None:
        return None
    return _row_to_head(r)


def get_head_by_name(conn: sqlite3.Connection, name: str) -> RidgeHead | None:
    r = conn.execute(
        "SELECT id, name, input_var_keys, artifact_dir, encoder_model_id, score_field_name FROM ridge_heads WHERE name = ?",
        (name.strip(),),
    ).fetchone()
    if r is None:
        return None
    return _row_to_head(r)


def update_head_artifact(conn: sqlite3.Connection, head_id: int, artifact_dir: str | None) -> None:
    conn.execute("UPDATE ridge_heads SET artifact_dir = ? WHERE id = ?", (artifact_dir, head_id))
    conn.commit()


def upsert_label(
    conn: sqlite3.Connection,
    *,
    head_id: int,
    task_id: str,
    claim_index: int,
    y: float,
    split: str,
) -> None:
    conn.execute(
        """
        INSERT INTO labels (head_id, task_id, claim_index, y, split)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(head_id, task_id, claim_index) DO UPDATE SET
            y = excluded.y,
            split = excluded.split,
            created_at = datetime('now')
        """,
        (head_id, task_id, claim_index, y, split),
    )
    conn.commit()


def get_label(conn: sqlite3.Connection, head_id: int, task_id: str, claim_index: int) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT y, split FROM labels WHERE head_id = ? AND task_id = ? AND claim_index = ?",
        (head_id, task_id, claim_index),
    ).fetchone()
    if r is None:
        return None
    return {"y": float(r["y"]), "split": str(r["split"])}


def count_labels(conn: sqlite3.Connection, head_id: int, split: str | None = None) -> int:
    if split is None:
        return int(conn.execute("SELECT COUNT(*) FROM labels WHERE head_id = ?", (head_id,)).fetchone()[0])
    return int(
        conn.execute("SELECT COUNT(*) FROM labels WHERE head_id = ? AND split = ?", (head_id, split)).fetchone()[0]
    )


def fetch_labels_xy(
    conn: sqlite3.Connection, head_id: int, split: str | None
) -> list[tuple[str, int, float]]:
    """Return (task_id, claim_index, y) for training/scoring."""
    if split is None:
        q = "SELECT task_id, claim_index, y FROM labels WHERE head_id = ?"
        params: tuple[Any, ...] = (head_id,)
    else:
        q = "SELECT task_id, claim_index, y FROM labels WHERE head_id = ? AND split = ?"
        params = (head_id, split)
    rows = conn.execute(q, params).fetchall()
    return [(str(r["task_id"]), int(r["claim_index"]), float(r["y"])) for r in rows]


def fetch_labels_sorted(
    conn: sqlite3.Connection,
    head_id: int,
    split: str | None = None,
    *,
    descending: bool = False,
) -> list[dict[str, Any]]:
    """Return label rows for a head, sorted by score (y)."""
    order = "DESC" if descending else "ASC"
    if split is None:
        q = f"""
            SELECT task_id, claim_index, y, split, created_at
            FROM labels WHERE head_id = ?
            ORDER BY y {order}, task_id, claim_index
        """
        params: tuple[Any, ...] = (head_id,)
    else:
        q = f"""
            SELECT task_id, claim_index, y, split, created_at
            FROM labels WHERE head_id = ? AND split = ?
            ORDER BY y {order}, task_id, claim_index
        """
        params = (head_id, split)
    rows = conn.execute(q, params).fetchall()
    return [
        {
            "task_id": str(r["task_id"]),
            "claim_index": int(r["claim_index"]),
            "y": float(r["y"]),
            "split": str(r["split"]),
            "created_at": str(r["created_at"]) if r["created_at"] else None,
        }
        for r in rows
    ]


def delete_label(conn: sqlite3.Connection, head_id: int, task_id: str, claim_index: int) -> bool:
    cur = conn.execute(
        "DELETE FROM labels WHERE head_id = ? AND task_id = ? AND claim_index = ?",
        (head_id, task_id, claim_index),
    )
    conn.commit()
    return cur.rowcount > 0


def _row_to_problem_claim(r: sqlite3.Row, *, parse_json: bool = True) -> dict[str, Any]:
    post_json = str(r["post_json"])
    claim_json = str(r["claim_json"])
    out: dict[str, Any] = {
        "task_id": str(r["task_id"]),
        "claim_index": int(r["claim_index"]),
        "note": str(r["note"] or ""),
        "head_id": int(r["head_id"]) if r["head_id"] is not None else None,
        "flagged_from_head": str(r["flagged_from_head"] or ""),
        "created_at": str(r["created_at"]) if r["created_at"] else None,
        "updated_at": str(r["updated_at"]) if r["updated_at"] else None,
    }
    if parse_json:
        out["post_row"] = json.loads(post_json)
        out["claim_dict"] = json.loads(claim_json)
    else:
        out["post_json"] = post_json
        out["claim_json"] = claim_json
    return out


def upsert_problem_claim(
    conn: sqlite3.Connection,
    *,
    task_id: str,
    claim_index: int,
    post_row: dict[str, Any],
    claim_dict: dict[str, Any],
    note: str = "",
    head_id: int | None = None,
    flagged_from_head: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO problem_claims (
            task_id, claim_index, post_json, claim_json, note, head_id, flagged_from_head
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(task_id, claim_index) DO UPDATE SET
            post_json = excluded.post_json,
            claim_json = excluded.claim_json,
            head_id = excluded.head_id,
            flagged_from_head = excluded.flagged_from_head,
            updated_at = datetime('now'),
            note = CASE
                WHEN excluded.note != '' THEN excluded.note
                ELSE problem_claims.note
            END
        """,
        (
            task_id,
            claim_index,
            json.dumps(post_row, ensure_ascii=False),
            json.dumps(claim_dict, ensure_ascii=False),
            note,
            head_id,
            flagged_from_head,
        ),
    )
    conn.commit()


def is_problem_claim(conn: sqlite3.Connection, task_id: str, claim_index: int) -> bool:
    r = conn.execute(
        "SELECT 1 FROM problem_claims WHERE task_id = ? AND claim_index = ?",
        (task_id, claim_index),
    ).fetchone()
    return r is not None


def count_problem_claims(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM problem_claims").fetchone()[0])


def fetch_problem_claims_sorted(
    conn: sqlite3.Connection,
    *,
    descending: bool = True,
) -> list[dict[str, Any]]:
    order = "DESC" if descending else "ASC"
    rows = conn.execute(
        f"""
        SELECT task_id, claim_index, post_json, claim_json, note, head_id,
               flagged_from_head, created_at, updated_at
        FROM problem_claims
        ORDER BY created_at {order}, task_id, claim_index
        """
    ).fetchall()
    return [_row_to_problem_claim(r) for r in rows]


def update_problem_claim_note(
    conn: sqlite3.Connection,
    *,
    task_id: str,
    claim_index: int,
    note: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE problem_claims
        SET note = ?, updated_at = datetime('now')
        WHERE task_id = ? AND claim_index = ?
        """,
        (note, task_id, claim_index),
    )
    conn.commit()
    return cur.rowcount > 0


def delete_problem_claim(conn: sqlite3.Connection, task_id: str, claim_index: int) -> bool:
    cur = conn.execute(
        "DELETE FROM problem_claims WHERE task_id = ? AND claim_index = ?",
        (task_id, claim_index),
    )
    conn.commit()
    return cur.rowcount > 0
