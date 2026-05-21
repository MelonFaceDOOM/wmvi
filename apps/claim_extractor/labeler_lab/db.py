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


@dataclass
class RidgeHead:
    id: int
    name: str
    input_var_keys: list[str]
    artifact_dir: str | None
    encoder_model_id: str | None


def create_head(conn: sqlite3.Connection, name: str, input_var_keys: list[str]) -> int:
    cur = conn.execute(
        "INSERT INTO ridge_heads (name, input_var_keys) VALUES (?, ?)",
        (name.strip(), json.dumps(input_var_keys)),
    )
    conn.commit()
    return int(cur.lastrowid)


def list_heads(conn: sqlite3.Connection) -> list[RidgeHead]:
    rows = conn.execute("SELECT id, name, input_var_keys, artifact_dir, encoder_model_id FROM ridge_heads ORDER BY id").fetchall()
    out: list[RidgeHead] = []
    for r in rows:
        out.append(
            RidgeHead(
                id=int(r["id"]),
                name=str(r["name"]),
                input_var_keys=json.loads(r["input_var_keys"]),
                artifact_dir=r["artifact_dir"],
                encoder_model_id=r["encoder_model_id"],
            )
        )
    return out


def get_head(conn: sqlite3.Connection, head_id: int) -> RidgeHead | None:
    r = conn.execute(
        "SELECT id, name, input_var_keys, artifact_dir, encoder_model_id FROM ridge_heads WHERE id = ?",
        (head_id,),
    ).fetchone()
    if r is None:
        return None
    return RidgeHead(
        id=int(r["id"]),
        name=str(r["name"]),
        input_var_keys=json.loads(r["input_var_keys"]),
        artifact_dir=r["artifact_dir"],
        encoder_model_id=r["encoder_model_id"],
    )


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
