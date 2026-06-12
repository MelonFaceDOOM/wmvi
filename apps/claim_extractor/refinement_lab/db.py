"""SQLite persistence for refinement lab (problem posts, profiles, extractions)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from apps.claim_extractor.refinement_lab.models import DEFAULT_MODEL


def default_db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "refinement.sqlite"


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS problem_posts (
            task_id TEXT PRIMARY KEY,
            post_json TEXT NOT NULL,
            baseline_claims_json TEXT,
            baseline_status TEXT NOT NULL,
            comment TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT 'browse',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS reviewed_skips (
            task_id TEXT PRIMARY KEY,
            reviewed_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS prompt_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            system_prompt TEXT NOT NULL DEFAULT '',
            user_prompt TEXT NOT NULL DEFAULT '',
            model TEXT NOT NULL,
            max_claims INTEGER NOT NULL DEFAULT 8,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS profile_extractions (
            profile_id INTEGER NOT NULL REFERENCES prompt_profiles(id) ON DELETE CASCADE,
            task_id TEXT NOT NULL,
            output_json TEXT,
            error TEXT,
            status TEXT NOT NULL CHECK (status IN ('success', 'failed')),
            model TEXT,
            run_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (profile_id, task_id)
        );
        CREATE INDEX IF NOT EXISTS idx_problem_posts_created ON problem_posts(created_at);
        CREATE INDEX IF NOT EXISTS idx_profile_extractions_profile ON profile_extractions(profile_id);
        """
    )
    conn.commit()


@dataclass
class PromptProfile:
    id: int
    name: str
    system_prompt: str
    user_prompt: str
    model: str
    max_claims: int
    created_at: str | None = None


def _row_to_profile(r: sqlite3.Row) -> PromptProfile:
    return PromptProfile(
        id=int(r["id"]),
        name=str(r["name"]),
        system_prompt=str(r["system_prompt"] or ""),
        user_prompt=str(r["user_prompt"] or ""),
        model=str(r["model"]),
        max_claims=int(r["max_claims"]),
        created_at=str(r["created_at"]) if r["created_at"] else None,
    )


def _parse_post_row(r: sqlite3.Row) -> dict[str, Any]:
    baseline_raw = r["baseline_claims_json"]
    baseline_claims: list | None = None
    if baseline_raw is not None and str(baseline_raw).strip():
        try:
            baseline_claims = json.loads(str(baseline_raw))
        except json.JSONDecodeError:
            baseline_claims = None
    return {
        "task_id": str(r["task_id"]),
        "post_row": json.loads(str(r["post_json"])),
        "baseline_claims": baseline_claims,
        "baseline_status": str(r["baseline_status"]),
        "comment": str(r["comment"] or ""),
        "source": str(r["source"]),
        "created_at": str(r["created_at"]) if r["created_at"] else None,
        "updated_at": str(r["updated_at"]) if r["updated_at"] else None,
    }


def count_problem_posts(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM problem_posts").fetchone()[0])


def fetch_problem_posts_sorted(conn: sqlite3.Connection, *, descending: bool = True) -> list[dict[str, Any]]:
    order = "DESC" if descending else "ASC"
    rows = conn.execute(
        f"""
        SELECT task_id, post_json, baseline_claims_json, baseline_status,
               comment, source, created_at, updated_at
        FROM problem_posts
        ORDER BY created_at {order}, task_id
        """
    ).fetchall()
    return [_parse_post_row(r) for r in rows]


def get_problem_post(conn: sqlite3.Connection, task_id: str) -> dict[str, Any] | None:
    r = conn.execute(
        """
        SELECT task_id, post_json, baseline_claims_json, baseline_status,
               comment, source, created_at, updated_at
        FROM problem_posts WHERE task_id = ?
        """,
        (task_id,),
    ).fetchone()
    if r is None:
        return None
    return _parse_post_row(r)


def is_problem_post(conn: sqlite3.Connection, task_id: str) -> bool:
    r = conn.execute("SELECT 1 FROM problem_posts WHERE task_id = ?", (task_id,)).fetchone()
    return r is not None


def upsert_problem_post(
    conn: sqlite3.Connection,
    *,
    task_id: str,
    post_row: dict[str, Any],
    baseline_claims: list | None,
    baseline_status: str,
    comment: str,
    source: str = "browse",
) -> None:
    claims_json = json.dumps(baseline_claims, ensure_ascii=False) if baseline_claims is not None else None
    conn.execute(
        """
        INSERT INTO problem_posts (
            task_id, post_json, baseline_claims_json, baseline_status, comment, source
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(task_id) DO UPDATE SET
            post_json = excluded.post_json,
            baseline_claims_json = excluded.baseline_claims_json,
            baseline_status = excluded.baseline_status,
            comment = excluded.comment,
            updated_at = datetime('now')
        """,
        (
            task_id,
            json.dumps(post_row, ensure_ascii=False),
            claims_json,
            baseline_status,
            comment,
            source,
        ),
    )
    conn.commit()


def insert_problem_post_ignore(
    conn: sqlite3.Connection,
    *,
    task_id: str,
    post_row: dict[str, Any],
    baseline_claims: list | None,
    baseline_status: str,
    comment: str,
    source: str,
) -> bool:
    claims_json = json.dumps(baseline_claims, ensure_ascii=False) if baseline_claims is not None else None
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO problem_posts (
            task_id, post_json, baseline_claims_json, baseline_status, comment, source
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            json.dumps(post_row, ensure_ascii=False),
            claims_json,
            baseline_status,
            comment,
            source,
        ),
    )
    conn.commit()
    return cur.rowcount > 0


def fetch_reviewed_skip_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT task_id FROM reviewed_skips").fetchall()
    return {str(r["task_id"]) for r in rows}


def add_reviewed_skip(conn: sqlite3.Connection, task_id: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO reviewed_skips (task_id) VALUES (?)",
        (task_id,),
    )
    conn.commit()


def count_reviewed_skips(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM reviewed_skips").fetchone()[0])


def get_latest_profile(conn: sqlite3.Connection) -> PromptProfile | None:
    r = conn.execute(
        """
        SELECT id, name, system_prompt, user_prompt, model, max_claims, created_at
        FROM prompt_profiles ORDER BY id DESC LIMIT 1
        """
    ).fetchone()
    if r is None:
        return None
    return _row_to_profile(r)


def list_profiles(conn: sqlite3.Connection) -> list[PromptProfile]:
    rows = conn.execute(
        """
        SELECT id, name, system_prompt, user_prompt, model, max_claims, created_at
        FROM prompt_profiles ORDER BY id DESC
        """
    ).fetchall()
    return [_row_to_profile(r) for r in rows]


def get_profile(conn: sqlite3.Connection, profile_id: int) -> PromptProfile | None:
    r = conn.execute(
        """
        SELECT id, name, system_prompt, user_prompt, model, max_claims, created_at
        FROM prompt_profiles WHERE id = ?
        """,
        (profile_id,),
    ).fetchone()
    if r is None:
        return None
    return _row_to_profile(r)


def create_profile(
    conn: sqlite3.Connection,
    *,
    name: str,
    system_prompt: str = "",
    user_prompt: str = "",
    model: str = DEFAULT_MODEL,
    max_claims: int = 8,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO prompt_profiles (name, system_prompt, user_prompt, model, max_claims)
        VALUES (?, ?, ?, ?, ?)
        """,
        (name.strip(), system_prompt, user_prompt, model, max_claims),
    )
    conn.commit()
    return int(cur.lastrowid)


def create_profile_from_latest(conn: sqlite3.Connection, name: str) -> int:
    latest = get_latest_profile(conn)
    if latest is None:
        return create_profile(conn, name=name)
    return create_profile(
        conn,
        name=name,
        system_prompt=latest.system_prompt,
        user_prompt=latest.user_prompt,
        model=latest.model,
        max_claims=latest.max_claims,
    )


def update_profile(
    conn: sqlite3.Connection,
    profile_id: int,
    *,
    name: str,
    system_prompt: str,
    user_prompt: str,
    model: str,
    max_claims: int,
) -> None:
    conn.execute(
        """
        UPDATE prompt_profiles
        SET name = ?, system_prompt = ?, user_prompt = ?, model = ?, max_claims = ?
        WHERE id = ?
        """,
        (name.strip(), system_prompt, user_prompt, model, max_claims, profile_id),
    )
    conn.commit()


def upsert_profile_extraction(
    conn: sqlite3.Connection,
    *,
    profile_id: int,
    task_id: str,
    status: str,
    output_json: dict[str, Any] | None = None,
    error: str | None = None,
    model: str | None = None,
) -> None:
    out_text = json.dumps(output_json, ensure_ascii=False) if output_json is not None else None
    conn.execute(
        """
        INSERT INTO profile_extractions (profile_id, task_id, output_json, error, status, model)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(profile_id, task_id) DO UPDATE SET
            output_json = excluded.output_json,
            error = excluded.error,
            status = excluded.status,
            model = excluded.model,
            run_at = datetime('now')
        """,
        (profile_id, task_id, out_text, error, status, model),
    )
    conn.commit()


def fetch_extractions_for_profile(
    conn: sqlite3.Connection, profile_id: int
) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT task_id, output_json, error, status, model, run_at
        FROM profile_extractions WHERE profile_id = ?
        """,
        (profile_id,),
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        tid = str(r["task_id"])
        parsed: dict[str, Any] | None = None
        if r["output_json"]:
            try:
                parsed = json.loads(str(r["output_json"]))
            except json.JSONDecodeError:
                parsed = None
        out[tid] = {
            "output_json": parsed,
            "error": str(r["error"]) if r["error"] else None,
            "status": str(r["status"]),
            "model": str(r["model"]) if r["model"] else None,
            "run_at": str(r["run_at"]) if r["run_at"] else None,
        }
    return out


def fetch_extractions_for_task(
    conn: sqlite3.Connection, task_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT e.profile_id, e.output_json, e.error, e.status, e.model, e.run_at,
               p.name AS profile_name
        FROM profile_extractions e
        JOIN prompt_profiles p ON p.id = e.profile_id
        WHERE e.task_id = ?
        ORDER BY e.run_at DESC
        """,
        (task_id,),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for r in rows:
        parsed: dict[str, Any] | None = None
        if r["output_json"]:
            try:
                parsed = json.loads(str(r["output_json"]))
            except json.JSONDecodeError:
                parsed = None
        result.append(
            {
                "profile_id": int(r["profile_id"]),
                "profile_name": str(r["profile_name"]),
                "output_json": parsed,
                "error": str(r["error"]) if r["error"] else None,
                "status": str(r["status"]),
                "model": str(r["model"]) if r["model"] else None,
                "run_at": str(r["run_at"]) if r["run_at"] else None,
            }
        )
    return result
