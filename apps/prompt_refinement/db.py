"""SQLite persistence for refinement lab (problem posts, profiles, extractions)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from apps.prompt_refinement.models import DEFAULT_MODEL

BASELINE_PROFILE_NAME = "Baseline"

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
            run_label TEXT NOT NULL DEFAULT '1',
            output_json TEXT,
            error TEXT,
            status TEXT NOT NULL CHECK (status IN ('success', 'failed')),
            model TEXT,
            run_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (profile_id, task_id, run_label)
        );
        CREATE INDEX IF NOT EXISTS idx_problem_posts_created ON problem_posts(created_at);
        CREATE INDEX IF NOT EXISTS idx_profile_extractions_profile ON profile_extractions(profile_id);
        CREATE TABLE IF NOT EXISTS reference_claims (
            task_id TEXT PRIMARY KEY,
            claims_json TEXT NOT NULL,
            source TEXT NOT NULL CHECK (source IN ('manual', 'generated')),
            generated_from_profile_id INTEGER,
            generated_model TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS evaluations (
            profile_id INTEGER NOT NULL REFERENCES prompt_profiles(id) ON DELETE CASCADE,
            task_id TEXT NOT NULL,
            alignment_json TEXT,
            precision REAL,
            recall REAL,
            f1 REAL,
            judged_model TEXT,
            run_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (profile_id, task_id)
        );
        CREATE TABLE IF NOT EXISTS optimization_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            input_profile_id INTEGER NOT NULL REFERENCES prompt_profiles(id),
            status TEXT NOT NULL DEFAULT 'running',
            config_json TEXT NOT NULL DEFAULT '{}',
            summary_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS optimization_iterations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL REFERENCES optimization_runs(id) ON DELETE CASCADE,
            iter_index INTEGER NOT NULL,
            profile_id INTEGER REFERENCES prompt_profiles(id),
            metrics_json TEXT,
            diagnosis_json TEXT,
            proposed_changes_json TEXT,
            accepted INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS profile_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL REFERENCES prompt_profiles(id) ON DELETE CASCADE,
            run_id INTEGER REFERENCES optimization_runs(id) ON DELETE SET NULL,
            kind TEXT NOT NULL CHECK (kind IN ('problems', 'solutions', 'evaluation')),
            content TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS meta_prompts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            template TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_evaluations_profile ON evaluations(profile_id);
        CREATE INDEX IF NOT EXISTS idx_optimization_iterations_run ON optimization_iterations(run_id);
        CREATE INDEX IF NOT EXISTS idx_profile_notes_profile ON profile_notes(profile_id);
        """
    )
    conn.commit()
    _migrate_profile_extractions_run_label(conn)


def _migrate_profile_extractions_run_label(conn: sqlite3.Connection) -> None:
    """Add run_label column / rebuild PK for DBs created before multi-run support."""
    info = conn.execute("PRAGMA table_info(profile_extractions)").fetchall()
    if not info:
        return
    cols = {str(r[1]) for r in info}
    if "run_label" in cols:
        return
    conn.executescript(
        """
        ALTER TABLE profile_extractions RENAME TO profile_extractions_pre_run_label;
        CREATE TABLE profile_extractions (
            profile_id INTEGER NOT NULL REFERENCES prompt_profiles(id) ON DELETE CASCADE,
            task_id TEXT NOT NULL,
            run_label TEXT NOT NULL DEFAULT '1',
            output_json TEXT,
            error TEXT,
            status TEXT NOT NULL CHECK (status IN ('success', 'failed')),
            model TEXT,
            run_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (profile_id, task_id, run_label)
        );
        INSERT INTO profile_extractions (
            profile_id, task_id, run_label, output_json, error, status, model, run_at
        )
        SELECT profile_id, task_id, '1', output_json, error, status, model, run_at
        FROM profile_extractions_pre_run_label;
        DROP TABLE profile_extractions_pre_run_label;
        CREATE INDEX IF NOT EXISTS idx_profile_extractions_profile
            ON profile_extractions(profile_id);
        """
    )
    conn.commit()


def init_lab(conn: sqlite3.Connection) -> None:
    """Schema + baseline profile, extraction sync, default meta-prompts."""
    init_schema(conn)
    ensure_baseline_profile(conn)
    sync_baseline_extractions(conn)
    from apps.prompt_refinement.meta_defaults import seed_default_meta_prompts

    seed_default_meta_prompts(conn)

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

def delete_problem_post(conn: sqlite3.Connection, task_id: str) -> bool:
    cur = conn.execute("DELETE FROM problem_posts WHERE task_id = ?", (task_id,))
    conn.commit()
    return cur.rowcount > 0

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
    run_label: str = "1",
) -> None:
    label = str(run_label or "1").strip() or "1"
    out_text = json.dumps(output_json, ensure_ascii=False) if output_json is not None else None
    conn.execute(
        """
        INSERT INTO profile_extractions (
            profile_id, task_id, run_label, output_json, error, status, model
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(profile_id, task_id, run_label) DO UPDATE SET
            output_json = excluded.output_json,
            error = excluded.error,
            status = excluded.status,
            model = excluded.model,
            run_at = datetime('now')
        """,
        (profile_id, task_id, label, out_text, error, status, model),
    )
    conn.commit()


def list_run_labels_for_profile(conn: sqlite3.Connection, profile_id: int) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT run_label FROM profile_extractions
        WHERE profile_id = ?
        ORDER BY run_label
        """,
        (profile_id,),
    ).fetchall()
    return [str(r["run_label"]) for r in rows]


def next_run_label(conn: sqlite3.Connection, profile_id: int) -> str:
    """Return the next unused integer label as a string ('1', '2', …)."""
    existing = set(list_run_labels_for_profile(conn, profile_id))
    n = 1
    while str(n) in existing:
        n += 1
    return str(n)


def list_extraction_snapshots(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Distinct (profile, run_label) pairs that have at least one extraction."""
    rows = conn.execute(
        """
        SELECT e.profile_id, p.name AS profile_name, e.run_label,
               COUNT(*) AS n, MAX(e.run_at) AS last_run_at
        FROM profile_extractions e
        JOIN prompt_profiles p ON p.id = e.profile_id
        GROUP BY e.profile_id, e.run_label
        ORDER BY p.id, e.run_label
        """
    ).fetchall()
    return [
        {
            "profile_id": int(r["profile_id"]),
            "profile_name": str(r["profile_name"]),
            "run_label": str(r["run_label"]),
            "n": int(r["n"]),
            "last_run_at": str(r["last_run_at"]) if r["last_run_at"] else None,
            "key": f"{r['profile_name']} / {r['run_label']}",
        }
        for r in rows
    ]


def fetch_extractions_for_profile(
    conn: sqlite3.Connection,
    profile_id: int,
    *,
    run_label: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Map task_id → extraction. If run_label is None, prefer latest run_at per task."""
    if run_label is not None:
        rows = conn.execute(
            """
            SELECT task_id, run_label, output_json, error, status, model, run_at
            FROM profile_extractions
            WHERE profile_id = ? AND run_label = ?
            """,
            (profile_id, str(run_label)),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT task_id, run_label, output_json, error, status, model, run_at
            FROM profile_extractions
            WHERE profile_id = ?
            ORDER BY run_at DESC
            """,
            (profile_id,),
        ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        tid = str(r["task_id"])
        if tid in out and run_label is None:
            continue  # already have latest
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
            "run_label": str(r["run_label"]),
        }
    return out


def fetch_extractions_for_task(
    conn: sqlite3.Connection, task_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT e.profile_id, e.run_label, e.output_json, e.error, e.status, e.model, e.run_at,
               p.name AS profile_name
        FROM profile_extractions e
        JOIN prompt_profiles p ON p.id = e.profile_id
        WHERE e.task_id = ?
        ORDER BY e.profile_id, e.run_label
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
                "run_label": str(r["run_label"]),
                "output_json": parsed,
                "error": str(r["error"]) if r["error"] else None,
                "status": str(r["status"]),
                "model": str(r["model"]) if r["model"] else None,
                "run_at": str(r["run_at"]) if r["run_at"] else None,
                "key": f"{r['profile_name']} / {r['run_label']}",
            }
        )
    return result


def get_extraction(
    conn: sqlite3.Connection,
    *,
    profile_id: int,
    task_id: str,
    run_label: str = "1",
) -> dict[str, Any] | None:
    r = conn.execute(
        """
        SELECT output_json, error, status, model, run_at, run_label
        FROM profile_extractions
        WHERE profile_id = ? AND task_id = ? AND run_label = ?
        """,
        (profile_id, task_id, str(run_label)),
    ).fetchone()
    if r is None:
        return None
    parsed: dict[str, Any] | None = None
    if r["output_json"]:
        try:
            parsed = json.loads(str(r["output_json"]))
        except json.JSONDecodeError:
            parsed = None
    return {
        "output_json": parsed,
        "error": str(r["error"]) if r["error"] else None,
        "status": str(r["status"]),
        "model": str(r["model"]) if r["model"] else None,
        "run_at": str(r["run_at"]) if r["run_at"] else None,
        "run_label": str(r["run_label"]),
    }

def _baseline_prompt_texts() -> tuple[str, str]:
    from nlp.claim_extraction.prompts import load_system_template, load_user_template
    from apps.prompt_refinement.eval_sample import canonical_to_lab_placeholders

    return canonical_to_lab_placeholders(load_system_template(), load_user_template())


def load_prompts_from_files(
    *,
    system_path: Path,
    user_path: Path,
) -> tuple[str, str]:
    """Load canonical prompt files and convert placeholders for the lab."""
    from apps.prompt_refinement.eval_sample import canonical_to_lab_placeholders

    system = Path(system_path).read_text(encoding="utf-8-sig")
    user = Path(user_path).read_text(encoding="utf-8-sig")
    return canonical_to_lab_placeholders(system, user)


def duplicate_profile(
    conn: sqlite3.Connection,
    profile_id: int,
    *,
    new_name: str,
) -> int:
    """Copy prompts/settings into a new profile; returns new id."""
    src = get_profile(conn, profile_id)
    if src is None:
        raise KeyError(f"profile id={profile_id} not found")
    return create_profile(
        conn,
        name=new_name,
        system_prompt=src.system_prompt,
        user_prompt=src.user_prompt,
        model=src.model,
        max_claims=src.max_claims,
    )


def get_profile_by_name(conn: sqlite3.Connection, name: str) -> PromptProfile | None:
    r = conn.execute(
        """
        SELECT id, name, system_prompt, user_prompt, model, max_claims, created_at
        FROM prompt_profiles WHERE name = ?
        """,
        (name,),
    ).fetchone()
    if r is None:
        return None
    return _row_to_profile(r)

def ensure_baseline_profile(conn: sqlite3.Connection) -> int:
    existing = get_profile_by_name(conn, BASELINE_PROFILE_NAME)
    if existing is not None:
        return existing.id
    system, user = _baseline_prompt_texts()
    return create_profile(
        conn,
        name=BASELINE_PROFILE_NAME,
        system_prompt=system,
        user_prompt=user,
        model=DEFAULT_MODEL,
        max_claims=8,
    )

def sync_baseline_extractions(conn: sqlite3.Connection) -> int:
    """Copy problem_posts.baseline_claims_json into Baseline profile_extractions."""
    baseline_id = ensure_baseline_profile(conn)
    rows = conn.execute(
        "SELECT task_id, baseline_claims_json FROM problem_posts WHERE baseline_claims_json IS NOT NULL"
    ).fetchall()
    synced = 0
    for r in rows:
        tid = str(r["task_id"])
        try:
            claims = json.loads(str(r["baseline_claims_json"]))
        except json.JSONDecodeError:
            continue
        if not isinstance(claims, list):
            continue
        upsert_profile_extraction(
            conn,
            profile_id=baseline_id,
            task_id=tid,
            status="success",
            output_json={"claims": claims},
            error=None,
            model=DEFAULT_MODEL,
            run_label="1",
        )
        synced += 1
    return synced

# --- Reference claims ---

@dataclass
class ReferenceClaims:
    task_id: str
    claims: list[dict[str, Any]]
    source: str
    generated_from_profile_id: int | None
    generated_model: str | None
    updated_at: str | None

def _parse_reference_row(r: sqlite3.Row) -> ReferenceClaims:
    claims: list[dict[str, Any]] = []
    try:
        parsed = json.loads(str(r["claims_json"]))
        if isinstance(parsed, list):
            claims = parsed
    except json.JSONDecodeError:
        pass
    return ReferenceClaims(
        task_id=str(r["task_id"]),
        claims=claims,
        source=str(r["source"]),
        generated_from_profile_id=int(r["generated_from_profile_id"]) if r["generated_from_profile_id"] else None,
        generated_model=str(r["generated_model"]) if r["generated_model"] else None,
        updated_at=str(r["updated_at"]) if r["updated_at"] else None,
    )

def get_reference_claims(conn: sqlite3.Connection, task_id: str) -> ReferenceClaims | None:
    r = conn.execute("SELECT * FROM reference_claims WHERE task_id = ?", (task_id,)).fetchone()
    if r is None:
        return None
    return _parse_reference_row(r)

def fetch_all_reference_claims(conn: sqlite3.Connection) -> dict[str, ReferenceClaims]:
    rows = conn.execute("SELECT * FROM reference_claims").fetchall()
    return {str(r["task_id"]): _parse_reference_row(r) for r in rows}

def upsert_reference_claims(
    conn: sqlite3.Connection,
    *,
    task_id: str,
    claims: list[dict[str, Any]],
    source: str,
    generated_from_profile_id: int | None = None,
    generated_model: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO reference_claims (
            task_id, claims_json, source, generated_from_profile_id, generated_model
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(task_id) DO UPDATE SET
            claims_json = excluded.claims_json,
            source = excluded.source,
            generated_from_profile_id = excluded.generated_from_profile_id,
            generated_model = excluded.generated_model,
            updated_at = datetime('now')
        """,
        (
            task_id,
            json.dumps(claims, ensure_ascii=False),
            source,
            generated_from_profile_id,
            generated_model,
        ),
    )
    conn.commit()

def set_reference_claims_list(conn: sqlite3.Connection, task_id: str, claims: list[dict[str, Any]]) -> None:
    ref = get_reference_claims(conn, task_id)
    source = ref.source if ref else "manual"
    upsert_reference_claims(
        conn,
        task_id=task_id,
        claims=claims,
        source=source,
        generated_from_profile_id=ref.generated_from_profile_id if ref else None,
        generated_model=ref.generated_model if ref else None,
    )

def add_reference_claim(conn: sqlite3.Connection, task_id: str, claim_text: str) -> None:
    ref = get_reference_claims(conn, task_id)
    claims = list(ref.claims) if ref else []
    claims.append({"claim": claim_text.strip()})
    upsert_reference_claims(conn, task_id=task_id, claims=claims, source="manual")

def edit_reference_claim(conn: sqlite3.Connection, task_id: str, index: int, claim_text: str) -> None:
    ref = get_reference_claims(conn, task_id)
    if ref is None or index < 0 or index >= len(ref.claims):
        raise IndexError("claim index out of range")
    claims = list(ref.claims)
    claims[index] = {"claim": claim_text.strip()}
    set_reference_claims_list(conn, task_id, claims)

def delete_reference_claim(conn: sqlite3.Connection, task_id: str, index: int) -> None:
    ref = get_reference_claims(conn, task_id)
    if ref is None or index < 0 or index >= len(ref.claims):
        raise IndexError("claim index out of range")
    claims = [c for i, c in enumerate(ref.claims) if i != index]
    set_reference_claims_list(conn, task_id, claims)

# --- Evaluations ---

def upsert_evaluation(
    conn: sqlite3.Connection,
    *,
    profile_id: int,
    task_id: str,
    alignment: dict[str, Any],
    precision: float,
    recall: float,
    f1: float,
    judged_model: str,
) -> None:
    conn.execute(
        """
        INSERT INTO evaluations (
            profile_id, task_id, alignment_json, precision, recall, f1, judged_model
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(profile_id, task_id) DO UPDATE SET
            alignment_json = excluded.alignment_json,
            precision = excluded.precision,
            recall = excluded.recall,
            f1 = excluded.f1,
            judged_model = excluded.judged_model,
            run_at = datetime('now')
        """,
        (
            profile_id,
            task_id,
            json.dumps(alignment, ensure_ascii=False),
            precision,
            recall,
            f1,
            judged_model,
        ),
    )
    conn.commit()

def fetch_evaluations_for_profile(conn: sqlite3.Connection, profile_id: int) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM evaluations WHERE profile_id = ?",
        (profile_id,),
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        tid = str(r["task_id"])
        alignment = None
        if r["alignment_json"]:
            try:
                alignment = json.loads(str(r["alignment_json"]))
            except json.JSONDecodeError:
                alignment = None
        out[tid] = {
            "alignment": alignment,
            "precision": float(r["precision"]) if r["precision"] is not None else None,
            "recall": float(r["recall"]) if r["recall"] is not None else None,
            "f1": float(r["f1"]) if r["f1"] is not None else None,
            "judged_model": str(r["judged_model"]) if r["judged_model"] else None,
            "run_at": str(r["run_at"]) if r["run_at"] else None,
        }
    return out

def get_evaluation(conn: sqlite3.Connection, profile_id: int, task_id: str) -> dict[str, Any] | None:
    return fetch_evaluations_for_profile(conn, profile_id).get(task_id)

# --- Profile notes ---

def add_profile_note(
    conn: sqlite3.Connection,
    *,
    profile_id: int,
    kind: str,
    content: str,
    run_id: int | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO profile_notes (profile_id, run_id, kind, content)
        VALUES (?, ?, ?, ?)
        """,
        (profile_id, run_id, kind, content),
    )
    conn.commit()
    return int(cur.lastrowid)

def fetch_profile_notes(conn: sqlite3.Connection, profile_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, profile_id, run_id, kind, content, created_at
        FROM profile_notes WHERE profile_id = ? ORDER BY created_at DESC
        """,
        (profile_id,),
    ).fetchall()
    return [
        {
            "id": int(r["id"]),
            "profile_id": int(r["profile_id"]),
            "run_id": int(r["run_id"]) if r["run_id"] else None,
            "kind": str(r["kind"]),
            "content": str(r["content"]),
            "created_at": str(r["created_at"]),
        }
        for r in rows
    ]

# --- Meta prompts ---

def get_meta_prompt(conn: sqlite3.Connection, name: str) -> str | None:
    r = conn.execute("SELECT template FROM meta_prompts WHERE name = ?", (name,)).fetchone()
    if r is None:
        return None
    return str(r["template"])

def list_meta_prompts(conn: sqlite3.Connection) -> list[dict[str, str]]:
    rows = conn.execute("SELECT name, template FROM meta_prompts ORDER BY name").fetchall()
    return [{"name": str(r["name"]), "template": str(r["template"])}]

def upsert_meta_prompt(conn: sqlite3.Connection, name: str, template: str) -> None:
    from apps.prompt_refinement.meta_defaults import validate_meta_prompt

    validate_meta_prompt(name, template)
    conn.execute(
        """
        INSERT INTO meta_prompts (name, template) VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE SET template = excluded.template, updated_at = datetime('now')
        """,
        (name, template),
    )
    conn.commit()

# --- Optimization runs ---

def create_optimization_run(
    conn: sqlite3.Connection,
    *,
    input_profile_id: int,
    config: dict[str, Any],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO optimization_runs (input_profile_id, status, config_json)
        VALUES (?, 'running', ?)
        """,
        (input_profile_id, json.dumps(config, ensure_ascii=False)),
    )
    conn.commit()
    return int(cur.lastrowid)

def update_optimization_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str | None = None,
    summary: dict[str, Any] | None = None,
) -> None:
    if status is not None:
        conn.execute(
            "UPDATE optimization_runs SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, run_id),
        )
    if summary is not None:
        conn.execute(
            "UPDATE optimization_runs SET summary_json = ?, updated_at = datetime('now') WHERE id = ?",
            (json.dumps(summary, ensure_ascii=False), run_id),
        )
    conn.commit()

def touch_optimization_run(conn: sqlite3.Connection, run_id: int) -> None:
    """Heartbeat: bump ``updated_at`` so liveness can be inferred for running runs."""
    conn.execute(
        "UPDATE optimization_runs SET updated_at = datetime('now') WHERE id = ?",
        (run_id,),
    )
    conn.commit()

def mark_stale_running_runs(conn: sqlite3.Connection, *, max_idle_seconds: int) -> int:
    """Flag ``running`` runs with no heartbeat within ``max_idle_seconds`` as interrupted."""
    cur = conn.execute(
        """
        UPDATE optimization_runs
        SET status = 'interrupted', updated_at = datetime('now')
        WHERE status = 'running'
          AND (julianday('now') - julianday(updated_at)) * 86400.0 > ?
        """,
        (max_idle_seconds,),
    )
    conn.commit()
    return cur.rowcount

def list_optimization_runs(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT r.id, r.input_profile_id, r.status, r.config_json, r.summary_json,
               r.created_at, r.updated_at, p.name AS input_profile_name,
               (julianday('now') - julianday(r.updated_at)) * 86400.0 AS idle_seconds
        FROM optimization_runs r
        JOIN prompt_profiles p ON p.id = r.input_profile_id
        ORDER BY r.created_at DESC LIMIT ?
        """,
        (limit,),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for r in rows:
        summary = None
        if r["summary_json"]:
            try:
                summary = json.loads(str(r["summary_json"]))
            except json.JSONDecodeError:
                summary = None
        config = {}
        if r["config_json"]:
            try:
                config = json.loads(str(r["config_json"]))
            except json.JSONDecodeError:
                config = {}
        result.append(
            {
                "id": int(r["id"]),
                "input_profile_id": int(r["input_profile_id"]),
                "input_profile_name": str(r["input_profile_name"]),
                "status": str(r["status"]),
                "config": config,
                "summary": summary,
                "created_at": str(r["created_at"]),
                "updated_at": str(r["updated_at"]),
                "idle_seconds": float(r["idle_seconds"]) if r["idle_seconds"] is not None else None,
            }
        )
    return result

def add_optimization_iteration(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    iter_index: int,
    profile_id: int | None,
    metrics: dict[str, Any] | None,
    diagnosis: dict[str, Any] | None,
    proposed_changes: dict[str, Any] | None,
    accepted: bool,
    notes: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO optimization_iterations (
            run_id, iter_index, profile_id, metrics_json, diagnosis_json,
            proposed_changes_json, accepted, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            iter_index,
            profile_id,
            json.dumps(metrics, ensure_ascii=False) if metrics else None,
            json.dumps(diagnosis, ensure_ascii=False) if diagnosis else None,
            json.dumps(proposed_changes, ensure_ascii=False) if proposed_changes else None,
            1 if accepted else 0,
            notes,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)

def fetch_iterations_for_run(conn: sqlite3.Connection, run_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT i.*, p.name AS profile_name
        FROM optimization_iterations i
        LEFT JOIN prompt_profiles p ON p.id = i.profile_id
        WHERE i.run_id = ?
        ORDER BY i.iter_index
        """,
        (run_id,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        def _load(field: str) -> Any:
            raw = r[field]
            if not raw:
                return None
            try:
                return json.loads(str(raw))
            except json.JSONDecodeError:
                return None

        out.append(
            {
                "id": int(r["id"]),
                "run_id": int(r["run_id"]),
                "iter_index": int(r["iter_index"]),
                "profile_id": int(r["profile_id"]) if r["profile_id"] else None,
                "profile_name": str(r["profile_name"]) if r["profile_name"] else None,
                "metrics": _load("metrics_json"),
                "diagnosis": _load("diagnosis_json"),
                "proposed_changes": _load("proposed_changes_json"),
                "accepted": bool(r["accepted"]),
                "notes": str(r["notes"]) if r["notes"] else None,
                "created_at": str(r["created_at"]),
            }
        )
    return out
