"""SQLite persistence for the embedding lab (metadata only; heavy arrays live on disk)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from apps.claim_extractor.embedding_lab.models import (
    DEFAULT_DOC_INSTRUCTION,
    DEFAULT_MODEL,
    DEFAULT_QUERY_INSTRUCTION,
)


def default_db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "embedding.sqlite"


def artifacts_root() -> Path:
    return Path(__file__).resolve().parent / "data" / "artifacts"


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if column not in _table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS embed_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            model_id TEXT NOT NULL,
            doc_instruction TEXT NOT NULL DEFAULT '',
            query_instruction TEXT NOT NULL DEFAULT '',
            normalize INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS embed_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL REFERENCES embed_profiles(id) ON DELETE CASCADE,
            source_hash TEXT NOT NULL,
            source_path TEXT,
            claim_count INTEGER NOT NULL DEFAULT 0,
            source_claim_count INTEGER,
            vector_dim INTEGER,
            dtype TEXT,
            artifact_dir TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'success',
            device TEXT,
            wall_seconds REAL,
            claims_per_sec REAL,
            peak_ram_mb REAL,
            ram_delta_mb REAL,
            peak_gpu_mb REAL,
            artifact_bytes INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (profile_id, source_hash)
        );
        CREATE TABLE IF NOT EXISTS cluster_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            algorithm TEXT NOT NULL,
            params_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS cluster_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            embed_run_id INTEGER NOT NULL REFERENCES embed_runs(id) ON DELETE CASCADE,
            cluster_profile_id INTEGER NOT NULL REFERENCES cluster_profiles(id) ON DELETE CASCADE,
            labels_path TEXT NOT NULL,
            n_clusters INTEGER,
            n_noise INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (embed_run_id, cluster_profile_id)
        );
        CREATE TABLE IF NOT EXISTS triplets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anchor TEXT NOT NULL,
            positive TEXT NOT NULL,
            negative TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS triplet_results (
            embed_run_id INTEGER PRIMARY KEY REFERENCES embed_runs(id) ON DELETE CASCADE,
            accuracy REAL,
            mean_margin REAL,
            triplet_count INTEGER,
            per_triplet_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS cluster_names (
            cluster_run_id INTEGER NOT NULL REFERENCES cluster_runs(id) ON DELETE CASCADE,
            cluster_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (cluster_run_id, cluster_id)
        );
        CREATE INDEX IF NOT EXISTS idx_embed_runs_profile ON embed_runs(profile_id);
        CREATE INDEX IF NOT EXISTS idx_cluster_runs_embed ON cluster_runs(embed_run_id);
        CREATE INDEX IF NOT EXISTS idx_cluster_names_run ON cluster_names(cluster_run_id);
        """
    )
    _ensure_column(conn, "embed_runs", "source_claim_count", "source_claim_count INTEGER")
    conn.commit()


def init_lab(conn: sqlite3.Connection) -> None:
    init_schema(conn)


# --- Embedding profiles ---


@dataclass
class EmbedProfile:
    id: int
    name: str
    model_id: str
    doc_instruction: str
    query_instruction: str
    normalize: bool
    created_at: str | None = None


def _row_to_embed_profile(r: sqlite3.Row) -> EmbedProfile:
    return EmbedProfile(
        id=int(r["id"]),
        name=str(r["name"]),
        model_id=str(r["model_id"]),
        doc_instruction=str(r["doc_instruction"] or ""),
        query_instruction=str(r["query_instruction"] or ""),
        normalize=bool(r["normalize"]),
        created_at=str(r["created_at"]) if r["created_at"] else None,
    )


def list_embed_profiles(conn: sqlite3.Connection) -> list[EmbedProfile]:
    rows = conn.execute("SELECT * FROM embed_profiles ORDER BY id DESC").fetchall()
    return [_row_to_embed_profile(r) for r in rows]


def get_embed_profile(conn: sqlite3.Connection, profile_id: int) -> EmbedProfile | None:
    r = conn.execute("SELECT * FROM embed_profiles WHERE id = ?", (profile_id,)).fetchone()
    return _row_to_embed_profile(r) if r is not None else None


def get_embed_profile_by_name(conn: sqlite3.Connection, name: str) -> EmbedProfile | None:
    r = conn.execute("SELECT * FROM embed_profiles WHERE name = ?", (name.strip(),)).fetchone()
    return _row_to_embed_profile(r) if r is not None else None


def create_embed_profile(
    conn: sqlite3.Connection,
    *,
    name: str,
    model_id: str = DEFAULT_MODEL,
    doc_instruction: str = DEFAULT_DOC_INSTRUCTION,
    query_instruction: str = DEFAULT_QUERY_INSTRUCTION,
    normalize: bool = True,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO embed_profiles (name, model_id, doc_instruction, query_instruction, normalize)
        VALUES (?, ?, ?, ?, ?)
        """,
        (name.strip(), model_id, doc_instruction, query_instruction, 1 if normalize else 0),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_embed_profile(
    conn: sqlite3.Connection,
    profile_id: int,
    *,
    name: str,
    model_id: str,
    doc_instruction: str,
    query_instruction: str,
    normalize: bool,
) -> None:
    conn.execute(
        """
        UPDATE embed_profiles
        SET name = ?, model_id = ?, doc_instruction = ?, query_instruction = ?, normalize = ?
        WHERE id = ?
        """,
        (name.strip(), model_id, doc_instruction, query_instruction, 1 if normalize else 0, profile_id),
    )
    conn.commit()


# --- Embedding runs ---


def get_embed_run(conn: sqlite3.Connection, run_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM embed_runs WHERE id = ?", (run_id,)).fetchone()
    return dict(r) if r is not None else None


def get_embed_run_for(conn: sqlite3.Connection, profile_id: int, source_hash: str) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM embed_runs WHERE profile_id = ? AND source_hash = ?",
        (profile_id, source_hash),
    ).fetchone()
    return dict(r) if r is not None else None


def list_embed_runs(conn: sqlite3.Connection, *, profile_id: int | None = None) -> list[dict[str, Any]]:
    if profile_id is None:
        rows = conn.execute(
            """
            SELECT r.*, p.name AS profile_name
            FROM embed_runs r JOIN embed_profiles p ON p.id = r.profile_id
            ORDER BY r.created_at DESC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT r.*, p.name AS profile_name
            FROM embed_runs r JOIN embed_profiles p ON p.id = r.profile_id
            WHERE r.profile_id = ?
            ORDER BY r.created_at DESC
            """,
            (profile_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def upsert_embed_run(
    conn: sqlite3.Connection,
    *,
    profile_id: int,
    source_hash: str,
    source_path: str,
    claim_count: int,
    source_claim_count: int | None = None,
    vector_dim: int,
    dtype: str,
    artifact_dir: str,
    metrics: dict[str, Any],
) -> int:
    conn.execute(
        """
        INSERT INTO embed_runs (
            profile_id, source_hash, source_path, claim_count, source_claim_count,
            vector_dim, dtype, artifact_dir, status, device, wall_seconds, claims_per_sec,
            peak_ram_mb, ram_delta_mb, peak_gpu_mb, artifact_bytes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'success', ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(profile_id, source_hash) DO UPDATE SET
            source_path = excluded.source_path,
            claim_count = excluded.claim_count,
            source_claim_count = excluded.source_claim_count,
            vector_dim = excluded.vector_dim,
            dtype = excluded.dtype,
            artifact_dir = excluded.artifact_dir,
            status = excluded.status,
            device = excluded.device,
            wall_seconds = excluded.wall_seconds,
            claims_per_sec = excluded.claims_per_sec,
            peak_ram_mb = excluded.peak_ram_mb,
            ram_delta_mb = excluded.ram_delta_mb,
            peak_gpu_mb = excluded.peak_gpu_mb,
            artifact_bytes = excluded.artifact_bytes,
            created_at = datetime('now')
        """,
        (
            profile_id,
            source_hash,
            source_path,
            claim_count,
            source_claim_count,
            vector_dim,
            dtype,
            artifact_dir,
            metrics.get("device"),
            metrics.get("wall_seconds"),
            metrics.get("claims_per_sec"),
            metrics.get("peak_ram_mb"),
            metrics.get("ram_delta_mb"),
            metrics.get("peak_gpu_mb"),
            metrics.get("artifact_bytes"),
        ),
    )
    conn.commit()
    row = get_embed_run_for(conn, profile_id, source_hash)
    return int(row["id"]) if row else -1


# --- Cluster profiles / runs ---


@dataclass
class ClusterProfile:
    id: int
    name: str
    algorithm: str
    params: dict[str, Any]
    created_at: str | None = None


def _row_to_cluster_profile(r: sqlite3.Row) -> ClusterProfile:
    try:
        params = json.loads(str(r["params_json"])) if r["params_json"] else {}
    except json.JSONDecodeError:
        params = {}
    return ClusterProfile(
        id=int(r["id"]),
        name=str(r["name"]),
        algorithm=str(r["algorithm"]),
        params=params if isinstance(params, dict) else {},
        created_at=str(r["created_at"]) if r["created_at"] else None,
    )


def list_cluster_profiles(conn: sqlite3.Connection) -> list[ClusterProfile]:
    rows = conn.execute("SELECT * FROM cluster_profiles ORDER BY id DESC").fetchall()
    return [_row_to_cluster_profile(r) for r in rows]


def get_cluster_profile(conn: sqlite3.Connection, profile_id: int) -> ClusterProfile | None:
    r = conn.execute("SELECT * FROM cluster_profiles WHERE id = ?", (profile_id,)).fetchone()
    return _row_to_cluster_profile(r) if r is not None else None


def create_cluster_profile(
    conn: sqlite3.Connection, *, name: str, algorithm: str, params: dict[str, Any]
) -> int:
    cur = conn.execute(
        "INSERT INTO cluster_profiles (name, algorithm, params_json) VALUES (?, ?, ?)",
        (name.strip(), algorithm, json.dumps(params, ensure_ascii=False)),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_cluster_profile(
    conn: sqlite3.Connection, profile_id: int, *, name: str, algorithm: str, params: dict[str, Any]
) -> None:
    conn.execute(
        "UPDATE cluster_profiles SET name = ?, algorithm = ?, params_json = ? WHERE id = ?",
        (name.strip(), algorithm, json.dumps(params, ensure_ascii=False), profile_id),
    )
    conn.commit()


def upsert_cluster_run(
    conn: sqlite3.Connection,
    *,
    embed_run_id: int,
    cluster_profile_id: int,
    labels_path: str,
    n_clusters: int,
    n_noise: int,
) -> int:
    conn.execute(
        """
        INSERT INTO cluster_runs (embed_run_id, cluster_profile_id, labels_path, n_clusters, n_noise)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(embed_run_id, cluster_profile_id) DO UPDATE SET
            labels_path = excluded.labels_path,
            n_clusters = excluded.n_clusters,
            n_noise = excluded.n_noise,
            created_at = datetime('now')
        """,
        (embed_run_id, cluster_profile_id, labels_path, n_clusters, n_noise),
    )
    conn.commit()
    r = conn.execute(
        "SELECT id FROM cluster_runs WHERE embed_run_id = ? AND cluster_profile_id = ?",
        (embed_run_id, cluster_profile_id),
    ).fetchone()
    return int(r["id"]) if r else -1


def list_cluster_runs_for_embed(conn: sqlite3.Connection, embed_run_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT cr.*, cp.name AS cluster_profile_name, cp.algorithm AS algorithm
        FROM cluster_runs cr JOIN cluster_profiles cp ON cp.id = cr.cluster_profile_id
        WHERE cr.embed_run_id = ?
        ORDER BY cr.created_at DESC
        """,
        (embed_run_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_cluster_run(conn: sqlite3.Connection, cluster_run_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM cluster_runs WHERE id = ?", (cluster_run_id,)).fetchone()
    return dict(r) if r is not None else None


# --- Cluster names ---


def upsert_cluster_names(
    conn: sqlite3.Connection,
    *,
    cluster_run_id: int,
    names: dict[int, str],
    sizes: dict[int, int],
) -> None:
    rows = [
        (cluster_run_id, int(cluster_id), str(name), int(sizes.get(cluster_id, 0)))
        for cluster_id, name in names.items()
    ]
    conn.executemany(
        """
        INSERT INTO cluster_names (cluster_run_id, cluster_id, name, size)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(cluster_run_id, cluster_id) DO UPDATE SET
            name = excluded.name,
            size = excluded.size
        """,
        rows,
    )
    conn.commit()


def list_cluster_names(conn: sqlite3.Connection, cluster_run_id: int) -> dict[int, dict[str, Any]]:
    rows = conn.execute(
        "SELECT cluster_id, name, size FROM cluster_names WHERE cluster_run_id = ? ORDER BY size DESC, cluster_id",
        (cluster_run_id,),
    ).fetchall()
    return {
        int(r["cluster_id"]): {"name": str(r["name"]), "size": int(r["size"])}
        for r in rows
    }


def update_cluster_name(
    conn: sqlite3.Connection,
    *,
    cluster_run_id: int,
    cluster_id: int,
    name: str,
    size: int,
) -> None:
    conn.execute(
        """
        INSERT INTO cluster_names (cluster_run_id, cluster_id, name, size)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(cluster_run_id, cluster_id) DO UPDATE SET name = excluded.name
        """,
        (cluster_run_id, cluster_id, name.strip(), size),
    )
    conn.commit()


# --- Triplets ---


@dataclass
class Triplet:
    id: int
    anchor: str
    positive: str
    negative: str


def list_triplets(conn: sqlite3.Connection) -> list[Triplet]:
    rows = conn.execute("SELECT * FROM triplets ORDER BY id").fetchall()
    return [
        Triplet(id=int(r["id"]), anchor=str(r["anchor"]), positive=str(r["positive"]), negative=str(r["negative"]))
        for r in rows
    ]


def replace_triplets(conn: sqlite3.Connection, triplets: list[tuple[str, str, str]]) -> None:
    """Replace the entire global triplet set with the provided rows."""
    conn.execute("DELETE FROM triplets")
    conn.executemany(
        "INSERT INTO triplets (anchor, positive, negative) VALUES (?, ?, ?)",
        [(a, p, n) for (a, p, n) in triplets],
    )
    conn.commit()


def upsert_triplet_result(
    conn: sqlite3.Connection,
    *,
    embed_run_id: int,
    accuracy: float,
    mean_margin: float,
    triplet_count: int,
    per_triplet: list[dict[str, Any]],
) -> None:
    conn.execute(
        """
        INSERT INTO triplet_results (embed_run_id, accuracy, mean_margin, triplet_count, per_triplet_json)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(embed_run_id) DO UPDATE SET
            accuracy = excluded.accuracy,
            mean_margin = excluded.mean_margin,
            triplet_count = excluded.triplet_count,
            per_triplet_json = excluded.per_triplet_json,
            created_at = datetime('now')
        """,
        (embed_run_id, accuracy, mean_margin, triplet_count, json.dumps(per_triplet, ensure_ascii=False)),
    )
    conn.commit()


def get_triplet_result(conn: sqlite3.Connection, embed_run_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM triplet_results WHERE embed_run_id = ?", (embed_run_id,)).fetchone()
    if r is None:
        return None
    out = dict(r)
    if out.get("per_triplet_json"):
        try:
            out["per_triplet"] = json.loads(str(out["per_triplet_json"]))
        except json.JSONDecodeError:
            out["per_triplet"] = []
    else:
        out["per_triplet"] = []
    return out
