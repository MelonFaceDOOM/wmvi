"""Build measles2_demo.sqlite from exp labels, names, claims.json, and the embed index."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims.demo import ANTI_CUTOFF, BUNDLE_FILE
from apps.claims.demo.catalog import (
    DEFAULT_CLAIMS,
    DEFAULT_EXP_DIR,
    DEFAULT_RUN,
    load_catalog,
    load_label_arrays,
    load_membership,
    load_names,
)
from apps.claims.io import load_run_index

SCHEMA_SQL = """
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE narratives (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    blurb TEXT NOT NULL DEFAULT '',
    n_leaves INTEGER NOT NULL DEFAULT 0,
    n_occurrences INTEGER NOT NULL DEFAULT 0,
    n_anti INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE leaves (
    id INTEGER PRIMARY KEY,
    narrative_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    blurb TEXT NOT NULL DEFAULT '',
    n_claims INTEGER NOT NULL DEFAULT 0,
    n_occurrences INTEGER NOT NULL DEFAULT 0,
    n_anti INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE claims (
    idx INTEGER PRIMARY KEY,
    claim_key TEXT NOT NULL,
    claim_text TEXT NOT NULL,
    leaf_id INTEGER NOT NULL,
    narrative_id INTEGER NOT NULL,
    occurrence_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE occurrences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_idx INTEGER NOT NULL,
    leaf_id INTEGER NOT NULL,
    narrative_id INTEGER NOT NULL,
    ts TEXT,
    week TEXT,
    platform TEXT,
    alignment REAL,
    url TEXT,
    post_id TEXT,
    snippet TEXT
);
CREATE INDEX occ_leaf ON occurrences(leaf_id);
CREATE INDEX occ_nar ON occurrences(narrative_id);
CREATE INDEX occ_week ON occurrences(week);
CREATE INDEX occ_align ON occurrences(alignment);
"""


def _iso_week(ts: str | None) -> str | None:
    if not ts:
        return None
    raw = str(ts).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return str(ts)[:10] or None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _occ_key(task_id: str, claim_index: int) -> tuple[str, int]:
    return str(task_id), int(claim_index)


def load_occurrence_map(claims_path: Path) -> dict[tuple[str, int], dict[str, Any]]:
    payload = json.loads(Path(claims_path).read_text(encoding="utf-8"))
    posts = payload.get("posts") if isinstance(payload, dict) else payload
    out: dict[tuple[str, int], dict[str, Any]] = {}
    if not isinstance(posts, list):
        return out
    for post in posts:
        if not isinstance(post, dict):
            continue
        platform = str(post.get("platform") or "unknown")
        ts = post.get("created_at_ts")
        url = str(post.get("url") or "")
        post_id = str(post.get("post_id") or "")
        post_text = str(post.get("text") or "")
        for chunk in post.get("chunks") or []:
            if not isinstance(chunk, dict):
                continue
            task_id = str(chunk.get("task_id") or "")
            if not task_id:
                cidx = chunk.get("chunk_index")
                if post_id and cidx is not None:
                    task_id = f"{post_id}:{cidx}"
            chunk_text = str(chunk.get("text") or "").strip()
            snippet = chunk_text or post_text
            for ci, claim in enumerate(chunk.get("claims") or []):
                if not isinstance(claim, dict):
                    continue
                align = claim.get("claim_vaccine_alignment_score")
                try:
                    align_f = float(align) if align is not None else None
                except (TypeError, ValueError):
                    align_f = None
                out[_occ_key(task_id, ci)] = {
                    "ts": ts,
                    "week": _iso_week(str(ts) if ts else None),
                    "platform": platform,
                    "alignment": align_f,
                    "url": url,
                    "post_id": post_id,
                    "snippet": snippet,
                }
    return out


def _title_maps(names: dict[str, Any]) -> tuple[dict[int, tuple[str, str]], dict[int, tuple[str, str]]]:
    nars: dict[int, tuple[str, str]] = {}
    leaves: dict[int, tuple[str, str]] = {}
    for row in names.get("narratives") or []:
        nars[int(row["id"])] = (str(row.get("title") or f"Narrative {row['id']}"), str(row.get("blurb") or ""))
    for row in names.get("leaves") or []:
        leaves[int(row["id"])] = (str(row.get("title") or f"Leaf {row['id']}"), str(row.get("blurb") or ""))
    return nars, leaves


def pack_bundle(
    *,
    exp_dir: Path | None = None,
    claims_path: Path | None = None,
    run_dir: Path | None = None,
    out_path: Path | None = None,
) -> dict[str, Any]:
    exp_dir = Path(exp_dir or DEFAULT_EXP_DIR)
    claims_path = Path(claims_path or DEFAULT_CLAIMS)
    run_dir = Path(run_dir or DEFAULT_RUN)
    out_path = Path(out_path or (exp_dir / BUNDLE_FILE))

    catalog = load_catalog(exp_dir)
    leaf_labels, nar_labels = load_label_arrays(catalog)
    membership = load_membership(exp_dir)
    names = load_names(exp_dir)
    nar_titles, leaf_titles = _title_maps(names)
    index = load_run_index(run_dir)
    groups = list(index.get("groups") or [])
    n = int(leaf_labels.shape[0])
    if len(groups) != n:
        raise ValueError(f"index groups ({len(groups)}) != labels ({n})")

    occ_map = load_occurrence_map(claims_path)

    resolved_nar = np.array(nar_labels, dtype=int, copy=True)
    for i in range(n):
        lid = int(leaf_labels[i])
        if lid < 0:
            resolved_nar[i] = -1
        elif lid in membership:
            resolved_nar[i] = int(membership[lid])

    if out_path.exists():
        out_path.unlink()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(out_path))
    try:
        conn.executescript(SCHEMA_SQL)
        cur = conn.cursor()
        occ_rows: list[tuple[Any, ...]] = []
        claim_rows: list[tuple[Any, ...]] = []
        leaf_claim_n: dict[int, int] = {}
        leaf_occ_n: dict[int, int] = {}
        leaf_anti_n: dict[int, int] = {}
        nar_occ_n: dict[int, int] = {}
        nar_anti_n: dict[int, int] = {}
        nar_leaf_ids: dict[int, set[int]] = {}

        for i in range(n):
            lid = int(leaf_labels[i])
            nid = int(resolved_nar[i])
            g = groups[i] if i < len(groups) else {}
            text = str(g.get("claim_text") or "")
            key = str(g.get("claim_key") or "")
            sources = list(g.get("sources") or [])
            n_src = len(sources) or int(g.get("count") or 0)
            claim_rows.append((i, key, text, lid, nid, n_src))
            if lid >= 0:
                leaf_claim_n[lid] = leaf_claim_n.get(lid, 0) + 1
                nar_leaf_ids.setdefault(nid, set()).add(lid)
            for src in sources:
                if not isinstance(src, dict):
                    continue
                tid = str(src.get("task_id") or "")
                cidx = int(src.get("claim_index") or 0)
                meta = occ_map.get(_occ_key(tid, cidx), {})
                align = meta.get("alignment")
                occ_rows.append(
                    (
                        i,
                        lid,
                        nid,
                        meta.get("ts"),
                        meta.get("week"),
                        meta.get("platform"),
                        align,
                        meta.get("url"),
                        meta.get("post_id"),
                        meta.get("snippet"),
                    )
                )
                if lid >= 0:
                    leaf_occ_n[lid] = leaf_occ_n.get(lid, 0) + 1
                    nar_occ_n[nid] = nar_occ_n.get(nid, 0) + 1
                    if align is not None and float(align) <= ANTI_CUTOFF:
                        leaf_anti_n[lid] = leaf_anti_n.get(lid, 0) + 1
                        nar_anti_n[nid] = nar_anti_n.get(nid, 0) + 1

        cur.executemany(
            "INSERT INTO claims(idx, claim_key, claim_text, leaf_id, narrative_id, occurrence_count) "
            "VALUES (?,?,?,?,?,?)",
            claim_rows,
        )
        cur.executemany(
            "INSERT INTO occurrences(claim_idx, leaf_id, narrative_id, ts, week, platform, alignment, url, post_id, snippet) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            occ_rows,
        )

        leaf_ids = sorted({int(x) for x in leaf_labels.tolist() if int(x) >= 0})
        for lid in leaf_ids:
            nid = int(membership[lid]) if lid in membership else int(
                next((resolved_nar[i] for i in range(n) if int(leaf_labels[i]) == lid), -1)
            )
            title, blurb = leaf_titles.get(lid, (f"Leaf {lid}", ""))
            if not title:
                info = catalog.leaves_by_id.get(lid)
                title = (info.medoid[:120] if info and info.medoid else f"Leaf {lid}")
            cur.execute(
                "INSERT INTO leaves(id, narrative_id, title, blurb, n_claims, n_occurrences, n_anti) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    lid,
                    nid,
                    title,
                    blurb,
                    leaf_claim_n.get(lid, 0),
                    leaf_occ_n.get(lid, 0),
                    leaf_anti_n.get(lid, 0),
                ),
            )

        nar_ids = sorted(set(int(x) for x in resolved_nar.tolist() if int(x) >= 0))
        for nid in nar_ids:
            title, blurb = nar_titles.get(nid, (f"Narrative {nid}", ""))
            cur.execute(
                "INSERT INTO narratives(id, title, blurb, n_leaves, n_occurrences, n_anti) "
                "VALUES (?,?,?,?,?,?)",
                (
                    nid,
                    title,
                    blurb,
                    len(nar_leaf_ids.get(nid, set())),
                    nar_occ_n.get(nid, 0),
                    nar_anti_n.get(nid, 0),
                ),
            )

        ts_vals = [r[3] for r in occ_rows if r[3]]
        meta = {
            "corpus": "measles2",
            "exp_dir": str(exp_dir),
            "anti_cutoff": str(ANTI_CUTOFF),
            "n_claims": str(n),
            "n_occurrences": str(len(occ_rows)),
            "n_narratives": str(len(nar_ids)),
            "n_leaves": str(len(leaf_ids)),
            "ts_min": str(min(ts_vals)) if ts_vals else "",
            "ts_max": str(max(ts_vals)) if ts_vals else "",
        }
        cur.executemany("INSERT INTO meta(key, value) VALUES (?,?)", list(meta.items()))
        conn.commit()
    finally:
        conn.close()

    return {
        "path": str(out_path.resolve()),
        "n_claims": n,
        "n_occurrences": len(occ_rows),
        "n_narratives": len(nar_ids),
        "n_leaves": len(leaf_ids),
    }
