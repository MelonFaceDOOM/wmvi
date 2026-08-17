"""Flatten corpus chunks for prompt-lab eval samples (pool / write / import)."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from apps.claims import annotations as ann_mod
from apps.claims import claims_data
from apps.claims import corpus as corpus_mod
from apps.claims.keys import claim_key
from apps.prompt_refinement import db, posts_data

DEFAULT_STANDALONE_ANN = "standalone_pred_m1"
SAMPLES_DIR = Path(__file__).resolve().parent / "data" / "samples"


def _claim_text(c: Any) -> str:
    if isinstance(c, dict):
        return str(c.get("claim") or "").strip()
    return str(c or "").strip()


def flatten_corpus_chunks(
    *,
    corpus: str,
    standalone_ann: str = DEFAULT_STANDALONE_ANN,
) -> list[dict[str, Any]]:
    """One pool row per usable chunk with standalone flags from annotation."""
    corp = corpus_mod.get_corpus(corpus)
    if not corp.claims.is_file():
        raise FileNotFoundError(f"Missing claims.json at {corp.claims}")
    _, posts = claims_data.load_posts_from_claims_json(corp.claims)

    standalone_values: dict[str, Any] = {}
    ann_path = corp.root / "annotations" / f"{standalone_ann}.jsonl"
    if ann_path.is_file():
        ann = ann_mod.read_annotation(corp.root, standalone_ann)
        standalone_values = dict(ann.values)

    rows: list[dict[str, Any]] = []
    for post in posts:
        chunks = post.get("chunks")
        if not isinstance(chunks, list):
            continue
        platform = str(post.get("platform") or "(unknown)")
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            if not claims_data.chunk_is_usable(chunk):
                continue
            ctx = claims_data.context_row_for_chunk(post, chunk)
            tid = str(ctx["task_id"])
            claims_raw = chunk.get("claims") or []
            if not isinstance(claims_raw, list):
                claims_raw = []
            claim_objs: list[dict[str, Any]] = []
            claim_scores: list[dict[str, Any]] = []
            has_standalone_0 = False
            for i, c in enumerate(claims_raw):
                text = _claim_text(c)
                if not text:
                    continue
                obj = {"claim": text} if not isinstance(c, dict) else {"claim": text}
                claim_objs.append(obj)
                ck = claim_key(text)
                score = standalone_values.get(ck)
                if score is not None:
                    try:
                        score_f = float(score)
                    except (TypeError, ValueError):
                        score_f = None
                else:
                    score_f = None
                # Predictions are probabilities; binary "0" ≈ score < 0.5.
                if score_f is not None and score_f < 0.5:
                    has_standalone_0 = True
                claim_scores.append(
                    {
                        "claim_index": i,
                        "claim_key": ck,
                        "standalone": score_f,
                        "claim": text,
                    }
                )
            if not claim_objs:
                continue
            # Shape for prompt lab: post_row with extraction fields
            post_row = dict(ctx)
            post_row["claim_extraction_status"] = "success"
            post_row["claim_extraction_output"] = {"claims": claim_objs}
            rows.append(
                {
                    "task_id": tid,
                    "platform": platform,
                    "has_standalone_0": has_standalone_0,
                    "n_claims": len(claim_objs),
                    "claim_scores": claim_scores,
                    "text": str(post_row.get("text") or ""),
                    "claims": claim_objs,
                    "post_row": post_row,
                }
            )
    return rows


def pool_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_plat = Counter(str(r.get("platform") or "?") for r in rows)
    by_plat_s0 = Counter(
        str(r.get("platform") or "?") for r in rows if r.get("has_standalone_0")
    )
    return {
        "n_chunks": len(rows),
        "n_has_standalone_0": sum(1 for r in rows if r.get("has_standalone_0")),
        "by_platform": dict(sorted(by_plat.items())),
        "by_platform_standalone_0": dict(sorted(by_plat_s0.items())),
    }


def write_pool_json(rows: list[dict[str, Any]], out: Path, *, corpus: str) -> dict[str, Any]:
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    summary = pool_summary(rows)
    payload = {
        "kind": "prompt_refinement_eval_pool",
        "corpus": corpus,
        "standalone_ann": DEFAULT_STANDALONE_ANN,
        "summary": summary,
        "chunks": rows,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    return summary


def load_pool(path: Path) -> dict[str, Any]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("chunks"), list):
        raise ValueError(f"Invalid pool file: {path}")
    return data


def read_task_ids(path: Path) -> list[str]:
    """Read task_ids from text (one per line) or JSON list / {ids|task_ids}."""
    raw = Path(path).read_text(encoding="utf-8").strip()
    if not raw:
        return []
    if raw[0] in "[{":
        data = json.loads(raw)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
        if isinstance(data, dict):
            for key in ("task_ids", "ids", "keys"):
                if isinstance(data.get(key), list):
                    return [str(x).strip() for x in data[key] if str(x).strip()]
        raise ValueError(f"Unrecognized JSON id file: {path}")
    out: list[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def write_sample_from_ids(
    *,
    pool_path: Path,
    ids: list[str],
    out: Path,
) -> dict[str, Any]:
    pool = load_pool(pool_path)
    by_id = {str(c["task_id"]): c for c in pool["chunks"] if isinstance(c, dict)}
    missing = [tid for tid in ids if tid not in by_id]
    if missing:
        raise KeyError(f"task_id(s) not in pool: {missing[:10]}")
    selected = [by_id[tid] for tid in ids]
    # Lab-friendly posts array
    posts = [c["post_row"] for c in selected]
    payload = {
        "kind": "prompt_refinement_eval_sample",
        "corpus": pool.get("corpus"),
        "n": len(selected),
        "task_ids": ids,
        "chunks": selected,
        "posts": posts,
    }
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    return {"n": len(selected), "out": str(out), "summary": pool_summary(selected)}


def import_sample_to_lab(
    *,
    sample_path: Path,
    db_path: Path | None = None,
    clear_existing: bool = False,
) -> dict[str, Any]:
    """Load eval sample into problem_posts; sync Baseline extractions."""
    data = json.loads(Path(sample_path).read_text(encoding="utf-8"))
    posts = data.get("posts")
    chunks = data.get("chunks")
    if not isinstance(posts, list) or not posts:
        # Fall back to reconstructing from chunks
        if not isinstance(chunks, list) or not chunks:
            raise ValueError("Sample must have posts[] or chunks[]")
        posts = [c["post_row"] for c in chunks if isinstance(c, dict) and c.get("post_row")]

    path = db_path or db.default_db_path()
    conn = db.connect(path)
    try:
        db.init_lab(conn)
        if clear_existing:
            conn.execute("DELETE FROM profile_extractions")
            conn.execute("DELETE FROM problem_posts")
            conn.commit()
        inserted = 0
        skipped = 0
        for post_row in posts:
            if not isinstance(post_row, dict):
                continue
            tid = str(post_row.get("task_id") or "")
            if not tid:
                continue
            if db.is_problem_post(conn, tid):
                skipped += 1
                continue
            baseline = posts_data.baseline_claims_from_post(post_row)
            status = posts_data.extraction_status(post_row)
            if db.insert_problem_post_ignore(
                conn,
                task_id=tid,
                post_row=post_row,
                baseline_claims=baseline,
                baseline_status=status,
                comment="eval_sample",
                source="eval_sample",
            ):
                inserted += 1
            else:
                skipped += 1
        synced = db.sync_baseline_extractions(conn)
        return {
            "db": str(path),
            "inserted": inserted,
            "skipped": skipped,
            "baseline_synced": synced,
            "n_problem": db.count_problem_posts(conn),
        }
    finally:
        conn.close()


def canonical_to_lab_placeholders(system: str, user: str) -> tuple[str, str]:
    """Convert ``{{var}}`` prompt templates to Prompt Lab ``{var}`` form."""
    user = (
        user.replace("{{text_input}}", "{text_input}")
        .replace("{{max_claims}}", "{max_claims}")
        .replace("[[text_input]]", "{text_input}")
        .replace("[[max_claims]]", "{max_claims}")
    )
    system = (
        system.replace("{{max_claims}}", "{max_claims}")
        .replace("[[max_claims]]", "{max_claims}")
    )
    return system, user
