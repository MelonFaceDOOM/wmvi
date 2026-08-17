"""Blind probe injection + neighbor-aware sample for embedder labeling runs."""

from __future__ import annotations

import secrets
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims import provenance as prov


def runs_dir(intent: str) -> Path:
    from apps.claims.embedding import triplets as trip_data

    return trip_data.intent_dir(intent) / "runs"


def run_ledger_path(intent: str, run_id: str) -> Path:
    return runs_dir(intent) / f"{prov.safe_slug(run_id)}.json"


def mint_run_id() -> str:
    return f"run_{prov.utc_now().replace(':', '').replace('-', '')}_{secrets.token_hex(4)}"


def load_run_ledger(intent: str, run_id: str) -> dict[str, Any] | None:
    path = run_ledger_path(intent, run_id)
    if not path.is_file():
        return None
    return claims_io.read_json(path)


def save_run_ledger(intent: str, ledger: dict[str, Any]) -> Path:
    run_id = str(ledger["run_id"])
    path = run_ledger_path(intent, run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    claims_io.write_json(path, ledger)
    return path


def ensure_run_ledger(
    intent: str,
    *,
    corpus: str,
    run_id: str | None = None,
    model_tag: str | None = None,
) -> dict[str, Any]:
    rid = run_id or mint_run_id()
    existing = load_run_ledger(intent, rid)
    if existing is not None:
        return existing
    ledger = {
        "run_id": rid,
        "intent": intent,
        "corpus": corpus,
        "model_tag": model_tag,
        "created_at": prov.utc_now(),
        "served_probes": [],
        "sample_batches": [],  # [{batch_id, anchors: [{claim_key, neighbor_keys}]}]
    }
    save_run_ledger(intent, ledger)
    return ledger


def served_probe_keys(intent: str, run_id: str) -> set[str]:
    ledger = load_run_ledger(intent, run_id)
    if not ledger:
        return set()
    return {str(p["claim_key"]) for p in (ledger.get("served_probes") or [])}


def record_served_probes(
    intent: str,
    run_id: str,
    claim_keys: list[str],
    *,
    served_at: str | None = None,
) -> dict[str, Any]:
    ledger = load_run_ledger(intent, run_id)
    if ledger is None:
        raise FileNotFoundError(f"Missing run ledger for {intent}/{run_id}")
    ts = served_at or prov.utc_now()
    existing = served_probe_keys(intent, run_id)
    for ck in claim_keys:
        if ck in existing:
            continue
        ledger.setdefault("served_probes", []).append({"claim_key": ck, "served_at": ts})
        existing.add(ck)
    save_run_ledger(intent, ledger)
    return ledger


def record_sample_batch(
    intent: str,
    run_id: str,
    *,
    batch_id: str,
    anchors: list[dict[str, Any]],
) -> dict[str, Any]:
    """Persist shown neighbor key lists so triplets-import can resolve 1-based indices."""
    ledger = load_run_ledger(intent, run_id)
    if ledger is None:
        raise FileNotFoundError(f"Missing run ledger for {intent}/{run_id}")
    ledger.setdefault("sample_batches", []).append(
        {
            "batch_id": batch_id,
            "recorded_at": prov.utc_now(),
            "anchors": anchors,
        }
    )
    save_run_ledger(intent, ledger)
    return ledger


def latest_sample_neighbor_map(
    intent: str,
    run_id: str,
) -> dict[str, list[str]]:
    """Map anchor_key → ordered neighbor claim_keys from the most recent sample batch."""
    ledger = load_run_ledger(intent, run_id)
    if not ledger:
        return {}
    batches = ledger.get("sample_batches") or []
    if not batches:
        return {}
    out: dict[str, list[str]] = {}
    for batch in batches:
        for a in batch.get("anchors") or []:
            ck = str(a.get("claim_key") or "")
            if not ck:
                continue
            out[ck] = [str(x) for x in (a.get("neighbor_keys") or [])]
    return out


def unused_gold_for_run(intent: str, corpus: str, run_id: str) -> list[Any]:
    from apps.claims.embedding import gold as gold_mod

    served = served_probe_keys(intent, run_id)
    rows = list(gold_mod.resolved_gold(intent, corpus).values())
    return [r for r in rows if r.claim_key not in served]


def sample_triplet_batch(
    *,
    intent: str,
    corpus: str,
    model_tag: str,
    n: int,
    run_size: int | None = None,
    run_id: str | None = None,
    seed: int = 0,
    allow_keys: set[str] | None = None,
    filter_meta: dict[str, Any] | None = None,
    run_dir: Path | None = None,
    neighbor_k: int | None = None,
) -> dict[str, Any]:
    """Draw ordinary anchors + blind gold probes; attach numbered neighbors from embed run.

    Ordinary draws exclude already-labeled anchors and all gold anchors.
    Probes are injected unmarked (still with neighbors), recorded in the run ledger.
    """
    import random

    from apps.claims import claim_sample
    from apps.claims import corpus as corpus_mod
    from apps.claims import selections as sel_mod
    from apps.claims.embedding import gold as gold_mod
    from apps.claims.embedding import triplets as trip_data
    from apps.claims.embedding.triplet_neighbors import neighbors_for_claim_index

    if n < 1:
        raise ValueError("--n must be >= 1")

    gold_mod.require_gold_gate(intent, corpus)
    spec = trip_data.load_spec(intent)
    k = int(neighbor_k if neighbor_k is not None else spec.neighbor_k)
    probe_target = int(spec.probe_target)
    rsize = int(run_size or max(n, 1))
    expected_probes = max(1, round(probe_target * (n / float(rsize)))) if rsize else 0
    expected_probes = min(expected_probes, n)

    corp = corpus_mod.get_corpus(corpus)
    rdir = Path(run_dir) if run_dir is not None else corp.run_dir(model_tag)
    if not rdir.is_dir():
        raise FileNotFoundError(
            f"Missing embed run for corpus={corpus!r} model_tag={model_tag!r}: {rdir}"
        )
    vectors, index = claims_io.load_run_arrays(rdir)
    claim_texts = claims_io.claim_texts_from_index(index)
    claim_keys = sel_mod.claim_keys_from_index(index)
    if len(claim_texts) != len(vectors):
        raise ValueError(
            f"claim_texts length ({len(claim_texts)}) != vectors rows ({len(vectors)})"
        )

    key_to_idx: dict[str, int] = {}
    for i, ck in enumerate(claim_keys):
        if ck and ck not in key_to_idx:
            key_to_idx[ck] = i

    # Restrict pool to run keys ∩ allow_keys
    pool_indices = list(range(len(vectors)))
    if allow_keys is not None:
        pool_indices = [i for i in pool_indices if claim_keys[i] in allow_keys]
    if not pool_indices:
        raise ValueError(
            "No claims in embed-run pool"
            + (" after applying filter/selection" if allow_keys is not None else "")
        )

    labeled = trip_data.labeled_anchor_keys(intent)
    gkeys_all = gold_mod.gold_anchor_keys(intent)
    gkeys = gold_mod.gold_anchor_keys(intent, corpus)

    ledger = ensure_run_ledger(
        intent, corpus=corpus, run_id=run_id, model_tag=model_tag
    )
    rid = str(ledger["run_id"])
    unused = unused_gold_for_run(intent, corpus, rid)
    n_probe_pool_unfiltered = len(unused)
    # Probes must exist in the run index
    unused = [g for g in unused if g.claim_key in key_to_idx]
    if allow_keys is not None:
        unused = [g for g in unused if g.claim_key in allow_keys]
    n_probe_pool = len(unused)
    rng = random.Random(int(seed) + hash(rid) % 10_000)
    n_probes = min(expected_probes, len(unused), n)
    rng.shuffle(unused)
    probe_rows = unused[:n_probes]
    n_probes = len(probe_rows)

    exclude = labeled | gkeys_all
    n_ordinary = n - n_probes
    ordinary_indices: list[int] = []
    if n_ordinary > 0:
        candidates = [
            i
            for i in pool_indices
            if (claim_texts[i] or "").strip()
            and claim_keys[i]
            and claim_keys[i] not in exclude
        ]
        if len(candidates) <= n_ordinary:
            ordinary_indices = list(candidates)
        else:
            ordinary_indices = rng.sample(candidates, n_ordinary)

    def _neighbors_for_idx(idx: int) -> list[dict[str, Any]]:
        raw = neighbors_for_claim_index(
            idx, vectors=vectors, claim_texts=claim_texts, top_k=k
        )
        out: list[dict[str, Any]] = []
        for rank, (ni, score, text) in enumerate(raw, start=1):
            nkey = claim_keys[ni] if ni < len(claim_keys) else ""
            out.append(
                {
                    "n": rank,
                    "claim_key": nkey,
                    "text": text,
                    "score": float(score),
                    "idx": int(ni),
                }
            )
        return out

    claims: list[dict[str, Any]] = []
    batch_anchors: list[dict[str, Any]] = []

    for idx in ordinary_indices:
        ck = claim_keys[idx]
        neighbors = _neighbors_for_idx(idx)
        claims.append(
            {
                "claim_key": ck,
                "text": claim_texts[idx],
                "idx": idx,
                "neighbors": neighbors,
            }
        )
        batch_anchors.append(
            {
                "claim_key": ck,
                "neighbor_keys": [str(n["claim_key"]) for n in neighbors],
            }
        )

    for g in probe_rows:
        idx = key_to_idx[g.claim_key]
        neighbors = _neighbors_for_idx(idx)
        claims.append(
            {
                "claim_key": g.claim_key,
                "text": g.claim_text or claim_texts[idx],
                "idx": idx,
                "neighbors": neighbors,
            }
        )
        batch_anchors.append(
            {
                "claim_key": g.claim_key,
                "neighbor_keys": [str(n["claim_key"]) for n in neighbors],
            }
        )

    rng.shuffle(claims)

    if probe_rows:
        record_served_probes(intent, rid, [g.claim_key for g in probe_rows])

    batch_id = f"batch_{prov.utc_now().replace(':', '').replace('-', '')}_{secrets.token_hex(3)}"
    record_sample_batch(intent, rid, batch_id=batch_id, anchors=batch_anchors)

    warning = None
    if allow_keys is not None and n_probe_pool < expected_probes:
        warning = (
            f"Filtered probe pool ({n_probe_pool}) is below expected probes "
            f"({expected_probes}); injected {n_probes}"
        )
    if n_probe_pool_unfiltered and n_probe_pool < n_probe_pool_unfiltered:
        missing = n_probe_pool_unfiltered - n_probe_pool
        extra = f"{missing} gold anchors missing from embed run index"
        warning = f"{warning}; {extra}" if warning else extra

    return {
        "ok": True,
        "intent": intent,
        "corpus": corpus,
        "model_tag": model_tag,
        "run_dir": str(rdir.resolve()),
        "run_id": rid,
        "batch_id": batch_id,
        "neighbor_k": k,
        "n_requested": n,
        "n_returned": len(claims),
        "n_ordinary": len(ordinary_indices),
        "n_probes_injected": n_probes,
        "probe_target": probe_target,
        "run_size": rsize,
        "n_pool_ordinary": sum(
            1
            for i in pool_indices
            if (claim_texts[i] or "").strip()
            and claim_keys[i]
            and claim_keys[i] not in exclude
        ),
        "n_probe_pool": n_probe_pool,
        "n_probe_pool_unfiltered": n_probe_pool_unfiltered,
        "filter": filter_meta,
        "warning": warning,
        "claims": claims,
    }


def resolve_pos_neg_from_judgment(
    *,
    neighbor_keys: list[str],
    neighbor_texts: list[str] | None,
    pos: list[Any] | None,
    neg: list[Any] | None,
    key_to_text: dict[str, str] | None = None,
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Resolve pos/neg lists of 1-based indices and/or claim_keys to key/text lists."""

    def _resolve_side(items: list[Any] | None) -> tuple[list[str], list[str]]:
        keys: list[str] = []
        texts: list[str] = []
        for item in items or []:
            if isinstance(item, int) or (isinstance(item, str) and item.isdigit()):
                idx = int(item)
                if idx < 1 or idx > len(neighbor_keys):
                    raise ValueError(
                        f"neighbor index {idx} out of range 1..{len(neighbor_keys)}"
                    )
                k = neighbor_keys[idx - 1]
                t = (
                    neighbor_texts[idx - 1]
                    if neighbor_texts and idx - 1 < len(neighbor_texts)
                    else (key_to_text or {}).get(k, "")
                )
                keys.append(k)
                texts.append(t)
            else:
                k = str(item)
                keys.append(k)
                t = ""
                if neighbor_keys and k in neighbor_keys:
                    j = neighbor_keys.index(k)
                    if neighbor_texts and j < len(neighbor_texts):
                        t = neighbor_texts[j]
                if not t and key_to_text:
                    t = key_to_text.get(k, "")
                texts.append(t)
        return keys, texts

    pk, pt = _resolve_side(pos)
    nk, nt = _resolve_side(neg)
    return pk, pt, nk, nt
