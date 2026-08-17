"""Blind probe injection ledgers for agentic labeling runs."""

from __future__ import annotations

import secrets
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims import provenance as prov


def runs_dir(intent: str) -> Path:
    from apps.claims import labeling as label_data

    return label_data.intent_dir(intent) / "runs"


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
) -> dict[str, Any]:
    rid = run_id or mint_run_id()
    existing = load_run_ledger(intent, rid)
    if existing is not None:
        return existing
    ledger = {
        "run_id": rid,
        "intent": intent,
        "corpus": corpus,
        "created_at": prov.utc_now(),
        "served_probes": [],  # [{claim_key, served_at}]
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


def unused_gold_for_run(intent: str, corpus: str, run_id: str) -> list[Any]:
    """Gold rows for corpus not yet served in this run."""
    from apps.claims.labeling import gold as gold_mod

    served = served_probe_keys(intent, run_id)
    rows = list(gold_mod.resolved_gold(intent, corpus).values())
    return [r for r in rows if r.claim_key not in served]


def pick_stratified_probes(
    unused: list[Any],
    n: int,
    *,
    value_type: str,
    labels: dict[str, str] | None,
    rng: Any,
) -> list[Any]:
    """Pick up to ``n`` unused gold rows, oversampling rarer value classes.

    Round-robin from rarest class first so small probe budgets still hit
    minority buckets when any remain unused.
    """
    from apps.claims.labeling import gold as gold_mod

    if n < 1 or not unused:
        return []
    by_class: dict[str, list[Any]] = {}
    for row in unused:
        ck = gold_mod._class_key(
            float(row.value),
            value_type=value_type,
            labels=labels or None,
        )
        by_class.setdefault(ck, []).append(row)
    for rows in by_class.values():
        rng.shuffle(rows)
    # Rarest first; tie-break by class id for stability
    order = sorted(by_class.keys(), key=lambda c: (len(by_class[c]), c))
    selected: list[Any] = []
    while len(selected) < n:
        progressed = False
        for cls in order:
            pool = by_class.get(cls) or []
            if not pool:
                continue
            selected.append(pool.pop())
            progressed = True
            if len(selected) >= n:
                break
        if not progressed:
            break
    rng.shuffle(selected)  # don't leak class order into the batch
    return selected


def list_run_ledgers(intent: str) -> list[dict[str, Any]]:
    root = runs_dir(intent)
    if not root.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(root.glob("*.json")):
        try:
            out.append(claims_io.read_json(p))
        except Exception:  # noqa: BLE001
            continue
    return out


def attribute_probe_run(
    intent: str,
    claim_key: str,
    labeled_at: str,
    *,
    corpus: str | None = None,
) -> str | None:
    """Match a label row to the most recent ledger serve at or before labeled_at."""
    best: tuple[str, str] | None = None  # (served_at, run_id)
    for ledger in list_run_ledgers(intent):
        if corpus and str(ledger.get("corpus") or "") != corpus:
            continue
        for p in ledger.get("served_probes") or []:
            if str(p.get("claim_key")) != claim_key:
                continue
            served_at = str(p.get("served_at") or "")
            if not served_at or served_at > labeled_at:
                continue
            if best is None or served_at >= best[0]:
                best = (served_at, str(ledger["run_id"]))
    return best[1] if best else None


def sample_labeling_batch(
    *,
    intent: str,
    corpus: str,
    n: int,
    run_size: int | None = None,
    run_id: str | None = None,
    seed: int = 0,
    allow_keys: set[str] | None = None,
    filter_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Draw ordinary claims + blind gold probes for a labeling batch.

    Ordinary draws exclude already-labeled keys and all gold keys.
    Probes are injected unmarked, recorded in the run ledger.
    When ``allow_keys`` is set, both ordinary draws and probes are restricted
    to that key set.
    """
    import random

    from apps.claims import claim_sample
    from apps.claims import labeling as label_data
    from apps.claims.labeling import gold as gold_mod

    if n < 1:
        raise ValueError("--n must be >= 1")

    gold_mod.require_gold_gate(intent, corpus)
    spec = label_data.load_spec(intent)
    probe_target = int(spec.probe_target)
    rsize = int(run_size or max(n, 1))
    # Expected probes in this batch proportional to batch/run_size of target
    expected_probes = max(1, round(probe_target * (n / float(rsize)))) if rsize else 0
    expected_probes = min(expected_probes, n)

    index, claim_texts, claim_keys = claim_sample.load_corpus_pool(
        corpus, allow_keys=allow_keys
    )
    if not claim_texts or not any((t or "").strip() for t in claim_texts):
        raise ValueError(
            "No claims in pool"
            + (" after applying filter/selection" if allow_keys is not None else "")
        )

    labeled = set(label_data.resolved_labels(intent).keys())
    gkeys = gold_mod.gold_keys(intent, corpus)
    # Also exclude gold from other corpora for ordinary draws (union model hygiene)
    gkeys_all = gold_mod.gold_keys(intent)

    ledger = ensure_run_ledger(intent, corpus=corpus, run_id=run_id)
    rid = str(ledger["run_id"])
    unused = unused_gold_for_run(intent, corpus, rid)
    n_probe_pool_unfiltered = len(unused)
    if allow_keys is not None:
        unused = [g for g in unused if g.claim_key in allow_keys]
    n_probe_pool = len(unused)
    rng = random.Random(int(seed) + hash(rid) % 10_000)
    n_probes = min(expected_probes, len(unused), n)
    probe_rows = pick_stratified_probes(
        unused,
        n_probes,
        value_type=spec.value_type,
        labels=dict(spec.labels or {}),
        rng=rng,
    )
    n_probes = len(probe_rows)

    exclude = labeled | gkeys_all
    n_ordinary = n - n_probes
    ordinary: list[dict[str, Any]] = []
    if n_ordinary > 0:
        ordinary_indices = claim_sample.sample_claim_indices(
            claim_texts,
            n=n_ordinary,
            seed=int(seed),
            claim_keys=claim_keys,
            exclude_keys=exclude,
        )
        ordinary = claim_sample.claim_rows_from_index(
            index,
            ordinary_indices,
            claim_texts=claim_texts,
            claim_keys=claim_keys,
        )

    claims: list[dict[str, Any]] = []
    for row in ordinary:
        claims.append(
            {
                "claim_key": row.get("claim_key"),
                "text": row.get("text"),
                "idx": row.get("idx"),
            }
        )
    for g in probe_rows:
        claims.append(
            {
                "claim_key": g.claim_key,
                "text": g.claim_text,
                "idx": None,
            }
        )
    rng.shuffle(claims)

    if probe_rows:
        record_served_probes(intent, rid, [g.claim_key for g in probe_rows])

    warning = None
    if allow_keys is not None and n_probe_pool < expected_probes:
        warning = (
            f"Filtered probe pool ({n_probe_pool}) is below expected probes "
            f"({expected_probes}); injected {n_probes}"
        )

    return {
        "ok": True,
        "intent": intent,
        "corpus": corpus,
        "run_id": rid,
        "n_requested": n,
        "n_returned": len(claims),
        "n_ordinary": len(ordinary),
        "n_probes_injected": n_probes,
        "probe_target": probe_target,
        "run_size": rsize,
        "n_pool_ordinary": sum(
            1
            for i, t in enumerate(claim_texts)
            if (t or "").strip() and (not claim_keys or claim_keys[i] not in exclude)
        ),
        "n_probe_pool": n_probe_pool,
        "n_probe_pool_unfiltered": n_probe_pool_unfiltered,
        "filter": filter_meta,
        "warning": warning,
        "claims": claims,
    }
