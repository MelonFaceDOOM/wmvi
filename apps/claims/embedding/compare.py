"""Agent-eval and train-compare helpers for similarity intents."""

from __future__ import annotations

from typing import Any


def set_agreement(pred: set[str], gold: set[str]) -> dict[str, float]:
    """Precision / recall / Jaccard for a single set prediction vs gold."""
    pred = {str(x) for x in pred if x}
    gold = {str(x) for x in gold if x}
    if not pred and not gold:
        return {"precision": 1.0, "recall": 1.0, "jaccard": 1.0, "n_pred": 0, "n_gold": 0, "n_tp": 0}
    tp = pred & gold
    precision = len(tp) / len(pred) if pred else 0.0
    recall = len(tp) / len(gold) if gold else 0.0
    union = pred | gold
    jaccard = len(tp) / len(union) if union else 1.0
    return {
        "precision": float(precision),
        "recall": float(recall),
        "jaccard": float(jaccard),
        "n_pred": len(pred),
        "n_gold": len(gold),
        "n_tp": len(tp),
    }


def mean_metric(rows: list[dict[str, float]], key: str) -> float | None:
    vals = [float(r[key]) for r in rows if key in r]
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def evaluate_agent_triplets(
    *,
    intent: str,
    corpus: str,
    run_id: str | None = None,
    min_probes: int = 1,
) -> dict[str, Any]:
    """Compare agent training triplets on probe anchors against gold."""
    from apps.claims import provenance as prov
    from apps.claims.embedding import gold as gold_mod
    from apps.claims.embedding import probes as probes_mod
    from apps.claims.embedding import triplets as trip_data

    gold = gold_mod.resolved_gold(intent, corpus)
    if not gold:
        raise ValueError(f"No gold for intent={intent!r} corpus={corpus!r}")

    if run_id:
        served = probes_mod.served_probe_keys(intent, run_id)
    else:
        served = set()
        for ledger in _list_ledgers(intent, corpus=corpus):
            for p in ledger.get("served_probes") or []:
                served.add(str(p["claim_key"]))

    probe_keys = sorted(k for k in served if k in gold)
    if len(probe_keys) < min_probes:
        return {
            "ok": True,
            "intent": intent,
            "corpus": corpus,
            "run_id": run_id,
            "reportable": False,
            "n_probes": len(probe_keys),
            "min_probes": min_probes,
            "metrics": None,
            "created_at": prov.utc_now(),
        }

    # Latest agent triplet per anchor among training log
    by_anchor: dict[str, Any] = {}
    for row in trip_data.load_triplets(intent):
        if row.corpus and row.corpus != corpus:
            continue
        if row.anchor_key not in probe_keys:
            continue
        if run_id and row.probe_run_id and row.probe_run_id != run_id:
            continue
        by_anchor[row.anchor_key] = row

    per_anchor: list[dict[str, Any]] = []
    pos_rows: list[dict[str, float]] = []
    neg_rows: list[dict[str, float]] = []
    missing = 0
    for ck in probe_keys:
        g = gold[ck]
        pred = by_anchor.get(ck)
        if pred is None:
            missing += 1
            continue
        pos_m = set_agreement(set(pred.positive_keys), set(g.positive_keys))
        neg_m = set_agreement(set(pred.negative_keys), set(g.negative_keys))
        pos_rows.append(pos_m)
        neg_rows.append(neg_m)
        per_anchor.append(
            {
                "claim_key": ck,
                "pos": pos_m,
                "neg": neg_m,
                "n_pred_pos": len(pred.positive_keys),
                "n_pred_neg": len(pred.negative_keys),
                "n_gold_pos": len(g.positive_keys),
                "n_gold_neg": len(g.negative_keys),
            }
        )

    metrics = {
        "n_probes": len(probe_keys),
        "n_labeled": len(per_anchor),
        "n_missing": missing,
        "pos_precision": mean_metric(pos_rows, "precision"),
        "pos_recall": mean_metric(pos_rows, "recall"),
        "pos_jaccard": mean_metric(pos_rows, "jaccard"),
        "neg_precision": mean_metric(neg_rows, "precision"),
        "neg_recall": mean_metric(neg_rows, "recall"),
        "neg_jaccard": mean_metric(neg_rows, "jaccard"),
        "mean_jaccard": None,
    }
    pj = metrics["pos_jaccard"]
    nj = metrics["neg_jaccard"]
    if pj is not None and nj is not None:
        metrics["mean_jaccard"] = float((pj + nj) / 2.0)
    elif pj is not None:
        metrics["mean_jaccard"] = float(pj)
    elif nj is not None:
        metrics["mean_jaccard"] = float(nj)

    return {
        "ok": True,
        "intent": intent,
        "corpus": corpus,
        "run_id": run_id,
        "reportable": True,
        "n_probes": len(probe_keys),
        "metrics": metrics,
        "per_anchor": per_anchor,
        "gold_hash": gold_mod.gold_hash(intent, corpus),
        "created_at": prov.utc_now(),
    }


def _list_ledgers(intent: str, *, corpus: str | None = None) -> list[dict[str, Any]]:
    from apps.claims.embedding import probes as probes_mod

    out: list[dict[str, Any]] = []
    root = probes_mod.runs_dir(intent)
    if not root.is_dir():
        return out
    for p in sorted(root.glob("*.json")):
        try:
            ledger = claims_io_read(p)
        except Exception:  # noqa: BLE001
            continue
        if corpus and str(ledger.get("corpus") or "") != corpus:
            continue
        out.append(ledger)
    return out


def claims_io_read(path: Any) -> dict[str, Any]:
    from apps.claims import io as claims_io

    return claims_io.read_json(path)


def pick_train_compare_winner(
    mnrl_metrics: dict[str, Any],
    triplet_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Choose winner by gold pairwise pass rate; tie-break mean margin then fewer empty.

    Expected metric keys (from eval_triplets.aggregate_scores overall + extras):
      pass_pct / overall_pass_pct
      mean_margin (optional)
      n_empty_eval (optional) — anchors skipped for lack of pos+neg
    """

    def _pass(m: dict[str, Any]) -> float:
        for k in ("pass_pct", "overall_pass_pct", "pairwise_pass_pct"):
            if m.get(k) is not None:
                return float(m[k])
        return 0.0

    def _margin(m: dict[str, Any]) -> float:
        for k in ("mean_margin", "avg_margin"):
            if m.get(k) is not None:
                return float(m[k])
        return 0.0

    def _empty(m: dict[str, Any]) -> int:
        for k in ("n_empty_eval", "n_skipped", "n_empty"):
            if m.get(k) is not None:
                return int(m[k])
        return 0

    a_pass, b_pass = _pass(mnrl_metrics), _pass(triplet_metrics)
    a_margin, b_margin = _margin(mnrl_metrics), _margin(triplet_metrics)
    a_empty, b_empty = _empty(mnrl_metrics), _empty(triplet_metrics)

    if a_pass > b_pass:
        winner = "mnrl"
    elif b_pass > a_pass:
        winner = "triplet"
    elif a_margin > b_margin:
        winner = "mnrl"
    elif b_margin > a_margin:
        winner = "triplet"
    elif a_empty < b_empty:
        winner = "mnrl"
    elif b_empty < a_empty:
        winner = "triplet"
    else:
        winner = "mnrl"  # stable default

    return {
        "winner": winner,
        "winner_loss": (
            "MultipleNegativesRankingLoss" if winner == "mnrl" else "TripletLoss"
        ),
        "mnrl_pass_pct": a_pass,
        "triplet_pass_pct": b_pass,
        "mnrl_mean_margin": a_margin,
        "triplet_mean_margin": b_margin,
        "mnrl_n_empty": a_empty,
        "triplet_n_empty": b_empty,
    }


def gold_pairwise_metrics(
    *,
    intent: str,
    corpus: str,
    model_id: str,
    doc_instruction: str = "",
    dtype: str = "auto",
    max_seq_length: int | None = None,
    device: str | None = None,
) -> dict[str, Any]:
    """Score gold anchors with pos+neg under a model (pairwise pass rate)."""
    import numpy as np

    from apps.claims.embedding import eval_triplets as eval_mod
    from apps.claims.embedding import gold as gold_mod
    from apps.claims.embedding.encode import (
        DEFAULT_MAX_SEQ_LENGTH,
        encode_texts,
        load_sentence_transformer,
    )
    from apps.claims.types import TripletAnchor

    gold_rows = list(gold_mod.resolved_gold(intent, corpus).values())
    usable = [r for r in gold_rows if r.positive_keys and r.negative_keys]
    empty = len(gold_rows) - len(usable)
    if not usable:
        return {
            "pass_pct": 0.0,
            "n_gold": len(gold_rows),
            "n_scored": 0,
            "n_empty_eval": empty,
            "mean_margin": 0.0,
            "pairs_total": 0,
            "pairs_pass": 0,
        }

    model = load_sentence_transformer(
        model_id,
        device=device,
        dtype=dtype,
        max_seq_length=max_seq_length if max_seq_length is not None else DEFAULT_MAX_SEQ_LENGTH,
    )
    prompt = (doc_instruction or "").strip() or None

    def embed_fn(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, 0), dtype=np.float32)
        return encode_texts(model, texts, normalize_embeddings=True, prompt=prompt)

    anchors: list[TripletAnchor] = []
    scores: dict[int, Any] = {}
    margins: list[float] = []
    for i, r in enumerate(usable):
        pos_texts = [t for t in r.positive_texts if (t or "").strip()]
        neg_texts = [t for t in r.negative_texts if (t or "").strip()]
        if not pos_texts or not neg_texts:
            empty += 1
            continue
        a = TripletAnchor(
            id=i,
            text=r.claim_text,
            positives=pos_texts,
            negatives=neg_texts,
            pool="gold",
            too_hard=False,
            category="",
            family="",
        )
        anchors.append(a)
        sc = eval_mod.score_anchor(
            a.text,
            a.positives or [],
            a.negatives or [],
            embed_fn=embed_fn,
        )
        scores[i] = sc
        # Mean positive similarity minus mean negative similarity
        all_texts = [a.text] + pos_texts + neg_texts
        vecs = np.asarray(embed_fn(all_texts), dtype=np.float32)
        if len(vecs) == len(all_texts):
            av = vecs[0]
            pos_s = [float(np.dot(av, vecs[j + 1])) for j in range(len(pos_texts))]
            neg_s = [
                float(np.dot(av, vecs[1 + len(pos_texts) + j]))
                for j in range(len(neg_texts))
            ]
            if pos_s and neg_s:
                margins.append(float(sum(pos_s) / len(pos_s) - sum(neg_s) / len(neg_s)))

    overall, _ = eval_mod.aggregate_scores(anchors, scores)
    return {
        "pass_pct": float(overall),
        "n_gold": len(gold_rows),
        "n_scored": len(anchors),
        "n_empty_eval": empty,
        "mean_margin": float(sum(margins) / len(margins)) if margins else 0.0,
        "pairs_total": int(sum(getattr(s, "pairs_total", 0) for s in scores.values())),
        "pairs_pass": int(sum(getattr(s, "pairs_pass", 0) for s in scores.values())),
    }
