"""Labeler train / eval / apply lifecycle (file-mode, no lab imports)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from apps.claims import annotations as ann_mod
from apps.claims import io as claims_io
from apps.claims import provenance as prov
from apps.claims.keys import claim_key
from apps.claims.labeling import registry as label_reg
from apps.claims.labeling import train as train_mod
from apps.claims.labeling.inputs import build_input_for_head
from apps.claims.labeling.predict import FieldPredictor
from apps.claims.labeling.train import binary_metrics
from apps.claims import labeling as label_data

AGENT_EVAL_MIN_PROBES = 20


def train_labeler(
    *,
    intent: str,
    dataset_version: str,
    model_version: str,
    encoder_model_id: str | None = None,
    ridge_alpha: float = 1.0,
    batch_size: int = 32,
    seed: int = 0,
    set_active: bool = False,
) -> dict[str, Any]:
    """Train Ridge on frozen train rows only; write immutable model dir."""
    spec = label_data.load_spec(intent)
    ds = label_data.load_dataset_manifest(intent, dataset_version)
    train_rows = label_data.dataset_rows(intent, dataset_version, split="train")
    if not train_rows:
        raise ValueError("Frozen dataset has no train rows")

    out_dir = label_reg.model_dir(intent, model_version)
    if out_dir.exists():
        raise FileExistsError(f"Model version already exists (immutable): {out_dir}")

    texts = [r.claim_text for r in train_rows]
    ys = [float(r.value) for r in train_rows]
    enc_id = encoder_model_id or "BAAI/bge-small-en-v1.5"

    metrics = train_mod.run_train_from_pairs(
        texts=texts,
        ys=ys,
        out_dir=out_dir,
        head_name=spec.name,
        input_var_keys=["CLAIM"],
        val_ratio=0.0,
        seed=seed,
        ridge_alpha=ridge_alpha,
        batch_size=batch_size,
        encoder_model_id=enc_id,
        value_type=spec.value_type,
        manifest=None,
    )
    manifest = label_reg.write_model_manifest(
        intent,
        model_version,
        dataset_version=dataset_version,
        dataset_hash=str(ds.get("labels_hash") or ""),
        spec_hash=str(ds.get("spec_hash") or prov.sha256_json(spec.to_dict())),
        encoder_model_id=enc_id,
        train_config={
            "ridge_alpha": ridge_alpha,
            "batch_size": batch_size,
            "seed": seed,
            "n_train": len(train_rows),
            "value_type": spec.value_type,
        },
        metrics=metrics,
    )
    if set_active:
        label_reg.set_alias(intent, model_version, alias="active")
    return {"ok": True, "path": str(out_dir), "manifest": manifest, "metrics": metrics}


def _buckets_from_spec(spec: label_data.LabelSpec) -> list[float] | None:
    if spec.value_type != "float":
        return None
    if spec.labels:
        try:
            return sorted(float(k) for k in spec.labels.keys())
        except ValueError:
            return None
    return [0.0, 0.25, 0.5, 0.75, 1.0]


def evaluate_agent_labeler(
    *,
    intent: str,
    corpus: str | None = None,
    run_id: str | None = None,
    threshold: float = 0.5,
    min_probes: int = AGENT_EVAL_MIN_PROBES,
) -> dict[str, Any]:
    """Score agent probe labels against human gold."""
    from apps.claims.labeling import gold as gold_mod
    from apps.claims.labeling import metrics as metrics_mod
    from apps.claims.labeling import probes as probes_mod

    spec = label_data.load_spec(intent)
    gold = gold_mod.resolved_gold(intent, corpus)
    if not gold:
        return {
            "ok": True,
            "intent": intent,
            "corpus": corpus,
            "run_id": run_id,
            "reportable": False,
            "reason": "no_gold",
            "n_probes": 0,
        }

    # Collect label rows whose claim_key is in gold (probes)
    pairs: list[tuple[float, float, str, str]] = []  # y_gold, y_agent, claim_key, run
    for row in label_data.load_labels(intent):
        g = gold.get(row.claim_key)
        if g is None:
            continue
        if corpus and (row.corpus or g.corpus) and str(row.corpus or g.corpus) != corpus:
            continue
        attributed = row.probe_run_id or probes_mod.attribute_probe_run(
            intent, row.claim_key, row.labeled_at, corpus=corpus or g.corpus
        )
        if run_id and attributed != run_id and row.probe_run_id != run_id:
            # Also accept if claim was served in this run
            if row.claim_key not in probes_mod.served_probe_keys(intent, run_id):
                continue
        pairs.append((float(g.value), float(row.value), row.claim_key, attributed or ""))

    # Deduplicate by claim_key: keep last agent label
    by_key: dict[str, tuple[float, float, str]] = {}
    for yg, ya, ck, rid in pairs:
        by_key[ck] = (yg, ya, rid)
    if run_id:
        by_key = {ck: v for ck, v in by_key.items() if v[2] == run_id or ck in probes_mod.served_probe_keys(intent, run_id)}

    n = len(by_key)
    if n < int(min_probes):
        return {
            "ok": True,
            "intent": intent,
            "corpus": corpus,
            "run_id": run_id,
            "reportable": False,
            "reason": "below_probe_floor",
            "n_probes": n,
            "min_probes": min_probes,
        }

    y_true = [v[0] for v in by_key.values()]
    y_pred = [v[1] for v in by_key.values()]
    metrics = metrics_mod.agreement_metrics(
        y_true,
        y_pred,
        value_type=spec.value_type,
        threshold=threshold,
        buckets=_buckets_from_spec(spec),
    )
    payload = {
        "ok": True,
        "intent": intent,
        "corpus": corpus,
        "run_id": run_id,
        "reportable": True,
        "n_probes": n,
        "metrics": metrics,
        "gold_hash": (
            gold_mod.gold_hash(intent, corpus)
            if corpus
            else {c: gold_mod.gold_hash(intent, c) for c in gold_mod.list_gold_corpora(intent)}
        ),
        "created_at": prov.utc_now(),
    }
    stamp = f"agent__{prov.safe_slug(intent)}"
    if corpus:
        stamp += f"__{prov.safe_slug(corpus)}"
    if run_id:
        stamp += f"__{prov.safe_slug(run_id)}"
    out_dir = claims_io.labeler_eval_dir() / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    claims_io.write_json(out_dir / "metrics.json", payload)
    return {**payload, "out_dir": str(out_dir)}


def evaluate_labeler(
    *,
    intent: str,
    model_ref: str,
    corpus: str | None = None,
    batch_size: int = 32,
    threshold: float = 0.5,
    experiment_name: str | None = None,
) -> dict[str, Any]:
    """Score a model against human gold (per corpus)."""
    from apps.claims.labeling import gold as gold_mod
    from apps.claims.labeling import metrics as metrics_mod

    spec = label_data.load_spec(intent)
    corpora = [corpus] if corpus else gold_mod.list_gold_corpora(intent)
    if not corpora:
        raise ValueError(f"No gold labels for intent {intent!r}")

    model_path = label_reg.resolve_model_ref(model_ref)
    pred = FieldPredictor.load(model_path, batch_size=batch_size)

    per_corpus: dict[str, Any] = {}
    warnings: list[str] = []
    for c in corpora:
        gold_rows = list(gold_mod.resolved_gold(intent, c).values())
        if not gold_rows:
            continue
        for g in gold_rows:
            if int(g.spec_version) < int(spec.version):
                warnings.append(
                    f"gold corpus={c} claim_key={g.claim_key} "
                    f"spec_version={g.spec_version} < intent spec {spec.version}"
                )
                break
        texts = [g.claim_text for g in gold_rows]
        scores = pred.predict_scores(texts)
        y_true = [float(g.value) for g in gold_rows]
        y_pred = [float(s) for s in scores]
        metrics = metrics_mod.agreement_metrics(
            y_true,
            y_pred,
            value_type=spec.value_type,
            threshold=threshold,
            buckets=_buckets_from_spec(spec),
        )
        # Keep continuous regression stats too
        import numpy as np

        yt = np.array(y_true, dtype=np.float64)
        yp = np.array(y_pred, dtype=np.float64)
        err = yp - yt
        metrics["mae"] = float(np.mean(np.abs(err)))
        metrics["rmse"] = float(np.sqrt(float(np.mean(err**2))))
        if len(yt) >= 2 and float(np.std(yt)) > 1e-12 and float(np.std(yp)) > 1e-12:
            metrics["pearson"] = float(np.corrcoef(yt, yp)[0, 1])
        else:
            metrics["pearson"] = None
        if spec.value_type == "binary":
            metrics["binary_thresholded"] = binary_metrics(yt, yp, threshold=threshold)

        per_corpus[c] = {
            "n": len(gold_rows),
            "gold_hash": gold_mod.gold_hash(intent, c),
            "gold_spec_versions": sorted({int(g.spec_version) for g in gold_rows}),
            "metrics": metrics,
            "per_example": [
                {
                    "claim_key": g.claim_key,
                    "y_true": float(g.value),
                    "y_pred": float(s),
                    "abs_err": abs(float(s) - float(g.value)),
                }
                for g, s in zip(gold_rows, scores)
            ],
        }

    stamp = experiment_name or (
        f"{prov.safe_slug(intent)}__{prov.safe_slug(Path(model_path).name)}"
        + (f"__{prov.safe_slug(corpus)}" if corpus else "")
    )
    out_dir = claims_io.labeler_eval_dir() / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "intent": intent,
        "model_path": str(model_path),
        "corpus": corpus,
        "created_at": prov.utc_now(),
        "intent_spec_version": spec.version,
        "warnings": warnings,
        "per_corpus": {
            c: {k: v for k, v in block.items() if k != "per_example"}
            for c, block in per_corpus.items()
        },
        "threshold": threshold,
    }
    claims_io.write_json(out_dir / "metrics.json", payload)
    for c, block in per_corpus.items():
        claims_io.write_jsonl(out_dir / f"predictions_{prov.safe_slug(c)}.jsonl", block["per_example"])
    return {**payload, "out_dir": str(out_dir), "n_corpora": len(per_corpus)}


def evaluate_annotation(
    *,
    corpus: str,
    annotation_name: str,
    intent: str,
    threshold: float = 0.5,
) -> dict[str, Any]:
    """Score a corpus annotation against gold and against training labels."""
    from apps.claims import corpus as corpus_mod
    from apps.claims.labeling import gold as gold_mod
    from apps.claims.labeling import metrics as metrics_mod

    spec = label_data.load_spec(intent)
    corp = corpus_mod.get_corpus(corpus)
    ann = ann_mod.read_annotation(corp.root, annotation_name)
    buckets = _buckets_from_spec(spec)

    gold_rows = list(gold_mod.resolved_gold(intent, corpus).values())
    gold_metrics = None
    if gold_rows:
        y_true = [float(g.value) for g in gold_rows]
        y_pred = []
        missing = 0
        for g in gold_rows:
            if g.claim_key not in ann.values:
                missing += 1
                continue
            y_pred.append(float(ann.values[g.claim_key]))
        # Align: only keys present in annotation
        paired_true = [float(g.value) for g in gold_rows if g.claim_key in ann.values]
        if paired_true:
            gold_metrics = metrics_mod.agreement_metrics(
                paired_true,
                y_pred,
                value_type=spec.value_type,
                threshold=threshold,
                buckets=buckets,
            )
            gold_metrics["n_gold"] = len(gold_rows)
            gold_metrics["n_scored"] = len(paired_true)
            gold_metrics["n_missing_in_annotation"] = missing
            gold_metrics["gold_hash"] = gold_mod.gold_hash(intent, corpus)

    train_metrics = None
    train_rows = [
        r
        for r in label_data.resolved_labels(intent).values()
        if (r.corpus == corpus or r.corpus is None)
        and r.claim_key not in gold_mod.gold_keys(intent)
    ]
    if train_rows:
        paired_true = []
        paired_pred = []
        missing_t = 0
        for r in train_rows:
            if r.claim_key not in ann.values:
                missing_t += 1
                continue
            paired_true.append(float(r.value))
            paired_pred.append(float(ann.values[r.claim_key]))
        if paired_true:
            train_metrics = metrics_mod.agreement_metrics(
                paired_true,
                paired_pred,
                value_type=spec.value_type,
                threshold=threshold,
                buckets=buckets,
            )
            train_metrics["n_train_labels"] = len(train_rows)
            train_metrics["n_scored"] = len(paired_true)
            train_metrics["n_missing_in_annotation"] = missing_t
            train_metrics["note"] = (
                "fit_vs_train_labels: optimistic by construction; not model accuracy"
            )

    payload = {
        "ok": True,
        "intent": intent,
        "corpus": corpus,
        "annotation": annotation_name,
        "created_at": prov.utc_now(),
        "gold_metrics": gold_metrics,
        "fit_vs_train_labels": train_metrics,
    }
    stamp = (
        f"ann__{prov.safe_slug(intent)}__{prov.safe_slug(corpus)}"
        f"__{prov.safe_slug(annotation_name)}"
    )
    out_dir = claims_io.labeler_eval_dir() / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    claims_io.write_json(out_dir / "metrics.json", payload)
    return {**payload, "out_dir": str(out_dir)}


def apply_labeler(
    *,
    corpus_root: Path,
    groups_path: Path,
    model_ref: str,
    annotation_name: str,
    batch_size: int = 32,
    force: bool = False,
    source_hash: str | None = None,
    intent: str | None = None,
    spec_version: int | None = None,
    value_type: str | None = None,
    allow_keys: set[str] | None = None,
    filter_meta: dict[str, Any] | None = None,
) -> ann_mod.Annotation:
    """Score groups; write a versioned promoted annotation (not an experiment).

    When ``allow_keys`` is set, only those claim keys are scored; the resulting
    annotation covers a subset of the corpus.
    """
    jsonl_path, meta_path = ann_mod.annotation_paths(corpus_root, annotation_name)
    if (jsonl_path.exists() or meta_path.exists()) and not force:
        raise FileExistsError(
            f"Annotation {annotation_name!r} already exists at {jsonl_path.parent}; "
            "pass force=True / --force to overwrite"
        )

    model_path = label_reg.resolve_model_ref(model_ref)
    pred = FieldPredictor.load(model_path, batch_size=batch_size)
    meta = pred._meta
    groups_payload = claims_io.read_json(groups_path)
    groups = groups_payload.get("groups") or []
    src_hash = source_hash or str(groups_payload.get("source_hash") or "") or None

    texts: list[str] = []
    keys: list[str] = []
    n_skipped = 0
    for g in groups:
        text = str(g.get("claim_text") or "")
        ck = str(g.get("claim_key") or claim_key(text))
        if allow_keys is not None and ck not in allow_keys:
            n_skipped += 1
            continue
        keys.append(ck)
        texts.append(
            build_input_for_head(
                input_var_keys=list(meta.input_var_keys or ["CLAIM"]),
                claim_text=text,
                score_field_name=meta.score_field or meta.head_name,
            )
        )
    scores = pred.predict_scores(texts) if texts else []
    values = {k: float(s) for k, s in zip(keys, scores)}

    model_hash = None
    manifest_path = model_path / "manifest.json"
    if manifest_path.is_file():
        try:
            model_hash = claims_io.read_json(manifest_path).get("model_hash")
        except Exception:  # noqa: BLE001
            pass

    params: dict[str, Any] = {
        "batch_size": batch_size,
        "n_scored": len(values),
        "n_groups_total": len(groups),
        "n_skipped": n_skipped,
    }
    if filter_meta is not None:
        params["filter"] = filter_meta

    return ann_mod.write_annotation(
        corpus_root,
        annotation_name,
        values,
        scope="group",
        producer="apps.claims.labeling.apply",
        producer_kind="model_prediction",
        model=str(model_path),
        model_id=str(model_path),
        model_hash=model_hash,
        intent=intent,
        spec_version=spec_version,
        value_type=value_type or "float",
        params=params,
        source_hash=src_hash,
        force=force,
    )


def compare_labelers(
    *,
    intent: str,
    model_refs: list[str],
    corpus: str | None = None,
    batch_size: int = 32,
) -> dict[str, Any]:
    results = []
    for ref in model_refs:
        results.append(
            evaluate_labeler(
                intent=intent,
                model_ref=ref,
                corpus=corpus,
                batch_size=batch_size,
            )
        )
    return {"intent": intent, "corpus": corpus, "results": results}
