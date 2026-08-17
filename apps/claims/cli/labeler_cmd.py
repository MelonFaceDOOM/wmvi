"""CLI: labeler intents, labels, gold, datasets, train/eval/apply/promote."""

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from apps.claims import claim_sample
from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims import labeling as label_data
from apps.claims.labeling import gold as gold_mod
from apps.claims.labeling import lifecycle as life
from apps.claims.labeling import probes as probes_mod
from apps.claims.labeling import registry as label_reg


def cmd_labeler_intent_create(args: Namespace) -> int:
    try:
        labels = None
        if args.labels_json:
            labels = json.loads(args.labels_json)
        spec = label_data.create_intent(
            str(args.name),
            instructions=str(args.instructions or ""),
            value_type=str(args.value_type),
            scope=str(args.scope),
            labels=labels,
            min_gold_total=int(args.min_gold_total),
            min_gold_per_class=int(args.min_gold_per_class),
            probe_target=int(args.probe_target),
            agent_batch_size=getattr(args, "agent_batch_size", None),
            agent_model=getattr(args, "agent_model", None),
            force=bool(args.force),
        )
        claims_io.emit_json({"ok": True, "spec": spec.to_dict()})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_intent_list(args: Namespace) -> int:
    _ = args
    claims_io.emit_json({"ok": True, "intents": label_data.list_intents()})
    return 0


def cmd_labeler_intent_show(args: Namespace) -> int:
    try:
        spec = label_data.load_spec(str(args.name))
        claims_io.emit_json({"ok": True, "spec": spec.to_dict()})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_labels_add(args: Namespace) -> int:
    try:
        spec = label_data.load_spec(str(args.intent))
        producer = {"type": str(args.producer_type or "manual")}
        if args.producer_json:
            producer.update(json.loads(args.producer_json))
        row = label_data.make_label_row(
            spec=spec,
            text=str(args.text),
            value=float(args.value),
            producer=producer,
            reason=args.reason,
            confidence=float(args.confidence) if args.confidence is not None else None,
            corpus=args.corpus,
            claim_key_override=args.claim_key,
            probe_run_id=getattr(args, "probe_run_id", None),
        )
        # Auto-tag probe_run_id if claim was served as a probe
        if row.probe_run_id is None and args.corpus:
            attributed = probes_mod.attribute_probe_run(
                str(args.intent),
                row.claim_key,
                row.labeled_at,
                corpus=str(args.corpus),
            )
            if attributed:
                row.probe_run_id = attributed
        path = label_data.append_label(row)
        claims_io.emit_json(
            {
                "ok": True,
                "row_id": row.row_id,
                "claim_key": row.claim_key,
                "value": row.value,
                "path": str(path),
                "probe_run_id": row.probe_run_id,
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_labels_import(args: Namespace) -> int:
    try:
        spec = label_data.load_spec(str(args.intent))
        src = Path(args.from_path)
        rows_in = claims_io.read_jsonl(src) if src.suffix == ".jsonl" else claims_io.read_json(src)
        if isinstance(rows_in, dict):
            if "values" in rows_in:
                rows_in = [
                    {"claim_key": k, "value": v, "claim_text": ""}
                    for k, v in dict(rows_in["values"]).items()
                ]
            else:
                rows_in = [{"claim_key": k, "value": v} for k, v in rows_in.items()]
        added = 0
        skipped = 0
        for raw in rows_in:
            text = str(raw.get("claim_text") or raw.get("text") or "")
            ck = raw.get("claim_key") or raw.get("k")
            val = raw.get("value") if "value" in raw else raw.get("v")
            if val is None:
                skipped += 1
                continue
            producer = dict(raw.get("producer") or {})
            producer.setdefault("type", str(args.producer_type or "import"))
            try:
                row = label_data.make_label_row(
                    spec=spec,
                    text=text,
                    value=float(val),
                    producer=producer,
                    reason=raw.get("reason"),
                    confidence=(
                        float(raw["confidence"]) if raw.get("confidence") is not None else None
                    ),
                    corpus=args.corpus or raw.get("corpus"),
                    claim_key_override=str(ck) if ck else None,
                    labeled_at=raw.get("labeled_at"),
                    probe_run_id=raw.get("probe_run_id"),
                )
                label_data.append_label(row)
                added += 1
            except FileExistsError:
                skipped += 1
            except Exception:  # noqa: BLE001
                skipped += 1
        claims_io.emit_json({"ok": True, "added": added, "skipped": skipped})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def _print_labels_human(rows: list[dict]) -> None:
    for i, row in enumerate(rows, start=1):
        text = " ".join(str(row.get("claim_text") or "").split())
        print(f"{i}. value={row.get('value')} key={row.get('claim_key')}: {text}")


def cmd_labeler_labels_browse(args: Namespace) -> int:
    try:
        rows = list(label_data.resolved_labels(str(args.intent)).values())
        if args.value is not None:
            rows = [r for r in rows if float(r.value) == float(args.value)]
        if getattr(args, "corpus", None):
            rows = [r for r in rows if r.corpus == args.corpus]
        limit = int(args.limit or 20)
        sample = [
            {
                "row_id": r.row_id,
                "claim_key": r.claim_key,
                "value": r.value,
                "corpus": r.corpus,
                "claim_text": r.claim_text[:200],
            }
            for r in rows[:limit]
        ]
        claims_io.emit_json(
            {
                "ok": True,
                "n_total": len(rows),
                "n_returned": len(sample),
                "rows": sample,
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    if getattr(args, "human", False):
        _print_labels_human(sample)
    return 0


def _print_claims_human(claims: list[dict]) -> None:
    for i, row in enumerate(claims, start=1):
        text = " ".join(str(row.get("text") or "").split())
        print(f"{i}. key={row.get('claim_key')}: {text}")


def cmd_labeler_gold_sample(args: Namespace) -> int:
    try:
        from apps.claims import filtering as filt

        intent = str(args.intent)
        corpus = str(args.corpus)
        n = int(args.n)
        if n < 1:
            raise ValueError("--n must be >= 1")
        label_data.load_spec(intent)  # ensure intent exists
        corp = corpus_mod.get_corpus(corpus)
        allow, filter_meta = filt.resolve_keys_for_args(args, corp.root)
        index, claim_texts, claim_keys = claim_sample.load_corpus_pool(
            corpus, allow_keys=allow
        )
        if not claim_texts or not any((t or "").strip() for t in claim_texts):
            raise ValueError(
                "No claims in pool"
                + (" after applying filter/selection" if allow is not None else "")
            )
        exclude = gold_mod.gold_keys(intent, corpus)
        indices = claim_sample.sample_claim_indices(
            claim_texts,
            n=n,
            seed=int(args.seed),
            claim_keys=claim_keys,
            exclude_keys=exclude,
        )
        claims = claim_sample.claim_rows_from_index(
            index, indices, claim_texts=claim_texts, claim_keys=claim_keys
        )
        payload = {
            "ok": True,
            "intent": intent,
            "corpus": corpus,
            "n_requested": n,
            "n_returned": len(claims),
            "n_pool": len(claim_texts),
            "n_excluded_gold": len(exclude),
            "filter": filter_meta,
            "claims": [
                {"claim_key": c.get("claim_key"), "text": c.get("text"), "idx": c.get("idx")}
                for c in claims
            ],
        }
        claims_io.emit_json(payload)
        if getattr(args, "human", False):
            _print_claims_human(payload["claims"])
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_gold_add(args: Namespace) -> int:
    try:
        spec = label_data.load_spec(str(args.intent))
        row = gold_mod.make_gold_row(
            intent=spec.name,
            spec_version=spec.version,
            text=str(args.text),
            value=float(args.value),
            corpus=str(args.corpus),
            reason=args.reason,
            claim_key_override=args.claim_key,
        )
        path = gold_mod.append_gold(row)
        claims_io.emit_json(
            {
                "ok": True,
                "claim_key": row.claim_key,
                "value": row.value,
                "corpus": row.corpus,
                "path": str(path),
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_gold_import(args: Namespace) -> int:
    try:
        spec = label_data.load_spec(str(args.intent))
        src = Path(args.from_path)
        rows_in = claims_io.read_jsonl(src) if src.suffix == ".jsonl" else claims_io.read_json(src)
        if not isinstance(rows_in, list):
            raise ValueError("gold-import expects a jsonl list or JSON array")
        added = 0
        skipped = 0
        for raw in rows_in:
            text = str(raw.get("claim_text") or raw.get("text") or "")
            ck = raw.get("claim_key") or raw.get("k")
            val = raw.get("value") if "value" in raw else raw.get("v")
            if val is None:
                skipped += 1
                continue
            try:
                row = gold_mod.make_gold_row(
                    intent=spec.name,
                    spec_version=int(raw.get("spec_version") or spec.version),
                    text=text,
                    value=float(val),
                    corpus=str(args.corpus or raw.get("corpus")),
                    reason=raw.get("reason"),
                    claim_key_override=str(ck) if ck else None,
                    labeled_at=raw.get("labeled_at"),
                    sampling=str(raw.get("sampling") or "random"),
                )
                gold_mod.append_gold(row)
                added += 1
            except FileExistsError:
                skipped += 1
            except Exception:  # noqa: BLE001
                skipped += 1
        claims_io.emit_json({"ok": True, "added": added, "skipped": skipped})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_gold_status(args: Namespace) -> int:
    try:
        status = gold_mod.gold_status(str(args.intent), args.corpus)
        claims_io.emit_json({"ok": True, **status})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_gold_label(args: Namespace) -> int:
    """Interactive loop: sample n claims, prompt human for gold values, write gold rows."""
    import secrets
    import sys

    from apps.claims import filtering as filt

    try:
        intent = str(args.intent)
        corpus = str(args.corpus)
        n = int(args.n)
        if n < 1:
            raise ValueError("--n must be >= 1")
        seed = int(args.seed) if args.seed is not None else secrets.randbelow(2**31)
        spec = label_data.load_spec(intent)
        hint = gold_mod.expected_input_hint(spec)

        corp = corpus_mod.get_corpus(corpus)
        allow, filter_meta = filt.resolve_keys_for_args(args, corp.root)
        index, claim_texts, claim_keys = claim_sample.load_corpus_pool(
            corpus, allow_keys=allow
        )
        if not claim_texts or not any((t or "").strip() for t in claim_texts):
            raise ValueError(
                "No claims in pool"
                + (" after applying filter/selection" if allow is not None else "")
            )
        exclude = gold_mod.gold_keys(intent, corpus)
        indices = claim_sample.sample_claim_indices(
            claim_texts,
            n=n,
            seed=seed,
            claim_keys=claim_keys,
            exclude_keys=exclude,
        )
        claims = claim_sample.claim_rows_from_index(
            index, indices, claim_texts=claim_texts, claim_keys=claim_keys
        )
        sampling = filt.sampling_descriptor(filter_meta)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1

    status0 = gold_mod.gold_status(intent, corpus)
    corp_status = (status0.get("corpora") or {}).get(corpus) or {}
    print(f"Gold labeling: intent={intent}  corpus={corpus}  n={len(claims)}  seed={seed}")
    if filter_meta:
        print(f"Filter: {sampling}  n_pool={len(claim_texts)}")
    print(f"Expected input: {hint}")
    print("Commands: value to label | s=skip | q=quit | ?=show rubric snippet")
    print(
        f"Current gold: n={corp_status.get('n_total', 0)} "
        f"per_class={corp_status.get('per_class', {})} "
        f"gate_ok={corp_status.get('gate_ok', False)}"
    )
    print()

    added = 0
    skipped = 0
    for i, claim in enumerate(claims, start=1):
        ck = str(claim.get("claim_key") or "")
        text = str(claim.get("text") or "")
        print("-" * 60)
        print(f"[{i}/{len(claims)}] claim_key={ck}")
        print(text)
        print()
        while True:
            try:
                raw = input(f"value ({hint}) > ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nStopped.")
                print(f"Saved {added}, skipped {skipped}, remaining {len(claims) - i + 1}.")
                return 0 if added or skipped else 1
            low = raw.lower()
            if low in ("q", "quit", "exit"):
                print(f"Quit. Saved {added}, skipped {skipped}.")
                return 0
            if low in ("s", "skip"):
                skipped += 1
                print("skipped")
                break
            if low in ("?", "help"):
                instr = (spec.instructions or "").strip()
                snippet = instr[:800] + ("…" if len(instr) > 800 else "")
                print(snippet or "(no instructions on spec)")
                print(f"Expected input: {hint}")
                continue
            if not raw:
                continue
            try:
                value = float(raw)
                # For discrete labeled floats, require an exact bucket key
                if spec.value_type != "binary" and spec.labels:
                    allowed = {float(k) for k in spec.labels.keys()}
                    if not any(abs(value - a) < 1e-12 for a in allowed):
                        raise ValueError(
                            f"value must be one of {sorted(allowed)}; got {value}"
                        )
                row = gold_mod.make_gold_row(
                    intent=spec.name,
                    spec_version=spec.version,
                    text=text,
                    value=value,
                    corpus=corpus,
                    claim_key_override=ck or None,
                    sampling=sampling,
                )
                gold_mod.append_gold(row)
                added += 1
                print(f"saved value={row.value}")
                break
            except FileExistsError:
                print("already in gold; skipping")
                skipped += 1
                break
            except Exception as exc:  # noqa: BLE001
                print(f"invalid: {exc}")

    status1 = gold_mod.gold_status(intent, corpus)
    corp1 = (status1.get("corpora") or {}).get(corpus) or {}
    print("-" * 60)
    print(f"Done. saved={added} skipped={skipped}")
    print(
        f"Gold now: n={corp1.get('n_total', 0)} "
        f"per_class={corp1.get('per_class', {})} "
        f"gate_ok={corp1.get('gate_ok', False)}"
    )
    return 0


def cmd_labeler_sample(args: Namespace) -> int:
    try:
        from apps.claims import filtering as filt

        corp = corpus_mod.get_corpus(str(args.corpus))
        allow, filter_meta = filt.resolve_keys_for_args(args, corp.root)
        payload = probes_mod.sample_labeling_batch(
            intent=str(args.intent),
            corpus=str(args.corpus),
            n=int(args.n),
            run_size=int(args.run_size) if args.run_size is not None else None,
            run_id=args.run_id,
            seed=int(args.seed),
            allow_keys=allow,
            filter_meta=filter_meta,
        )
        claims_io.emit_json(payload)
        if getattr(args, "human", False):
            _print_claims_human(payload.get("claims") or [])
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_dataset_freeze(args: Namespace) -> int:
    try:
        manifest = label_data.freeze_dataset(
            str(args.intent),
            str(args.version),
            force=bool(args.force),
        )
        claims_io.emit_json({"ok": True, "manifest": manifest})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_train(args: Namespace) -> int:
    try:
        payload = life.train_labeler(
            intent=str(args.intent),
            dataset_version=str(args.dataset),
            model_version=str(args.version),
            encoder_model_id=args.encoder,
            ridge_alpha=float(args.ridge_alpha),
            batch_size=int(args.batch_size),
            seed=int(args.seed),
            set_active=bool(args.set_active),
        )
        claims_io.emit_json(payload)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_eval(args: Namespace) -> int:
    try:
        payload = life.evaluate_labeler(
            intent=str(args.intent),
            model_ref=str(args.model),
            corpus=args.corpus,
            batch_size=int(args.batch_size),
            threshold=float(args.threshold),
            experiment_name=args.name,
        )
        claims_io.emit_json({"ok": True, **payload})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_agent_eval(args: Namespace) -> int:
    try:
        payload = life.evaluate_agent_labeler(
            intent=str(args.intent),
            corpus=args.corpus,
            run_id=args.run_id,
            threshold=float(args.threshold),
        )
        claims_io.emit_json(payload)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    if getattr(args, "human", False):
        _print_agent_eval_human(payload)
    return 0


def _print_agent_eval_human(payload: dict) -> None:
    print(
        f"agent-eval  intent={payload.get('intent')}  "
        f"corpus={payload.get('corpus')}  run_id={payload.get('run_id')}"
    )
    if not payload.get("reportable"):
        print(
            f"not reportable: {payload.get('reason')}  "
            f"n_probes={payload.get('n_probes')}  "
            f"min_probes={payload.get('min_probes')}"
        )
        return
    print(f"n_probes={payload.get('n_probes')}")
    metrics = payload.get("metrics")
    if metrics:
        _print_metrics_block(metrics, indent="  ")
    if payload.get("out_dir"):
        print(f"out_dir: {payload['out_dir']}")


def _print_annotation_eval_human(payload: dict) -> None:
    print(
        f"annotation-eval  intent={payload.get('intent')}  "
        f"corpus={payload.get('corpus')}  annotation={payload.get('annotation')}"
    )
    gold = payload.get("gold_metrics")
    train = payload.get("fit_vs_train_labels")
    if gold is None:
        print("gold_metrics: (none — no gold or no overlap)")
    else:
        print("gold_metrics:")
        _print_metrics_block(gold, indent="  ")
    if train is None:
        print("fit_vs_train_labels: (none — no train overlap)")
    else:
        print("fit_vs_train_labels (optimistic):")
        _print_metrics_block(train, indent="  ")
    if payload.get("out_dir"):
        print(f"out_dir: {payload['out_dir']}")


def _print_metrics_block(m: dict, *, indent: str = "") -> None:
    vt = m.get("value_type")
    n = m.get("n") or m.get("n_scored")
    bits = [f"n={n}"]
    if m.get("n_gold") is not None:
        bits.append(f"n_gold={m['n_gold']}")
    if m.get("n_missing_in_annotation") is not None:
        bits.append(f"missing={m['n_missing_in_annotation']}")
    if m.get("accuracy") is not None:
        bits.append(f"accuracy={m['accuracy']:.4f}")
    if m.get("kappa") is not None:
        bits.append(f"kappa={m['kappa']:.4f}")
    if m.get("exact_agreement") is not None:
        bits.append(f"exact={m['exact_agreement']:.4f}")
    if m.get("adjacent_agreement") is not None:
        bits.append(f"adjacent={m['adjacent_agreement']:.4f}")
    if m.get("mae") is not None:
        bits.append(f"mae={m['mae']:.4f}")
    print(f"{indent}[{vt}] " + "  ".join(bits))
    per = m.get("per_class") or {}
    if per:
        for cls, stats in sorted(per.items(), key=lambda kv: str(kv[0])):
            print(
                f"{indent}  class {cls}: "
                f"p={float(stats.get('precision', 0)):.3f} "
                f"r={float(stats.get('recall', 0)):.3f} "
                f"f1={float(stats.get('f1', 0)):.3f} "
                f"support={stats.get('support')}"
            )


def cmd_labeler_annotation_eval(args: Namespace) -> int:
    try:
        payload = life.evaluate_annotation(
            corpus=str(args.corpus),
            annotation_name=str(args.name),
            intent=str(args.intent),
            threshold=float(args.threshold),
        )
        claims_io.emit_json(payload)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    if getattr(args, "human", False):
        _print_annotation_eval_human(payload)
    return 0


def cmd_labeler_promote(args: Namespace) -> int:
    try:
        payload = label_reg.set_alias(
            str(args.intent),
            str(args.version),
            alias=str(args.alias or "active"),
            force=True,
        )
        claims_io.emit_json({"ok": True, **payload})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_apply(args: Namespace) -> int:
    try:
        from apps.claims import filtering as filt
        from apps.claims.cli import paths as path_helpers

        corpus = path_helpers.require_corpus(args)
        if not corpus.groups.is_file():
            raise FileNotFoundError(f"Missing groups.json at {corpus.groups}")
        allow, filter_meta = filt.resolve_keys_for_args(args, corpus.root)
        ann = life.apply_labeler(
            corpus_root=corpus.root,
            groups_path=corpus.groups,
            model_ref=str(args.model),
            annotation_name=str(args.name),
            batch_size=int(args.batch_size),
            force=bool(args.force),
            intent=args.intent,
            value_type=args.value_type,
            allow_keys=allow,
            filter_meta=filter_meta,
        )
        n_scored = int((ann.meta.params or {}).get("n_scored") or ann.meta.count)
        n_skipped = int((ann.meta.params or {}).get("n_skipped") or 0)
        claims_io.emit_json(
            {
                "ok": True,
                "annotation": ann.name,
                "count": ann.meta.count,
                "n_scored": n_scored,
                "n_skipped": n_skipped,
                "filter": filter_meta,
                "model": ann.meta.model,
                "path": str(corpus.annotations / f"{ann.name}.jsonl"),
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_labeler_models_list(args: Namespace) -> int:
    try:
        claims_io.emit_json({"ok": True, "versions": label_reg.list_versions(str(args.intent))})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0
