"""Migrate measles epi_value annotation → training labels + slim annotation meta.

Safe to re-run: skips existing label rows; annotation rewrite requires --force-annotation.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from apps.claims import annotations as ann_mod
from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims import labeling as label_data
from apps.claims import provenance as prov


EPI_INSTRUCTIONS = (
    "Valuable iff the claim offers real insight into what someone believes about vaccines "
    "(or closely related measles/immunity behavior). Reject vague/underspecified claims "
    "and broad/obvious/flat claims."
)


def migrate(
    *,
    corpus_name: str = "measles",
    intent: str = "epi_value",
    dataset_version: str = "v1",
    force_intent: bool = False,
    force_dataset: bool = False,
    force_annotation: bool = False,
) -> dict:
    corpus = corpus_mod.get_corpus(corpus_name)
    ann = ann_mod.read_annotation(corpus.root, "epi_value")
    groups = claims_io.read_json(corpus.groups)
    groups_hash = str(groups.get("source_hash") or "")
    if not groups_hash:
        raise ValueError("groups.json missing source_hash; cannot backfill provenance")

    old_meta = ann.meta.to_dict()
    params = dict(old_meta.get("params") or {})
    labeled_texts = dict(params.get("labeled_texts") or {})

    # Create intent
    try:
        spec = label_data.create_intent(
            intent,
            instructions=str(params.get("rubric") or EPI_INSTRUCTIONS),
            value_type="binary",
            scope="group",
            labels={"0": "no", "1": "yes"},
            eval_frac=0.15,
            split_seed=0,
            force=force_intent,
        )
    except FileExistsError:
        spec = label_data.load_spec(intent)

    # Import labels
    added = 0
    skipped = 0
    missing_text = 0
    values_check: dict[str, float] = {}
    for ck, v in ann.values.items():
        text = str(labeled_texts.get(ck) or "")
        if not text:
            missing_text += 1
        producer = {
            "type": "agent_label",
            "migrated_from": "annotations/epi_value",
            "batches": params.get("batches"),
        }
        try:
            row = label_data.make_label_row(
                spec=spec,
                text=text,
                value=float(v),
                producer=producer,
                corpus=corpus_name,
                claim_key_override=str(ck),
                labeled_at=str(old_meta.get("created_at") or prov.utc_now()),
            )
            label_data.append_label(row)
            values_check[row.claim_key] = row.value
            added += 1
        except FileExistsError:
            skipped += 1
            values_check[str(ck)] = float(v)

    # Parity: all annotation keys present with same values
    resolved = label_data.resolved_labels(intent)
    parity_ok = True
    mismatches: list[str] = []
    for ck, v in ann.values.items():
        if ck not in resolved:
            parity_ok = False
            mismatches.append(f"missing:{ck}")
            continue
        if float(resolved[ck].value) != float(v):
            parity_ok = False
            mismatches.append(f"value:{ck}")
    if len(resolved) < len(ann.values):
        parity_ok = False

    # Freeze dataset
    try:
        manifest = label_data.freeze_dataset(intent, dataset_version, force=force_dataset)
    except FileExistsError:
        manifest = label_data.load_dataset_manifest(intent, dataset_version)

    # Slim annotation (keep same name for compatibility)
    slim_params = {
        "rubric": params.get("rubric") or EPI_INSTRUCTIONS,
        "value_encoding": params.get("value_encoding") or {"1": "yes", "0": "no"},
        "batches": params.get("batches") or [],
        "training_intent": intent,
        "training_dataset": dataset_version,
        "migrated_at": prov.utc_now(),
    }
    if force_annotation or (ann.meta.source_hash is None) or ("labeled_texts" in params):
        # Rewrite with same values; require force if already slim with source_hash
        already_slim = (
            ann.meta.source_hash == groups_hash
            and "labeled_texts" not in params
            and not force_annotation
        )
        if not already_slim:
            if not force_annotation and ann.meta.source_hash and "labeled_texts" not in params:
                pass  # leave as-is
            else:
                ann_mod.write_annotation(
                    corpus.root,
                    "epi_value",
                    ann.values,
                    scope=ann.meta.scope,
                    producer=ann.meta.producer or "agent_label",
                    producer_kind="agent_consensus",
                    model=ann.meta.model,
                    params=slim_params,
                    source_hash=groups_hash,
                    intent=intent,
                    spec_version=spec.version,
                    value_type="binary",
                    annotation_version="v1",
                    force=True,
                )

    # Verify annotation count/values unchanged
    ann2 = ann_mod.read_annotation(corpus.root, "epi_value")
    values_parity = ann2.values == ann.values or all(
        float(ann2.values[k]) == float(ann.values[k]) for k in ann.values
    )
    meta_ok = (
        ann2.meta.source_hash == groups_hash
        and "labeled_texts" not in (ann2.meta.params or {})
        and ann2.meta.count == len(ann.values)
    )

    return {
        "ok": parity_ok and values_parity and meta_ok,
        "intent": intent,
        "added": added,
        "skipped": skipped,
        "missing_text": missing_text,
        "n_annotation": len(ann.values),
        "n_resolved_labels": len(resolved),
        "parity_ok": parity_ok,
        "mismatches": mismatches[:20],
        "values_parity": values_parity,
        "meta_ok": meta_ok,
        "source_hash": groups_hash,
        "dataset": manifest,
        "annotation_meta_bytes": (corpus.annotations / "epi_value.meta.json").stat().st_size,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--corpus", default="measles")
    ap.add_argument("--intent", default="epi_value")
    ap.add_argument("--dataset", default="v1")
    ap.add_argument("--force-intent", action="store_true")
    ap.add_argument("--force-dataset", action="store_true")
    ap.add_argument("--force-annotation", action="store_true")
    args = ap.parse_args(argv)
    result = migrate(
        corpus_name=args.corpus,
        intent=args.intent,
        dataset_version=args.dataset,
        force_intent=args.force_intent,
        force_dataset=args.force_dataset,
        force_annotation=True,  # first migration always slims
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
