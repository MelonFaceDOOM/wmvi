"""Register labeler / embedder lifecycle CLI subparsers."""

from __future__ import annotations

import argparse
from pathlib import Path


def _add_corpus(p: argparse.ArgumentParser) -> None:
    p.add_argument("--corpus", type=str, default=None)


def add_filter_args(p: argparse.ArgumentParser) -> None:
    """Annotation filter args shared by embed/cluster/hierarchy/sweep/inspect/browse/neighbors/labeler."""
    p.add_argument(
        "--filter",
        action="append",
        default=None,
        metavar="SPEC",
        help=(
            "Annotation filter SPEC: 'name:eq=1', 'name:low=0.5', or "
            "'name:low=0.33,high=0.66'. Repeatable; clauses are AND-ed."
        ),
    )
    p.add_argument(
        "--where-annotation",
        type=str,
        default=None,
        help="Annotation name to filter on (legacy single-clause; AND-ed with --filter)",
    )
    p.add_argument("--eq", type=str, default=None, help="Keep keys where annotation value equals this")
    p.add_argument("--low", type=float, default=None, help="Inclusive lower bound for annotation value")
    p.add_argument("--high", type=float, default=None, help="Inclusive upper bound for annotation value")
    p.add_argument(
        "--selection",
        type=str,
        default=None,
        help="Named selection under corpora/<corpus>/selections/ (AND-ed with --filter)",
    )
    p.add_argument(
        "--save-selection",
        type=str,
        default=None,
        help="Optional: also write a named selection for compatibility",
    )
    p.add_argument(
        "--force-selection",
        action="store_true",
        help="Overwrite existing selection when using --save-selection",
    )


def register_lifecycle_parsers(sub: argparse._SubParsersAction) -> None:
    # --- labeler ---
    lab = sub.add_parser("labeler", help="Label intents, datasets, train/eval/apply Ridge models")
    lab_sub = lab.add_subparsers(dest="labeler_cmd", required=True)

    li_c = lab_sub.add_parser("intent-create", help="Create a label intent")
    li_c.add_argument("--name", required=True)
    li_c.add_argument("--instructions", default="")
    li_c.add_argument("--value-type", choices=("binary", "float"), default="binary")
    li_c.add_argument("--scope", choices=("group", "claim"), default="group")
    li_c.add_argument("--labels-json", default=None)
    li_c.add_argument("--min-gold-total", type=int, default=50)
    li_c.add_argument("--min-gold-per-class", type=int, default=10)
    li_c.add_argument("--probe-target", type=int, default=25)
    li_c.add_argument(
        "--agent-batch-size",
        type=int,
        default=None,
        help="Max claims per agentic labeling turn (written to spec)",
    )
    li_c.add_argument(
        "--agent-model",
        type=str,
        default=None,
        help="Preferred Cursor Task model slug for agentic labeling (written to spec)",
    )
    li_c.add_argument("--force", action="store_true")
    li_c.set_defaults(func="labeler-intent-create")

    li_l = lab_sub.add_parser("intent-list", help="List label intents")
    li_l.set_defaults(func="labeler-intent-list")

    li_s = lab_sub.add_parser("intent-show", help="Show label intent spec")
    li_s.add_argument("--name", required=True)
    li_s.set_defaults(func="labeler-intent-show")

    ll_a = lab_sub.add_parser("labels-add", help="Append one training label row")
    ll_a.add_argument("--intent", required=True)
    ll_a.add_argument("--text", required=True)
    ll_a.add_argument("--value", type=float, required=True)
    ll_a.add_argument("--claim-key", default=None)
    ll_a.add_argument("--reason", default=None)
    ll_a.add_argument("--confidence", type=float, default=None)
    ll_a.add_argument("--corpus", default=None)
    ll_a.add_argument("--producer-type", default="manual")
    ll_a.add_argument("--producer-json", default=None)
    ll_a.add_argument("--probe-run-id", default=None, help="Optional run id if this is a blind probe")
    ll_a.set_defaults(func="labeler-labels-add")

    ll_i = lab_sub.add_parser("labels-import", help="Import training labels from jsonl/json")
    ll_i.add_argument("--intent", required=True)
    ll_i.add_argument("--from", dest="from_path", type=Path, required=True)
    ll_i.add_argument("--corpus", default=None)
    ll_i.add_argument("--producer-type", default="import")
    ll_i.set_defaults(func="labeler-labels-import")

    ll_b = lab_sub.add_parser("labels-browse", help="Browse resolved training labels")
    ll_b.add_argument("--intent", required=True)
    ll_b.add_argument("--value", type=float, default=None)
    ll_b.add_argument("--corpus", default=None)
    ll_b.add_argument("--limit", type=int, default=20)
    ll_b.add_argument("--human", action="store_true", help="Also print human-readable labels")
    ll_b.set_defaults(func="labeler-labels-browse")

    # Gold (human-only eval yardstick)
    lg_s = lab_sub.add_parser("gold-sample", help="Random-sample candidates for gold labeling")
    lg_s.add_argument("--intent", required=True)
    lg_s.add_argument("--corpus", required=True)
    lg_s.add_argument("--n", type=int, required=True)
    lg_s.add_argument("--seed", type=int, default=0)
    lg_s.add_argument("--human", action="store_true")
    add_filter_args(lg_s)
    lg_s.set_defaults(func="labeler-gold-sample")

    lg_a = lab_sub.add_parser("gold-add", help="Append one human gold label")
    lg_a.add_argument("--intent", required=True)
    lg_a.add_argument("--corpus", required=True)
    lg_a.add_argument("--text", required=True)
    lg_a.add_argument("--value", type=float, required=True)
    lg_a.add_argument("--claim-key", default=None)
    lg_a.add_argument("--reason", default=None)
    lg_a.set_defaults(func="labeler-gold-add")

    lg_i = lab_sub.add_parser("gold-import", help="Import human gold labels from jsonl")
    lg_i.add_argument("--intent", required=True)
    lg_i.add_argument("--corpus", required=True)
    lg_i.add_argument("--from", dest="from_path", type=Path, required=True)
    lg_i.set_defaults(func="labeler-gold-import")

    lg_st = lab_sub.add_parser("gold-status", help="Gold counts and gate status")
    lg_st.add_argument("--intent", required=True)
    lg_st.add_argument("--corpus", default=None)
    lg_st.set_defaults(func="labeler-gold-status")

    lg_lab = lab_sub.add_parser(
        "gold-label",
        help="Interactive gold labeling loop (sample n claims, prompt for values)",
    )
    lg_lab.add_argument("--intent", required=True)
    lg_lab.add_argument("--corpus", required=True)
    lg_lab.add_argument("--n", type=int, required=True)
    lg_lab.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Sample seed (default: random each session)",
    )
    add_filter_args(lg_lab)
    lg_lab.set_defaults(func="labeler-gold-label")

    lsamp = lab_sub.add_parser(
        "sample",
        help="Sample claims for labeling (injects blind gold probes; requires gold gate)",
    )
    lsamp.add_argument("--intent", required=True)
    lsamp.add_argument("--corpus", required=True)
    lsamp.add_argument("--n", type=int, required=True)
    lsamp.add_argument("--run-size", type=int, default=None, help="Full run size for probe budgeting")
    lsamp.add_argument("--run-id", default=None)
    lsamp.add_argument("--seed", type=int, default=0)
    lsamp.add_argument("--human", action="store_true")
    add_filter_args(lsamp)
    lsamp.set_defaults(func="labeler-sample")

    ld_f = lab_sub.add_parser("dataset-freeze", help="Freeze a train-only dataset version")
    ld_f.add_argument("--intent", required=True)
    ld_f.add_argument("--version", required=True)
    ld_f.add_argument("--force", action="store_true")
    ld_f.set_defaults(func="labeler-dataset-freeze")

    lt = lab_sub.add_parser("train", help="Train Ridge from frozen dataset")
    lt.add_argument("--intent", required=True)
    lt.add_argument("--dataset", required=True)
    lt.add_argument("--version", required=True, help="Immutable model version id")
    lt.add_argument("--encoder", default=None)
    lt.add_argument("--ridge-alpha", type=float, default=1.0)
    lt.add_argument("--batch-size", type=int, default=32)
    lt.add_argument("--seed", type=int, default=0)
    lt.add_argument("--set-active", action="store_true")
    lt.set_defaults(func="labeler-train")

    le = lab_sub.add_parser("eval", help="Evaluate a model against human gold")
    le.add_argument("--intent", required=True)
    le.add_argument("--model", required=True, help="intent/version, intent@alias, or path")
    le.add_argument("--corpus", default=None)
    le.add_argument("--batch-size", type=int, default=32)
    le.add_argument("--threshold", type=float, default=0.5)
    le.add_argument("--name", default=None, help="Experiment name under model_eval/labelers/")
    le.set_defaults(func="labeler-eval")

    lae = lab_sub.add_parser("agent-eval", help="Evaluate agent probe labels against gold")
    lae.add_argument("--intent", required=True)
    lae.add_argument("--corpus", default=None)
    lae.add_argument("--run-id", default=None)
    lae.add_argument("--threshold", type=float, default=0.5)
    lae.add_argument("--human", action="store_true", help="Also print a human-readable summary")
    lae.set_defaults(func="labeler-agent-eval")

    lane = lab_sub.add_parser(
        "annotation-eval",
        help="Score an annotation vs gold and vs training labels",
    )
    lane.add_argument("--corpus", required=True)
    lane.add_argument("--name", required=True, help="Annotation name")
    lane.add_argument("--intent", required=True)
    lane.add_argument("--threshold", type=float, default=0.5)
    lane.add_argument("--human", action="store_true", help="Also print a human-readable summary")
    lane.set_defaults(func="labeler-annotation-eval")

    lp = lab_sub.add_parser("promote", help="Set alias (default active) to a model version")
    lp.add_argument("--intent", required=True)
    lp.add_argument("--version", required=True)
    lp.add_argument("--alias", default="active")
    lp.set_defaults(func="labeler-promote")

    la = lab_sub.add_parser("apply", help="Apply model to corpus → annotation")
    _add_corpus(la)
    la.add_argument("--model", required=True)
    la.add_argument("--name", required=True, help="Annotation name to write")
    la.add_argument("--intent", default=None)
    la.add_argument("--value-type", default="float")
    la.add_argument("--batch-size", type=int, default=32)
    la.add_argument("--force", action="store_true")
    add_filter_args(la)
    la.set_defaults(func="labeler-apply")

    lm = lab_sub.add_parser("models", help="List model versions for an intent")
    lm.add_argument("--intent", required=True)
    lm.set_defaults(func="labeler-models-list")

    # --- embedder ---
    emb = sub.add_parser("embedder", help="Similarity intents, triplets, train/eval embedders")
    emb_sub = emb.add_subparsers(dest="embedder_cmd", required=True)

    ei_c = emb_sub.add_parser("intent-create", help="Create a similarity intent")
    ei_c.add_argument("--name", required=True)
    ei_c.add_argument("--instructions", default="")
    ei_c.add_argument("--rubric", default="")
    ei_c.add_argument("--eval-frac", type=float, default=0.15)
    ei_c.add_argument("--split-seed", type=int, default=0)
    ei_c.add_argument("--min-gold-total", type=int, default=20)
    ei_c.add_argument("--probe-target", type=int, default=25)
    ei_c.add_argument("--neighbor-k", type=int, default=15)
    ei_c.add_argument("--agent-batch-size", type=int, default=20)
    ei_c.add_argument("--agent-model", default=None)
    ei_c.add_argument("--force", action="store_true")
    ei_c.set_defaults(func="embedder-intent-create")

    ei_l = emb_sub.add_parser("intent-list", help="List similarity intents")
    ei_l.set_defaults(func="embedder-intent-list")

    ei_s = emb_sub.add_parser("intent-show", help="Show similarity intent spec")
    ei_s.add_argument("--name", required=True)
    ei_s.set_defaults(func="embedder-intent-show")

    et_a = emb_sub.add_parser("triplets-add", help="Append one triplet row")
    et_a.add_argument("--intent", required=True)
    et_a.add_argument("--anchor", required=True)
    et_a.add_argument(
        "--positives",
        default="[]",
        help="JSON list or single text (empty list allowed)",
    )
    et_a.add_argument(
        "--negatives",
        default="[]",
        help="JSON list or single text (empty list allowed)",
    )
    et_a.add_argument("--anchor-key", default=None)
    et_a.add_argument("--reason", default=None)
    et_a.add_argument("--corpus", default=None)
    et_a.add_argument("--producer-type", default="manual")
    et_a.add_argument("--producer-json", default=None)
    et_a.set_defaults(func="embedder-triplets-add")

    et_i = emb_sub.add_parser(
        "triplets-import-neighbors",
        help="Import neighbors JSON with positives/negatives into triplets (legacy)",
    )
    et_i.add_argument("--intent", required=True)
    et_i.add_argument("--from", dest="from_path", type=Path, required=True)
    et_i.add_argument("--corpus", default=None)
    et_i.add_argument(
        "--auto-split",
        action="store_true",
        help="Heuristic split of neighbors into pos/neg when keys missing",
    )
    et_i.set_defaults(func="embedder-triplets-import-neighbors")

    et_j = emb_sub.add_parser(
        "triplets-import",
        help="Import judged sample batch (claim_key + pos/neg indices or keys)",
    )
    et_j.add_argument("--intent", required=True)
    et_j.add_argument("--from", dest="from_path", type=Path, required=True)
    et_j.add_argument("--corpus", default=None)
    et_j.add_argument("--run-id", default=None, help="Probe/sample run_id for neighbor map")
    et_j.add_argument(
        "--sample",
        type=Path,
        default=None,
        help="Optional sample JSON (from embedder sample) with neighbor texts",
    )
    et_j.add_argument("--producer-type", default="agent_label")
    et_j.add_argument("--model-tag", default=None)
    et_j.set_defaults(func="embedder-triplets-import")

    esamp = emb_sub.add_parser(
        "sample",
        help="Sample anchors + numbered neighbors from an embed run (injects gold probes)",
    )
    esamp.add_argument("--intent", required=True)
    _add_corpus(esamp)
    esamp.add_argument("--model-tag", required=True)
    esamp.add_argument("--run-dir", type=Path, default=None)
    esamp.add_argument("--n", type=int, required=True)
    esamp.add_argument("--run-size", type=int, default=None)
    esamp.add_argument("--run-id", default=None)
    esamp.add_argument("--seed", type=int, default=0)
    esamp.add_argument("--neighbor-k", type=int, default=None)
    esamp.add_argument("--human", action="store_true")
    add_filter_args(esamp)
    esamp.set_defaults(func="embedder-sample")

    egs = emb_sub.add_parser("gold-sample", help="Sample anchors+neighbors for human gold")
    egs.add_argument("--intent", required=True)
    _add_corpus(egs)
    egs.add_argument("--model-tag", required=True)
    egs.add_argument("--run-dir", type=Path, default=None)
    egs.add_argument("--n", type=int, required=True)
    egs.add_argument("--seed", type=int, default=0)
    egs.add_argument("--neighbor-k", type=int, default=None)
    egs.add_argument("--human", action="store_true")
    add_filter_args(egs)
    egs.set_defaults(func="embedder-gold-sample")

    ega = emb_sub.add_parser("gold-add", help="Append one human gold triplet")
    ega.add_argument("--intent", required=True)
    ega.add_argument("--corpus", required=True)
    ega.add_argument("--anchor", required=True)
    ega.add_argument("--positives", default="[]")
    ega.add_argument("--negatives", default="[]")
    ega.add_argument("--positive-keys", default=None, help="JSON list of claim_keys")
    ega.add_argument("--negative-keys", default=None, help="JSON list of claim_keys")
    ega.add_argument("--shown-keys", default=None, help="JSON list of shown neighbor keys")
    ega.add_argument("--claim-key", default=None)
    ega.add_argument("--reason", default=None)
    ega.add_argument("--model-tag", default=None)
    ega.set_defaults(func="embedder-gold-add")

    egi = emb_sub.add_parser("gold-import", help="Import human gold triplets from jsonl/JSON")
    egi.add_argument("--intent", required=True)
    egi.add_argument("--from", dest="from_path", type=Path, required=True)
    egi.add_argument("--corpus", default=None)
    egi.set_defaults(func="embedder-gold-import")

    egst = emb_sub.add_parser("gold-status", help="Gold gate status for a similarity intent")
    egst.add_argument("--intent", required=True)
    egst.add_argument("--corpus", default=None)
    egst.set_defaults(func="embedder-gold-status")

    egl = emb_sub.add_parser("gold-label", help="Interactive human gold labeling loop")
    egl.add_argument("--intent", required=True)
    _add_corpus(egl)
    egl.add_argument("--model-tag", required=True)
    egl.add_argument("--run-dir", type=Path, default=None)
    egl.add_argument("--n", type=int, required=True)
    egl.add_argument("--seed", type=int, default=None)
    egl.add_argument("--neighbor-k", type=int, default=None)
    add_filter_args(egl)
    egl.set_defaults(func="embedder-gold-label")

    ed_f = emb_sub.add_parser("dataset-freeze", help="Freeze a train-only triplet dataset")
    ed_f.add_argument("--intent", required=True)
    ed_f.add_argument("--version", required=True)
    ed_f.add_argument("--force", action="store_true")
    ed_f.set_defaults(func="embedder-dataset-freeze")

    et = emb_sub.add_parser("train", help="Train embedder from frozen dataset")
    et.add_argument("--intent", required=True)
    et.add_argument("--dataset", required=True)
    et.add_argument("--version", required=True)
    et.add_argument("--base-model", required=True)
    et.add_argument("--corpus", default=None, help="Corpus for gold-as-dev evaluation")
    et.add_argument("--loss", default="MultipleNegativesRankingLoss")
    et.add_argument("--batch-size", type=int, default=16)
    et.add_argument("--learning-rate", type=float, default=2e-5)
    et.add_argument("--epochs", type=int, default=1)
    et.add_argument(
        "--lora",
        action="store_true",
        help="PEFT LoRA + grad checkpointing (required for 8B on 32GB)",
    )
    et.add_argument("--lora-r", type=int, default=16)
    et.add_argument("--lora-alpha", type=int, default=32)
    et.add_argument(
        "--doc-instruction",
        type=str,
        default="",
        help="Same encode prompt used at embed time (prefix every train text)",
    )
    et.add_argument("--max-seq-length", type=int, default=512)
    et.add_argument(
        "--dtype",
        type=str,
        default="auto",
        choices=("auto", "bfloat16", "float16", "float32"),
    )
    et.add_argument("--set-active", action="store_true")
    et.set_defaults(func="embedder-train")

    etc = emb_sub.add_parser(
        "train-compare",
        help="Train MNRL and TripletLoss; eval on gold pairwise; promote winner",
    )
    etc.add_argument("--intent", required=True)
    etc.add_argument("--dataset", required=True)
    etc.add_argument("--version", required=True, help="Base version id (suffix _mnrl/_triplet)")
    etc.add_argument("--base-model", required=True)
    etc.add_argument("--corpus", required=True)
    etc.add_argument("--batch-size", type=int, default=16)
    etc.add_argument("--learning-rate", type=float, default=2e-5)
    etc.add_argument("--epochs", type=int, default=1)
    etc.add_argument(
        "--lora",
        action="store_true",
        help="PEFT LoRA + grad checkpointing (use batch 2–4, lr ~1e-4 for 8B)",
    )
    etc.add_argument("--lora-r", type=int, default=16)
    etc.add_argument("--lora-alpha", type=int, default=32)
    etc.add_argument("--doc-instruction", type=str, default="")
    etc.add_argument("--max-seq-length", type=int, default=512)
    etc.add_argument(
        "--dtype",
        type=str,
        default="auto",
        choices=("auto", "bfloat16", "float16", "float32"),
    )
    etc.add_argument("--set-active", action="store_true")
    etc.add_argument("--force", action="store_true")
    etc.add_argument("--human", action="store_true")
    etc.set_defaults(func="embedder-train-compare")

    ee = emb_sub.add_parser("eval", help="Evaluate embedder on gold pairwise")
    ee.add_argument("--intent", required=True)
    ee.add_argument("--model", required=True)
    ee.add_argument("--corpus", required=True)
    ee.add_argument("--name", default=None)
    ee.add_argument("--doc-instruction", type=str, default="")
    ee.add_argument("--max-seq-length", type=int, default=None)
    ee.add_argument(
        "--dtype",
        type=str,
        default=None,
        choices=("auto", "bfloat16", "float16", "float32"),
    )
    ee.add_argument("--human", action="store_true")
    ee.set_defaults(func="embedder-eval")

    eae = emb_sub.add_parser(
        "agent-eval",
        help="Evaluate agent probe triplets against gold (set agreement)",
    )
    eae.add_argument("--intent", required=True)
    eae.add_argument("--corpus", required=True)
    eae.add_argument("--run-id", default=None)
    eae.add_argument("--min-probes", type=int, default=1)
    eae.add_argument("--human", action="store_true")
    eae.set_defaults(func="embedder-agent-eval")

    ep = emb_sub.add_parser("promote", help="Set alias to embedder version")
    ep.add_argument("--intent", required=True)
    ep.add_argument("--version", required=True)
    ep.add_argument("--alias", default="active")
    ep.set_defaults(func="embedder-promote")
