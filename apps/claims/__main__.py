"""CLI entry: python -m apps.claims <command> ..."""

from __future__ import annotations

import argparse
from pathlib import Path


def _add_corpus_args(p: argparse.ArgumentParser, *, with_model_tag: bool = False) -> None:
    p.add_argument(
        "--corpus",
        type=str,
        default=None,
        help="Corpus slug under data/corpora/<slug>/ (fills default paths)",
    )
    if with_model_tag:
        p.add_argument(
            "--model-tag",
            type=str,
            default=None,
            help="Tag for run under data/runs/<corpus>/<tag>/ (default: derived from --model)",
        )


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="python -m apps.claims",
        description="Claims pipeline (file-mode): group, embed, annotate/select, train, cluster.",
    )
    sub = ap.add_subparsers(dest="command", required=True)

    from apps.claims.cli.lifecycle_parsers import add_filter_args, register_lifecycle_parsers

    register_lifecycle_parsers(sub)

    # --- corpus ---
    corpus_p = sub.add_parser("corpus", help="Create/list/seed/status corpora under data/corpora/")
    corpus_sub = corpus_p.add_subparsers(dest="corpus_cmd", required=True)
    c_create = corpus_sub.add_parser("create", help="Create a corpus directory + NOTES.md")
    c_create.add_argument("--name", type=str, required=True, help="Slug, e.g. measles")
    c_create.add_argument("--notes", type=str, default=None, help="Optional NOTES.md body")
    c_create.set_defaults(func="corpus-create")
    c_list = corpus_sub.add_parser("list", help="List corpora and artifact status")
    c_list.set_defaults(func="corpus-list")
    c_status = corpus_sub.add_parser("status", help="Detailed status for one corpus")
    c_status.add_argument("--name", type=str, required=True)
    c_status.add_argument("--human", action="store_true", help="Also print human checklist")
    c_status.set_defaults(func="corpus-status")
    c_seed = corpus_sub.add_parser(
        "seed",
        help="Fetch posts for search terms (+ date range) into data/corpora/<name>/posts.json",
    )
    c_seed.add_argument("--name", type=str, required=True, help="Corpus slug")
    c_seed.add_argument("--terms", nargs="*", default=[], help="taxonomy.vaccine_term.name values")
    c_seed.add_argument("--terms-file", type=Path, default=None, help="One term per line")
    c_seed.add_argument("--since", type=str, default=None, help="Inclusive lower bound (UTC)")
    c_seed.add_argument("--until", type=str, default=None, help="Exclusive upper bound (UTC)")
    c_seed.add_argument("--limit", type=int, default=None, help="Max posts to fetch")
    c_seed.add_argument("--prod", action="store_true", help="Use prod DB credentials")
    c_seed.add_argument("--create", action="store_true", help="Create corpus if missing")
    c_seed.add_argument("--notes", type=str, default=None, help="NOTES.md body when --create")
    c_seed.add_argument("--force", action="store_true", help="Overwrite existing posts.json")
    c_seed.add_argument("--count-first", action="store_true", help="COUNT(*) before streaming")
    c_seed.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate args / paths only (no DB)",
    )
    c_seed.set_defaults(func="corpus-seed")
    c_copy = corpus_sub.add_parser(
        "copy-posts",
        help="Copy an existing posts JSON into a corpus (no DB)",
    )
    c_copy.add_argument("--name", type=str, required=True)
    c_copy.add_argument("--from", dest="source", type=Path, required=True, help="Source posts JSON")
    c_copy.add_argument("--create", action="store_true")
    c_copy.add_argument("--notes", type=str, default=None)
    c_copy.add_argument("--force", action="store_true")
    c_copy.set_defaults(func="corpus-copy-posts")
    c_imp = corpus_sub.add_parser(
        "import-claims",
        help="Copy nested posts→chunks→claims JSON into a corpus as claims.json",
    )
    c_imp.add_argument("--name", type=str, required=True)
    c_imp.add_argument(
        "--from",
        dest="source",
        type=Path,
        required=True,
        help="Source nested claims JSON (e.g. data/measles_1.json)",
    )
    c_imp.add_argument("--create", action="store_true")
    c_imp.add_argument("--notes", type=str, default=None)
    c_imp.add_argument("--force", action="store_true")
    c_imp.set_defaults(func="corpus-import-claims")
    c_der = corpus_sub.add_parser(
        "derive",
        help=(
            "Derive a Reddit-deweighted corpus: keep all non-Reddit posts, "
            "downsample Reddit posts so Reddit claims ≈ target_ratio × other claims"
        ),
    )
    c_der.add_argument("--from", dest="from_corpus", type=str, required=True, help="Parent corpus slug")
    c_der.add_argument("--name", type=str, required=True, help="New corpus slug (e.g. measles_bal)")
    c_der.add_argument(
        "--target-ratio",
        type=float,
        default=1.0,
        help="Reddit claims ≈ this × non-Reddit claims (default 1.0)",
    )
    c_der.add_argument("--seed", type=int, default=0, help="RNG seed for Reddit post sample")
    c_der.add_argument(
        "--group",
        action="store_true",
        help="Also run group on the new corpus after writing claims.json",
    )
    c_der.add_argument("--notes", type=str, default=None, help="NOTES.md body when creating")
    c_der.add_argument("--force", action="store_true", help="Overwrite existing claims.json")
    c_der.set_defaults(func="corpus-derive")

    # --- model ---
    model_p = sub.add_parser(
        "model",
        help="Register/list local embedder models under data/models/registered/",
    )
    model_sub = model_p.add_subparsers(dest="model_cmd", required=True)
    m_reg = model_sub.add_parser(
        "register",
        help="Symlink/copy a model dir to data/models/registered/<tag>",
    )
    m_reg.add_argument("--path", type=Path, required=True)
    m_reg.add_argument("--tag", type=str, required=True)
    m_reg.add_argument("--mode", choices=("symlink", "copy"), default="symlink")
    m_reg.add_argument("--force", action="store_true")
    m_reg.set_defaults(func="model-register")
    m_list = model_sub.add_parser("list", help="List registered models")
    m_list.set_defaults(func="model-list")
    m_res = model_sub.add_parser("resolve", help="Resolve a tag or path to an absolute model path")
    m_res.add_argument("--model", type=str, required=True)
    m_res.set_defaults(func="model-resolve")

    # --- runs ---
    runs_p = sub.add_parser("runs", help="List / export / import embed runs")
    runs_sub = runs_p.add_subparsers(dest="runs_cmd", required=True)
    r_list = runs_sub.add_parser("list", help="List runs (optionally filter by --corpus)")
    r_list.add_argument("--corpus", type=str, default=None)
    r_list.set_defaults(func="runs-list")
    r_exp = runs_sub.add_parser("export", help="Zip a run dir (vectors/index/metrics)")
    r_exp.add_argument("--run-dir", type=Path, default=None)
    _add_corpus_args(r_exp, with_model_tag=True)
    r_exp.add_argument("--out", type=Path, required=True, help="Output .zip path")
    r_exp.set_defaults(func="runs-export")
    r_imp = runs_sub.add_parser("import", help="Extract a run zip under data/runs/")
    r_imp.add_argument("--from", dest="from_zip", type=Path, required=True)
    r_imp.add_argument("--run-name", type=str, default=None)
    _add_corpus_args(r_imp, with_model_tag=True)
    r_imp.add_argument("--force", action="store_true")
    r_imp.set_defaults(func="runs-import")

    # --- validate ---
    val_p = sub.add_parser("validate", help="Summarize nested posts→chunks→claims JSON")
    _add_corpus_args(val_p)
    val_p.add_argument("--claims", type=Path, default=None)
    val_p.add_argument("--human", action="store_true", help="Also print human summary")
    val_p.set_defaults(func="validate")

    # --- group ---
    group_p = sub.add_parser("group", help="Collapse duplicate claims into groups JSON")
    _add_corpus_args(group_p)
    group_p.add_argument("--claims", type=Path, default=None, help="Nested posts→chunks→claims JSON")
    group_p.add_argument("--out", type=Path, default=None, help="Output groups JSON")
    group_p.set_defaults(func="group")

    # --- embed ---
    embed_p = sub.add_parser("embed", help="Embed claim groups; write run dir artifacts")
    _add_corpus_args(embed_p, with_model_tag=True)
    embed_p.add_argument("--groups", type=Path, default=None, help="Groups JSON from `group`")
    embed_p.add_argument("--model", type=str, required=True, help="Model id, path, or registered tag")
    embed_p.add_argument("--run-name", type=str, default=None, help="Name under data/runs/")
    embed_p.add_argument("--doc-instruction", type=str, default="")
    embed_p.add_argument("--query-instruction", type=str, default="")
    embed_p.add_argument("--no-normalize", action="store_true")
    embed_p.add_argument(
        "--batch-size",
        type=int,
        default=16,
        help="Encode batch size (default 16; safer for large models)",
    )
    embed_p.add_argument(
        "--max-seq-length",
        type=int,
        default=512,
        help="Tokenizer max length (default 512; avoid 32k OOM on Qwen3)",
    )
    embed_p.add_argument(
        "--dtype",
        type=str,
        default="auto",
        choices=("auto", "bfloat16", "float16", "float32"),
        help="Weight dtype (auto=bfloat16 on CUDA)",
    )
    embed_p.add_argument(
        "--device",
        type=str,
        default="auto",
        choices=("auto", "cuda", "cpu"),
        help="Device for encoding",
    )
    embed_p.add_argument("--limit", type=int, default=None, help="Embed only first N groups")
    embed_p.add_argument("--force", action="store_true", help="Overwrite existing run dir")
    from apps.claims.cli.lifecycle_parsers import add_filter_args

    add_filter_args(embed_p)
    embed_p.set_defaults(func="embed")

    # --- cluster ---
    cluster_p = sub.add_parser("cluster", help="Run one clustering config on a run dir")
    _add_corpus_args(cluster_p, with_model_tag=True)
    cluster_p.add_argument("--run-dir", type=Path, default=None)
    add_filter_args(cluster_p)
    cluster_p.add_argument("--algorithm", type=str, required=True)
    cluster_p.add_argument("--params-json", type=str, default="{}")
    cluster_p.add_argument("--seed", type=int, default=0)
    cluster_p.add_argument("--out-dir", type=Path, default=None)
    cluster_p.add_argument("--save-labels", action="store_true")
    cluster_p.add_argument("--queries", type=Path, default=None)
    cluster_p.set_defaults(func="cluster")

    # --- hierarchy ---
    hier_p = sub.add_parser("hierarchy", help="Two-level leaf->narrative clustering")
    _add_corpus_args(hier_p, with_model_tag=True)
    hier_p.add_argument(
        "--preset",
        type=str,
        default=None,
        help="Named config (default=kmeans-800→agglo-25). Overrides leaf/narrative defaults.",
    )
    hier_p.add_argument("--run-dir", type=Path, default=None)
    add_filter_args(hier_p)
    hier_p.add_argument("--leaf-algorithm", type=str, default=None)
    hier_p.add_argument("--leaf-params-json", type=str, default=None)
    hier_p.add_argument("--narrative-algorithm", type=str, default=None)
    hier_p.add_argument("--narrative-params-json", type=str, default=None)
    hier_p.add_argument("--seed", type=int, default=0)
    hier_p.add_argument("--out-dir", type=Path, default=None)
    hier_p.add_argument("--save-labels", action="store_true")
    hier_p.add_argument("--queries", type=Path, default=None)
    hier_p.add_argument("--n-samples-per-leaf", type=int, default=3)
    hier_p.set_defaults(func="hierarchy")

    # --- inspect ---
    insp_p = sub.add_parser("inspect", help="Sample claim texts from clusters")
    _add_corpus_args(insp_p, with_model_tag=True)
    insp_p.add_argument("--run-dir", type=Path, default=None)
    add_filter_args(insp_p)
    insp_p.add_argument("--labels", type=Path, required=True)
    insp_p.add_argument("--parent-labels", type=Path, default=None)
    insp_p.add_argument("--mode", type=str, default="mixed")
    insp_p.add_argument("--n-clusters", type=int, default=8)
    insp_p.add_argument("--n-per-cluster", type=int, default=5)
    insp_p.add_argument("--cluster-ids", type=str, default=None)
    insp_p.add_argument("--min-size", type=int, default=3)
    insp_p.add_argument("--seed", type=int, default=0)
    insp_p.add_argument("--out-dir", type=Path, default=None)
    insp_p.add_argument("--queries", type=Path, default=None)
    insp_p.set_defaults(func="inspect")

    # --- cluster-browse ---
    cb_p = sub.add_parser(
        "cluster-browse",
        help="Open a Streamlit browser for one cluster/hierarchy output",
    )
    _add_corpus_args(cb_p, with_model_tag=True)
    cb_p.add_argument(
        "--from",
        dest="from_path",
        type=Path,
        default=None,
        help="Experiment directory, hierarchy_*.json / result_*.json, or labels .npy",
    )
    cb_p.add_argument("--run-dir", type=Path, default=None)
    cb_p.add_argument("--labels", type=Path, default=None, help="Override labels npy (flat or leaf)")
    cb_p.add_argument(
        "--parent-labels",
        type=Path,
        default=None,
        help="Narrative labels npy (hierarchy); inferred from --from when omitted",
    )
    cb_p.add_argument(
        "--claims",
        type=Path,
        default=None,
        help="Override nested claims.json (default: corpora/<corpus>/claims.json)",
    )
    add_filter_args(cb_p)
    cb_p.add_argument("--port", type=int, default=None, help="Streamlit server port")
    cb_p.set_defaults(func="cluster-browse")

    # --- neighbors ---
    nn_p = sub.add_parser(
        "neighbors",
        help="Browse claim nearest neighbors from an embed run",
    )
    _add_corpus_args(nn_p, with_model_tag=True)
    nn_p.add_argument("--run-dir", type=Path, default=None)
    nn_mode = nn_p.add_mutually_exclusive_group(required=True)
    nn_mode.add_argument(
        "--claim-index",
        type=int,
        default=None,
        help="Corpus claim index (row in vectors.npy)",
    )
    nn_mode.add_argument(
        "--text",
        type=str,
        default=None,
        help="Free-text query (embeds with run model_id, then NN search)",
    )
    nn_mode.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Sample N random non-empty claims and show neighbors for each",
    )
    nn_p.add_argument("--top-k", type=int, default=15, help="Neighbors per anchor (default 15)")
    nn_p.add_argument("--seed", type=int, default=0, help="RNG seed for --sample")
    nn_p.add_argument(
        "--exclude",
        type=Path,
        default=None,
        help="File of claim_keys to skip when using --sample (json/jsonl/txt)",
    )
    from apps.claims.cli.lifecycle_parsers import add_filter_args as _add_filt_nn

    _add_filt_nn(nn_p)
    nn_p.add_argument("--human", action="store_true", help="Also print human-readable neighbors")
    nn_p.set_defaults(func="neighbors")

    # --- browse ---
    br_p = sub.add_parser(
        "browse",
        help=(
            "Sample claim texts for pointwise labeling "
            "(default: corpus groups.json; optional embed-run index)"
        ),
    )
    _add_corpus_args(br_p, with_model_tag=True)
    br_p.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Optional embed run dir (samples its index.json instead of groups.json)",
    )
    br_p.add_argument(
        "--sample",
        type=int,
        required=True,
        help="Sample N random non-empty claims",
    )
    br_p.add_argument("--seed", type=int, default=0, help="RNG seed")
    br_p.add_argument(
        "--exclude",
        type=Path,
        default=None,
        help="File of claim_keys already labeled (json/jsonl/txt)",
    )
    from apps.claims.cli.lifecycle_parsers import add_filter_args as _add_filt_br

    _add_filt_br(br_p)
    br_p.add_argument("--human", action="store_true", help="Also print human-readable claims")
    br_p.set_defaults(func="browse")

    # --- prep-queries ---
    prep_p = sub.add_parser("prep-queries", help="Cache eval-query vectors for a model")
    prep_p.add_argument("--model", type=str, required=True)
    prep_p.add_argument("--out-dir", type=Path, required=True)
    prep_p.add_argument("--queries", type=Path, default=None)
    prep_p.add_argument("--query-instruction", type=str, default="")
    prep_p.add_argument("--force", action="store_true")
    prep_p.set_defaults(func="prep-queries")

    # --- sweep ---
    sweep_p = sub.add_parser("sweep", help="Run many clustering configs; write sweep.jsonl")
    _add_corpus_args(sweep_p, with_model_tag=True)
    sweep_p.add_argument("--run-dir", type=Path, default=None)
    from apps.claims.cli.lifecycle_parsers import add_filter_args as _add_filt

    _add_filt(sweep_p)
    sweep_p.add_argument("--configs", type=Path, required=True)
    sweep_p.add_argument("--seed", type=int, default=0)
    sweep_p.add_argument("--out-dir", type=Path, default=None)
    sweep_p.add_argument("--queries", type=Path, default=None)
    sweep_p.set_defaults(func="sweep")

    # --- annotations ---
    ann_p = sub.add_parser("annotations", help="List/show/remove corpus annotations")
    ann_sub = ann_p.add_subparsers(dest="ann_cmd", required=True)
    a_list = ann_sub.add_parser("list", help="List annotations for a corpus")
    _add_corpus_args(a_list)
    a_list.set_defaults(func="annotations-list")
    a_show = ann_sub.add_parser("show", help="Show annotation meta + sample values")
    _add_corpus_args(a_show)
    a_show.add_argument("--name", type=str, required=True)
    a_show.add_argument("--limit", type=int, default=20)
    a_show.add_argument("--eq", type=str, default=None)
    a_show.add_argument("--low", type=float, default=None)
    a_show.add_argument("--high", type=float, default=None)
    a_show.set_defaults(func="annotations-show")
    a_diff = ann_sub.add_parser("diff", help="Diff two annotations")
    _add_corpus_args(a_diff)
    a_diff.add_argument("--left", type=str, required=True)
    a_diff.add_argument("--right", type=str, required=True)
    a_diff.add_argument("--limit", type=int, default=20)
    a_diff.set_defaults(func="annotations-diff")
    a_rm = ann_sub.add_parser("rm", help="Remove an annotation")
    _add_corpus_args(a_rm)
    a_rm.add_argument("--name", type=str, required=True)
    a_rm.set_defaults(func="annotations-rm")

    # --- select ---
    sel_p = sub.add_parser(
        "select",
        help="Build a selection from an annotation + threshold (writes selections/<name>.json)",
    )
    _add_corpus_args(sel_p)
    sel_p.add_argument("--annotation", type=str, required=True, help="Annotation name")
    sel_p.add_argument("--name", type=str, required=True, help="Selection name to write")
    sel_p.add_argument("--low", type=float, default=None, help="Inclusive lower bound")
    sel_p.add_argument("--high", type=float, default=None, help="Inclusive upper bound")
    sel_p.add_argument(
        "--exclusive",
        action="store_true",
        help="Use open interval (exclude endpoints)",
    )
    sel_p.add_argument("--force", action="store_true")
    sel_p.set_defaults(func="select")

    # --- selections ---
    sels_p = sub.add_parser("selections", help="List selections for a corpus")
    sels_sub = sels_p.add_subparsers(dest="sels_cmd", required=True)
    s_list = sels_sub.add_parser("list", help="List selections")
    _add_corpus_args(s_list)
    s_list.set_defaults(func="selections-list")

    # --- doctor ---
    doc_p = sub.add_parser("doctor", help="Self-check run dir / deps (JSON)")
    _add_corpus_args(doc_p, with_model_tag=True)
    doc_p.add_argument("--run-dir", type=Path, default=None)
    doc_p.add_argument("--queries", type=Path, default=None)
    doc_p.add_argument("--skip-model", action="store_true")
    doc_p.set_defaults(func="doctor")

    # --- train ---
    train_p = sub.add_parser("train", help="Fine-tune embedder from triplets JSON")
    train_p.add_argument("--triplets", type=Path, required=True)
    train_p.add_argument("--base-model", type=str, required=True)
    train_p.add_argument("--output-name", type=str, required=True)
    train_p.add_argument("--loss", type=str, default="MultipleNegativesRankingLoss")
    train_p.add_argument("--batch-size", type=int, default=16)
    train_p.add_argument("--learning-rate", type=float, default=2e-5)
    train_p.add_argument("--epochs", type=int, default=1)
    train_p.set_defaults(func="train")

    # --- eval-triplets ---
    eval_p = sub.add_parser("eval-triplets", help="Score triplets against a run dir")
    _add_corpus_args(eval_p, with_model_tag=True)
    eval_p.add_argument("--run-dir", type=Path, default=None)
    eval_p.add_argument("--triplets", type=Path, required=True)
    eval_p.add_argument("--pool", type=str, default="eval")
    eval_p.set_defaults(func="eval-triplets")

    # --- discover-triplets ---
    disc_p = sub.add_parser("discover-triplets", help="LLM auto-discover triplet anchors")
    _add_corpus_args(disc_p, with_model_tag=True)
    disc_p.add_argument("--run-dir", type=Path, default=None)
    disc_p.add_argument("--out", type=Path, required=True, help="Output triplets JSON")
    disc_p.add_argument("--unusable-log", type=Path, default=None)
    disc_p.add_argument("--model", type=str, required=True, help="LLM model name")
    disc_p.add_argument("--n-claims", type=int, default=20)
    disc_p.add_argument("--existing", type=Path, default=None, help="Existing triplets to exclude")
    disc_p.set_defaults(func="discover-triplets")

    # --- ls-artifacts ---
    ls_p = sub.add_parser("ls-artifacts", help="List models/fixtures/runs/corpora under data/")
    ls_p.set_defaults(func="ls-artifacts")

    return ap


def main(argv: list[str] | None = None) -> int:
    from apps.claims import io as claims_io

    claims_io.ensure_data_dirs()
    ap = build_parser()
    args = ap.parse_args(argv)

    if args.func == "ls-artifacts":
        from apps.claims.cli.ls_artifacts import cmd_ls_artifacts

        return cmd_ls_artifacts()
    if args.func == "corpus-create":
        from apps.claims.cli.corpus_cmd import cmd_corpus_create

        return cmd_corpus_create(args)
    if args.func == "corpus-list":
        from apps.claims.cli.corpus_cmd import cmd_corpus_list

        return cmd_corpus_list(args)
    if args.func == "corpus-status":
        from apps.claims.cli.corpus_cmd import cmd_corpus_status

        return cmd_corpus_status(args)
    if args.func == "corpus-seed":
        from apps.claims.cli.corpus_cmd import cmd_corpus_seed

        return cmd_corpus_seed(args)
    if args.func == "corpus-copy-posts":
        from apps.claims.cli.corpus_cmd import cmd_corpus_copy_posts

        return cmd_corpus_copy_posts(args)
    if args.func == "corpus-import-claims":
        from apps.claims.cli.corpus_cmd import cmd_corpus_import_claims

        return cmd_corpus_import_claims(args)
    if args.func == "corpus-derive":
        from apps.claims.cli.corpus_cmd import cmd_corpus_derive

        return cmd_corpus_derive(args)
    if args.func == "model-register":
        from apps.claims.cli.model_cmd import cmd_model_register

        return cmd_model_register(args)
    if args.func == "model-list":
        from apps.claims.cli.model_cmd import cmd_model_list

        return cmd_model_list(args)
    if args.func == "model-resolve":
        from apps.claims.cli.model_cmd import cmd_model_resolve

        return cmd_model_resolve(args)
    if args.func == "runs-list":
        from apps.claims.cli.runs_cmd import cmd_runs_list

        return cmd_runs_list(args)
    if args.func == "runs-export":
        from apps.claims.cli.runs_cmd import cmd_runs_export

        return cmd_runs_export(args)
    if args.func == "runs-import":
        from apps.claims.cli.runs_cmd import cmd_runs_import

        return cmd_runs_import(args)
    if args.func == "validate":
        from apps.claims.cli.validate_cmd import cmd_validate

        return cmd_validate(args)
    if args.func == "group":
        from apps.claims.cli.group_cmd import cmd_group

        return cmd_group(args)
    if args.func == "embed":
        from apps.claims.cli.embed_cmd import cmd_embed

        return cmd_embed(args)
    if args.func == "cluster":
        from apps.claims.cli.cluster_cmd import cmd_cluster

        return cmd_cluster(args)
    if args.func == "hierarchy":
        from apps.claims.cli.cluster_cmd import cmd_hierarchy

        return cmd_hierarchy(args)
    if args.func == "inspect":
        from apps.claims.cli.cluster_cmd import cmd_inspect

        return cmd_inspect(args)
    if args.func == "cluster-browse":
        from apps.claims.cli.cluster_browse_cmd import cmd_cluster_browse

        return cmd_cluster_browse(args)
    if args.func == "neighbors":
        from apps.claims.cli.neighbors_cmd import cmd_neighbors

        return cmd_neighbors(args)
    if args.func == "browse":
        from apps.claims.cli.browse_cmd import cmd_browse

        return cmd_browse(args)
    if args.func == "prep-queries":
        from apps.claims.cli.cluster_cmd import cmd_prep_queries

        return cmd_prep_queries(args)
    if args.func == "sweep":
        from apps.claims.cli.cluster_cmd import cmd_sweep

        return cmd_sweep(args)
    if args.func == "doctor":
        from apps.claims.cli.cluster_cmd import cmd_doctor

        return cmd_doctor(args)
    if args.func == "annotations-list":
        from apps.claims.cli.annotations_cmd import cmd_annotations_list

        return cmd_annotations_list(args)
    if args.func == "annotations-show":
        from apps.claims.cli.annotations_cmd import cmd_annotations_show

        return cmd_annotations_show(args)
    if args.func == "annotations-diff":
        from apps.claims.cli.annotations_cmd import cmd_annotations_diff

        return cmd_annotations_diff(args)
    if args.func == "annotations-rm":
        from apps.claims.cli.annotations_cmd import cmd_annotations_rm

        return cmd_annotations_rm(args)
    if args.func == "select":
        from apps.claims.cli.annotations_cmd import cmd_select

        return cmd_select(args)
    if args.func == "selections-list":
        from apps.claims.cli.annotations_cmd import cmd_selections_list

        return cmd_selections_list(args)
    if args.func == "train":
        from apps.claims.cli.train_cmd import cmd_train

        return cmd_train(args)
    if args.func == "eval-triplets":
        from apps.claims.cli.train_cmd import cmd_eval_triplets

        return cmd_eval_triplets(args)
    if args.func == "discover-triplets":
        from apps.claims.cli.train_cmd import cmd_discover_triplets

        return cmd_discover_triplets(args)

    # Labeler lifecycle
    if args.func == "labeler-intent-create":
        from apps.claims.cli.labeler_cmd import cmd_labeler_intent_create

        return cmd_labeler_intent_create(args)
    if args.func == "labeler-intent-list":
        from apps.claims.cli.labeler_cmd import cmd_labeler_intent_list

        return cmd_labeler_intent_list(args)
    if args.func == "labeler-intent-show":
        from apps.claims.cli.labeler_cmd import cmd_labeler_intent_show

        return cmd_labeler_intent_show(args)
    if args.func == "labeler-labels-add":
        from apps.claims.cli.labeler_cmd import cmd_labeler_labels_add

        return cmd_labeler_labels_add(args)
    if args.func == "labeler-labels-import":
        from apps.claims.cli.labeler_cmd import cmd_labeler_labels_import

        return cmd_labeler_labels_import(args)
    if args.func == "labeler-labels-browse":
        from apps.claims.cli.labeler_cmd import cmd_labeler_labels_browse

        return cmd_labeler_labels_browse(args)
    if args.func == "labeler-gold-sample":
        from apps.claims.cli.labeler_cmd import cmd_labeler_gold_sample

        return cmd_labeler_gold_sample(args)
    if args.func == "labeler-gold-add":
        from apps.claims.cli.labeler_cmd import cmd_labeler_gold_add

        return cmd_labeler_gold_add(args)
    if args.func == "labeler-gold-import":
        from apps.claims.cli.labeler_cmd import cmd_labeler_gold_import

        return cmd_labeler_gold_import(args)
    if args.func == "labeler-gold-status":
        from apps.claims.cli.labeler_cmd import cmd_labeler_gold_status

        return cmd_labeler_gold_status(args)
    if args.func == "labeler-gold-label":
        from apps.claims.cli.labeler_cmd import cmd_labeler_gold_label

        return cmd_labeler_gold_label(args)
    if args.func == "labeler-sample":
        from apps.claims.cli.labeler_cmd import cmd_labeler_sample

        return cmd_labeler_sample(args)
    if args.func == "labeler-dataset-freeze":
        from apps.claims.cli.labeler_cmd import cmd_labeler_dataset_freeze

        return cmd_labeler_dataset_freeze(args)
    if args.func == "labeler-train":
        from apps.claims.cli.labeler_cmd import cmd_labeler_train

        return cmd_labeler_train(args)
    if args.func == "labeler-eval":
        from apps.claims.cli.labeler_cmd import cmd_labeler_eval

        return cmd_labeler_eval(args)
    if args.func == "labeler-agent-eval":
        from apps.claims.cli.labeler_cmd import cmd_labeler_agent_eval

        return cmd_labeler_agent_eval(args)
    if args.func == "labeler-annotation-eval":
        from apps.claims.cli.labeler_cmd import cmd_labeler_annotation_eval

        return cmd_labeler_annotation_eval(args)
    if args.func == "labeler-promote":
        from apps.claims.cli.labeler_cmd import cmd_labeler_promote

        return cmd_labeler_promote(args)
    if args.func == "labeler-apply":
        from apps.claims.cli.labeler_cmd import cmd_labeler_apply

        return cmd_labeler_apply(args)
    if args.func == "labeler-models-list":
        from apps.claims.cli.labeler_cmd import cmd_labeler_models_list

        return cmd_labeler_models_list(args)

    # Embedder lifecycle
    if args.func == "embedder-intent-create":
        from apps.claims.cli.embedder_cmd import cmd_embedder_intent_create

        return cmd_embedder_intent_create(args)
    if args.func == "embedder-intent-list":
        from apps.claims.cli.embedder_cmd import cmd_embedder_intent_list

        return cmd_embedder_intent_list(args)
    if args.func == "embedder-intent-show":
        from apps.claims.cli.embedder_cmd import cmd_embedder_intent_show

        return cmd_embedder_intent_show(args)
    if args.func == "embedder-triplets-add":
        from apps.claims.cli.embedder_cmd import cmd_embedder_triplets_add

        return cmd_embedder_triplets_add(args)
    if args.func == "embedder-triplets-import-neighbors":
        from apps.claims.cli.embedder_cmd import cmd_embedder_triplets_import_neighbors

        return cmd_embedder_triplets_import_neighbors(args)
    if args.func == "embedder-triplets-import":
        from apps.claims.cli.embedder_cmd import cmd_embedder_triplets_import

        return cmd_embedder_triplets_import(args)
    if args.func == "embedder-sample":
        from apps.claims.cli.embedder_cmd import cmd_embedder_sample

        return cmd_embedder_sample(args)
    if args.func == "embedder-gold-sample":
        from apps.claims.cli.embedder_cmd import cmd_embedder_gold_sample

        return cmd_embedder_gold_sample(args)
    if args.func == "embedder-gold-add":
        from apps.claims.cli.embedder_cmd import cmd_embedder_gold_add

        return cmd_embedder_gold_add(args)
    if args.func == "embedder-gold-import":
        from apps.claims.cli.embedder_cmd import cmd_embedder_gold_import

        return cmd_embedder_gold_import(args)
    if args.func == "embedder-gold-status":
        from apps.claims.cli.embedder_cmd import cmd_embedder_gold_status

        return cmd_embedder_gold_status(args)
    if args.func == "embedder-gold-label":
        from apps.claims.cli.embedder_cmd import cmd_embedder_gold_label

        return cmd_embedder_gold_label(args)
    if args.func == "embedder-dataset-freeze":
        from apps.claims.cli.embedder_cmd import cmd_embedder_dataset_freeze

        return cmd_embedder_dataset_freeze(args)
    if args.func == "embedder-train":
        from apps.claims.cli.embedder_cmd import cmd_embedder_train

        return cmd_embedder_train(args)
    if args.func == "embedder-train-compare":
        from apps.claims.cli.embedder_cmd import cmd_embedder_train_compare

        return cmd_embedder_train_compare(args)
    if args.func == "embedder-eval":
        from apps.claims.cli.embedder_cmd import cmd_embedder_eval

        return cmd_embedder_eval(args)
    if args.func == "embedder-agent-eval":
        from apps.claims.cli.embedder_cmd import cmd_embedder_agent_eval

        return cmd_embedder_agent_eval(args)
    if args.func == "embedder-promote":
        from apps.claims.cli.embedder_cmd import cmd_embedder_promote

        return cmd_embedder_promote(args)

    from apps.claims.io import emit_json

    emit_json({"error": f"Unknown command: {args.func}"})
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
