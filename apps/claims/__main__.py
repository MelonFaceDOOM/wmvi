"""CLI entry: python -m apps.claims <command> ..."""

from __future__ import annotations

import argparse
from pathlib import Path


def _add_corpus_args(p: argparse.ArgumentParser, *, with_model_tag: bool = False) -> None:
    p.add_argument(
        "--corpus",
        type=str,
        default=None,
        help="Corpus slug under data/inputs/<slug>/ (fills default paths)",
    )
    if with_model_tag:
        p.add_argument(
            "--model-tag",
            type=str,
            default=None,
            help="Tag for run name <corpus>__<tag> under data/runs/ (default: derived from --model)",
        )


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="python -m apps.claims",
        description="Claims pipeline (file-mode): extract, group, embed, train, cluster.",
    )
    sub = ap.add_subparsers(dest="command", required=True)

    # --- corpus ---
    corpus_p = sub.add_parser("corpus", help="Create/list/seed/status corpora under data/inputs/")
    corpus_sub = corpus_p.add_subparsers(dest="corpus_cmd", required=True)
    c_create = corpus_sub.add_parser("create", help="Create a corpus directory + NOTES.md")
    c_create.add_argument("--name", type=str, required=True, help="Slug, e.g. measles")
    c_create.add_argument("--notes", type=str, default=None, help="Optional NOTES.md body")
    c_create.set_defaults(func="corpus-create")
    c_list = corpus_sub.add_parser("list", help="List corpora and artifact status")
    c_list.set_defaults(func="corpus-list")
    c_status = corpus_sub.add_parser("status", help="Detailed status for one corpus")
    c_status.add_argument("--name", type=str, required=True)
    c_status.set_defaults(func="corpus-status")
    c_seed = corpus_sub.add_parser(
        "seed",
        help="Fetch posts for search terms (+ date range) into data/inputs/<name>/posts.json",
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

    # --- model ---
    model_p = sub.add_parser("model", help="Register/list local embedder models under data/models/")
    model_sub = model_p.add_subparsers(dest="model_cmd", required=True)
    m_reg = model_sub.add_parser("register", help="Symlink/copy a model dir to data/models/<tag>")
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
    val_p = sub.add_parser("validate", help="Summarize posts-with-claims JSON")
    _add_corpus_args(val_p)
    val_p.add_argument("--claims", type=Path, default=None)
    val_p.add_argument("--human", action="store_true", help="Also print human summary")
    val_p.set_defaults(func="validate")

    # --- prepare ---
    prepare_p = sub.add_parser("prepare", help="Pre-extract stages: trim / coref")
    prepare_sub = prepare_p.add_subparsers(dest="prepare_cmd", required=True)
    p_trim = prepare_sub.add_parser("trim", help="Sentence-boundary trim around hit spans")
    _add_corpus_args(p_trim)
    p_trim.add_argument("--posts", type=Path, default=None)
    p_trim.add_argument("--out", type=Path, default=None)
    p_trim.add_argument(
        "--force",
        action="store_true",
        help="With --corpus and no --out, overwrite posts.json",
    )
    p_trim.set_defaults(func="prepare-trim")
    p_coref = prepare_sub.add_parser("coref", help="Coreference resolution on posts JSON")
    _add_corpus_args(p_coref)
    p_coref.add_argument("--posts", type=Path, default=None)
    p_coref.add_argument("--out", type=Path, default=None)
    p_coref.add_argument("--batch-size", type=int, default=None)
    p_coref.add_argument(
        "--force",
        action="store_true",
        help="With --corpus and no --out, overwrite posts.json",
    )
    p_coref.set_defaults(func="prepare-coref")

    # --- group ---
    group_p = sub.add_parser("group", help="Collapse duplicate claims into groups JSON")
    _add_corpus_args(group_p)
    group_p.add_argument("--claims", type=Path, default=None, help="posts-with-claims JSON")
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
    embed_p.add_argument("--limit", type=int, default=None, help="Embed only first N groups")
    embed_p.add_argument("--force", action="store_true", help="Overwrite existing run dir")
    embed_p.set_defaults(func="embed")

    # --- cluster ---
    cluster_p = sub.add_parser("cluster", help="Run one clustering config on a run dir")
    _add_corpus_args(cluster_p, with_model_tag=True)
    cluster_p.add_argument("--run-dir", type=Path, default=None)
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
    sweep_p.add_argument("--configs", type=Path, required=True)
    sweep_p.add_argument("--seed", type=int, default=0)
    sweep_p.add_argument("--out-dir", type=Path, default=None)
    sweep_p.add_argument("--queries", type=Path, default=None)
    sweep_p.set_defaults(func="sweep")

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

    # --- extract ---
    extract_p = sub.add_parser("extract", help="Extract claims from posts JSON (network)")
    _add_corpus_args(extract_p)
    extract_p.add_argument("--posts", type=Path, default=None)
    extract_p.add_argument("--out", type=Path, default=None)
    extract_p.add_argument("--n-posts", type=int, default=0)
    extract_p.add_argument("--claims-only", action="store_true")
    extract_p.set_defaults(func="extract")

    # --- ls-artifacts ---
    ls_p = sub.add_parser("ls-artifacts", help="List models/labels/runs/corpora under data/")
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
    if args.func == "prepare-trim":
        from apps.claims.cli.validate_cmd import cmd_prepare_trim

        return cmd_prepare_trim(args)
    if args.func == "prepare-coref":
        from apps.claims.cli.validate_cmd import cmd_prepare_coref

        return cmd_prepare_coref(args)
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
    if args.func == "prep-queries":
        from apps.claims.cli.cluster_cmd import cmd_prep_queries

        return cmd_prep_queries(args)
    if args.func == "sweep":
        from apps.claims.cli.cluster_cmd import cmd_sweep

        return cmd_sweep(args)
    if args.func == "doctor":
        from apps.claims.cli.cluster_cmd import cmd_doctor

        return cmd_doctor(args)
    if args.func == "train":
        from apps.claims.cli.train_cmd import cmd_train

        return cmd_train(args)
    if args.func == "eval-triplets":
        from apps.claims.cli.train_cmd import cmd_eval_triplets

        return cmd_eval_triplets(args)
    if args.func == "discover-triplets":
        from apps.claims.cli.train_cmd import cmd_discover_triplets

        return cmd_discover_triplets(args)
    if args.func == "extract":
        from apps.claims.cli.extract_cmd import cmd_extract

        return cmd_extract(args)

    from apps.claims.io import emit_json

    emit_json({"error": f"Unknown command: {args.func}"})
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
