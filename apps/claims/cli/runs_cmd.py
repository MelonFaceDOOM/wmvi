from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims.cli import paths as path_helpers


def cmd_runs_list(args: Namespace) -> int:
    corpus_filter = getattr(args, "corpus", None)
    prefix = None
    if corpus_filter:
        slug = corpus_mod.validate_slug(str(corpus_filter))
        prefix = f"{slug}__"

    runs: list[dict] = []
    root = claims_io.runs_dir()
    if root.is_dir():
        for p in sorted(root.iterdir()):
            if not p.is_dir() or p.name.startswith("."):
                continue
            if prefix and not p.name.startswith(prefix):
                continue
            entry: dict = {"name": p.name, "path": str(p)}
            metrics_path = p / claims_io.METRICS_FILE
            if metrics_path.is_file():
                try:
                    m = claims_io.read_json(metrics_path)
                    entry["claim_count"] = m.get("claim_count")
                    entry["vector_dim"] = m.get("vector_dim")
                    entry["model_id"] = m.get("model_id")
                    entry["wall_seconds"] = m.get("wall_seconds")
                except Exception:  # noqa: BLE001
                    pass
            vectors = p / claims_io.VECTORS_FILE
            if vectors.is_file():
                entry["vectors_bytes"] = vectors.stat().st_size
            runs.append(entry)

    experiments: list[dict] = []
    exp_root = claims_io.experiments_dir()
    if exp_root.is_dir():
        for run_root in sorted(exp_root.iterdir()):
            if not run_root.is_dir():
                continue
            if prefix and not run_root.name.startswith(prefix):
                continue
            for exp in sorted(run_root.iterdir()):
                if exp.is_dir():
                    experiments.append(
                        {"run": run_root.name, "name": exp.name, "path": str(exp)}
                    )

    claims_io.emit_json(
        {
            "corpus": str(corpus_filter) if corpus_filter else None,
            "runs": runs,
            "n_runs": len(runs),
            "experiments": experiments,
            "n_experiments": len(experiments),
        }
    )
    return 0


def cmd_runs_export(args: Namespace) -> int:
    from apps.claims import runs_xfer

    try:
        run_dir = _resolve_run_dir(args)
        out = Path(args.out)
        summary = runs_xfer.export_run(run_dir=run_dir, out_zip=out)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(summary)
    return 0


def cmd_runs_import(args: Namespace) -> int:
    from apps.claims import runs_xfer

    try:
        run_name = getattr(args, "run_name", None)
        if not run_name:
            if getattr(args, "corpus", None) and getattr(args, "model_tag", None):
                slug = corpus_mod.validate_slug(str(args.corpus))
                tag = corpus_mod.validate_model_tag(str(args.model_tag))
                run_name = f"{slug}__{tag}"
            else:
                raise ValueError("Provide --run-name, or --corpus and --model-tag")
        summary = runs_xfer.import_run(
            from_zip=Path(args.from_zip),
            run_name=str(run_name),
            force=bool(getattr(args, "force", False)),
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(summary)
    return 0


def _resolve_run_dir(args: Namespace) -> Path:
    if getattr(args, "run_dir", None):
        return Path(args.run_dir)
    if getattr(args, "corpus", None):
        corpus = path_helpers.require_corpus(args)
        tag = path_helpers.resolve_model_tag(args)
        return claims_io.runs_dir() / f"{corpus.slug}__{tag}"
    raise ValueError("Provide --run-dir, or --corpus and --model-tag")
