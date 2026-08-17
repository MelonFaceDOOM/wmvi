from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims.cli import paths as path_helpers


def cmd_runs_list(args: Namespace) -> int:
    corpus_filter = getattr(args, "corpus", None)
    slug_filter = corpus_mod.validate_slug(str(corpus_filter)) if corpus_filter else None

    runs: list[dict] = []
    root = claims_io.runs_dir()
    if root.is_dir():
        for corpus_dir in sorted(root.iterdir()):
            if not corpus_dir.is_dir() or corpus_dir.name.startswith("."):
                continue
            if slug_filter and corpus_dir.name != slug_filter:
                continue
            for p in sorted(corpus_dir.iterdir()):
                if not p.is_dir() or p.name.startswith("."):
                    continue
                entry: dict = {
                    "name": f"{corpus_dir.name}/{p.name}",
                    "corpus": corpus_dir.name,
                    "tag": p.name,
                    "path": str(p),
                }
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
    clustering = claims_io.clustering_experiments_dir()
    if clustering.is_dir():
        for corpus_dir in sorted(clustering.iterdir()):
            if not corpus_dir.is_dir() or corpus_dir.name.startswith("."):
                continue
            if slug_filter and corpus_dir.name != slug_filter:
                continue
            for tag_dir in sorted(corpus_dir.iterdir()):
                if not tag_dir.is_dir() or tag_dir.name.startswith("."):
                    continue
                for exp in sorted(tag_dir.iterdir()):
                    if exp.is_dir() and not exp.name.startswith("."):
                        experiments.append(
                            {
                                "run": f"{corpus_dir.name}/{tag_dir.name}",
                                "name": exp.name,
                                "path": str(exp),
                            }
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
                run_name = f"{slug}/{tag}"
            else:
                raise ValueError("Provide --run-name, or --corpus and --model-tag")
        # Normalize legacy corpus__tag
        raw = str(run_name)
        if "__" in raw and "/" not in raw:
            slug, _, tag = raw.partition("__")
            run_name = f"{slug}/{tag}"
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
        return corpus.run_dir(tag)
    raise ValueError("Provide --run-dir, or --corpus and --model-tag")
