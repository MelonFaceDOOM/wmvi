"""Prompt Lab entrypoint: Streamlit UI (default) or sample CLI.

Run from repo root::

  python -m apps.prompt_refinement
  python -m apps.prompt_refinement export-pool --corpus measles_bal --out ...
  python -m apps.prompt_refinement write-sample --from-ids ids.txt --pool ... --out ...
  python -m apps.prompt_refinement import-sample --from eval30.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _run_streamlit(extra: list[str]) -> int:
    root = Path(__file__).resolve().parents[2]
    app_file = root / "apps" / "prompt_refinement" / "app.py"
    os.environ["PYTHONPATH"] = str(root) + (
        os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else ""
    )
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_file),
        "--server.runOnSave=true",
    ]
    if extra:
        cmd += ["--"] + extra
    import subprocess

    return subprocess.call(cmd)


def _cmd_export_pool(args: argparse.Namespace) -> int:
    from apps.prompt_refinement import eval_sample as es

    rows = es.flatten_corpus_chunks(
        corpus=str(args.corpus),
        standalone_ann=str(args.standalone_ann),
    )
    out = Path(args.out)
    summary = es.write_pool_json(rows, out, corpus=str(args.corpus))
    payload = {"ok": True, "out": str(out), "summary": summary}
    print(json.dumps(payload, ensure_ascii=False))
    if args.human:
        print(
            f"pool  n={summary['n_chunks']}  "
            f"standalone_0={summary['n_has_standalone_0']}  "
            f"platforms={summary['by_platform']}",
            file=sys.stderr,
        )
    return 0


def _cmd_write_sample(args: argparse.Namespace) -> int:
    from apps.prompt_refinement import eval_sample as es

    ids = es.read_task_ids(Path(args.from_ids))
    if not ids:
        print(json.dumps({"error": "no task_ids in --from-ids"}))
        return 1
    result = es.write_sample_from_ids(
        pool_path=Path(args.pool),
        ids=ids,
        out=Path(args.out),
    )
    print(json.dumps({"ok": True, **result}, ensure_ascii=False))
    if args.human:
        print(f"sample  n={result['n']}  out={result['out']}", file=sys.stderr)
    return 0


def _cmd_import_sample(args: argparse.Namespace) -> int:
    from apps.prompt_refinement import eval_sample as es
    from apps.prompt_refinement import db as db_mod

    result = es.import_sample_to_lab(
        sample_path=Path(args.from_path if hasattr(args, "from_path") else args.__dict__["from"]),
        db_path=Path(args.db) if args.db else None,
        clear_existing=bool(args.clear),
    )
    print(json.dumps({"ok": True, **result}, ensure_ascii=False))
    if args.human:
        print(
            f"import  inserted={result['inserted']}  skipped={result['skipped']}  "
            f"problem_posts={result['n_problem']}  db={result['db']}",
            file=sys.stderr,
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("--",) or argv[0].startswith("-") and argv[0] not in (
        "export-pool",
        "write-sample",
        "import-sample",
        "-h",
        "--help",
    ):
        # No subcommand → Streamlit (pass through any streamlit flags after --)
        extra: list[str] = []
        if argv and argv[0] == "--":
            extra = argv[1:]
        elif argv and argv[0].startswith("-"):
            extra = argv
        return _run_streamlit(extra)

    ap = argparse.ArgumentParser(description="Prompt Lab CLI / Streamlit")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ep = sub.add_parser("export-pool", help="Flatten corpus chunks + standalone flags")
    ep.add_argument("--corpus", required=True)
    ep.add_argument(
        "--out",
        type=Path,
        default=Path("apps/prompt_refinement/data/samples/pool.json"),
    )
    ep.add_argument("--standalone-ann", default="standalone_pred_m1")
    ep.add_argument("--human", action="store_true")
    ep.set_defaults(func=_cmd_export_pool)

    ws = sub.add_parser("write-sample", help="Write eval sample from selected task_ids")
    ws.add_argument("--from-ids", required=True, type=Path)
    ws.add_argument("--pool", required=True, type=Path)
    ws.add_argument(
        "--out",
        type=Path,
        default=Path("apps/prompt_refinement/data/samples/eval30.json"),
    )
    ws.add_argument("--human", action="store_true")
    ws.set_defaults(func=_cmd_write_sample)

    im = sub.add_parser("import-sample", help="Import sample into lab problem_posts")
    im.add_argument("--from", dest="from_path", required=True, type=Path)
    im.add_argument("--db", type=Path, default=None)
    im.add_argument("--clear", action="store_true", help="Clear existing problem_posts first")
    im.add_argument("--human", action="store_true")
    im.set_defaults(func=_cmd_import_sample)

    args = ap.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
