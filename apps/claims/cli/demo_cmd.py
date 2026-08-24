"""CLI: python -m apps.claims demo [massage|pack|serve]."""

from __future__ import annotations

import os
import subprocess
import sys
from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims.demo import DEFAULT_MODEL
from apps.claims.demo.catalog import (
    DEFAULT_CLAIMS,
    DEFAULT_EXP_DIR,
    DEFAULT_RUN,
    resolve_bundle_path,
)


def add_demo_parser(sub) -> None:
    demo_p = sub.add_parser(
        "demo",
        help="Measles2 hierarchy demo: massage titles, pack sqlite, serve Streamlit",
    )
    demo_sub = demo_p.add_subparsers(dest="demo_cmd")

    serve_p = demo_sub.add_parser("serve", help="Launch Streamlit UI from a sqlite bundle")
    serve_p.add_argument("--bundle", type=Path, default=None, help="measles2_demo.sqlite")
    serve_p.add_argument("--port", type=int, default=None)
    serve_p.set_defaults(func="demo")

    mass_p = demo_sub.add_parser("massage", help="LLM-name narratives/leaves or reassign membership")
    mass_sub = mass_p.add_subparsers(dest="massage_cmd", required=True)
    for name, help_ in (
        ("name-narratives", "Name all narratives in one/few API calls"),
        ("name-leaves", "Name leaves in batches (resumable)"),
        ("reassign", "Reassign leaves to narratives (resumable)"),
    ):
        sp = mass_sub.add_parser(name, help=help_)
        sp.add_argument("--exp-dir", type=Path, default=DEFAULT_EXP_DIR)
        sp.add_argument("--model", type=str, default=DEFAULT_MODEL)
        if name != "name-narratives":
            sp.add_argument("--batch-size", type=int, default=20)
            sp.add_argument("--limit", type=int, default=None, help="Max items this run (debug)")
        sp.set_defaults(func="demo")

    pack_p = demo_sub.add_parser("pack", help="Build measles2_demo.sqlite")
    pack_p.add_argument("--exp-dir", type=Path, default=DEFAULT_EXP_DIR)
    pack_p.add_argument("--claims", type=Path, default=DEFAULT_CLAIMS)
    pack_p.add_argument("--run-dir", type=Path, default=DEFAULT_RUN)
    pack_p.add_argument("--out", type=Path, default=None)
    pack_p.set_defaults(func="demo")

    demo_p.add_argument("--bundle", type=Path, default=None, help="When no subcommand: serve this sqlite")
    demo_p.add_argument("--port", type=int, default=None)
    demo_p.set_defaults(func="demo")



def cmd_demo(args: Namespace) -> int:
    which = getattr(args, "demo_cmd", None) or "serve"
    if which == "massage":
        return _massage(args)
    if which == "pack":
        return _pack(args)
    return _serve(args)


def _massage(args: Namespace) -> int:
    from apps.claims.demo import massage as massage_mod

    exp = Path(args.exp_dir)
    model = str(args.model or DEFAULT_MODEL)
    step = str(args.massage_cmd)
    try:
        if step == "name-narratives":
            out = massage_mod.name_narratives(exp, model=model)
        elif step == "name-leaves":
            out = massage_mod.name_leaves(
                exp,
                model=model,
                batch_size=int(args.batch_size),
                limit=args.limit,
            )
        elif step == "reassign":
            out = massage_mod.reassign_leaves(
                exp,
                model=model,
                batch_size=int(args.batch_size),
                limit=args.limit,
            )
        else:
            claims_io.emit_json({"error": f"unknown massage step {step}"})
            return 1
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json({"ok": True, "step": step, **out})
    return 0


def _pack(args: Namespace) -> int:
    from apps.claims.demo.pack import pack_bundle

    try:
        out = pack_bundle(
            exp_dir=Path(args.exp_dir),
            claims_path=Path(args.claims),
            run_dir=Path(args.run_dir),
            out_path=Path(args.out) if args.out else None,
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json({"ok": True, **out})
    return 0


def _serve(args: Namespace) -> int:
    bundle = resolve_bundle_path(getattr(args, "bundle", None))
    if not bundle.is_file():
        claims_io.emit_json(
            {
                "error": (
                    f"bundle not found: {bundle}. "
                    "Pass the packed sqlite path, or run: python -m apps.claims demo pack"
                )
            }
        )
        return 1
    root = Path(__file__).resolve().parents[3]
    app_file = root / "apps" / "claims" / "demo" / "app.py"
    os.environ["WMVI_DEMO_BUNDLE"] = str(bundle.resolve())
    os.environ["PYTHONPATH"] = str(root) + (
        os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else ""
    )
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_file),
        "--server.runOnSave=false",
        "--server.fileWatcherType=none",
    ]
    port = getattr(args, "port", None)
    if port:
        cmd += ["--server.port", str(int(port))]
    cmd += ["--", "--bundle", str(bundle.resolve())]
    try:
        return int(subprocess.call(cmd))
    except FileNotFoundError:
        claims_io.emit_json({"error": "demo requires streamlit (pip install streamlit)"})
        return 1
