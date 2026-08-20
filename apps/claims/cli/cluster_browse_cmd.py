"""Launch the cluster browser Streamlit UI for one clustering output."""

from __future__ import annotations

import os
import sys
from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io


def cmd_cluster_browse(args: Namespace) -> int:
    extra: list[str] = []
    from_path = getattr(args, "from_path", None)
    labels = getattr(args, "labels", None)
    if from_path is None and labels is None:
        claims_io.emit_json({"error": "Provide --from (experiment dir/JSON) or --labels"})
        return 1
    if from_path is not None:
        extra += ["--from", str(Path(from_path))]
        os.environ["WMVI_CLUSTER_BROWSE_FROM"] = str(Path(from_path).resolve())
    if labels is not None:
        extra += ["--labels", str(Path(labels))]
    parent_labels = getattr(args, "parent_labels", None)
    if parent_labels is not None:
        extra += ["--parent-labels", str(Path(parent_labels))]
    if getattr(args, "corpus", None):
        extra += ["--corpus", str(args.corpus)]
    if getattr(args, "model_tag", None):
        extra += ["--model-tag", str(args.model_tag)]
    if getattr(args, "run_dir", None) is not None:
        extra += ["--run-dir", str(Path(args.run_dir))]
    if getattr(args, "claims", None) is not None:
        extra += ["--claims", str(Path(args.claims))]
    if getattr(args, "selection", None):
        extra += ["--selection", str(args.selection)]
    for spec in getattr(args, "filter", None) or []:
        extra += ["--filter", str(spec)]
    if getattr(args, "where_annotation", None):
        extra += ["--where-annotation", str(args.where_annotation)]
        if getattr(args, "eq", None) is not None:
            extra += ["--eq", str(args.eq)]
        if getattr(args, "low", None) is not None:
            extra += ["--low", str(args.low)]
        if getattr(args, "high", None) is not None:
            extra += ["--high", str(args.high)]

    root = Path(__file__).resolve().parents[3]
    app_file = root / "apps" / "claims" / "cluster_browser_app.py"
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
    cmd += ["--"] + extra
    try:
        import subprocess

        return int(subprocess.call(cmd))
    except FileNotFoundError:
        claims_io.emit_json({"error": "cluster-browse requires streamlit (pip install streamlit)"})
        return 1
