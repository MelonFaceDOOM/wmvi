from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims import models as models_mod


def cmd_model_register(args: Namespace) -> int:
    try:
        result = models_mod.register_model(
            path=Path(args.path),
            tag=str(args.tag),
            mode=str(getattr(args, "mode", "symlink") or "symlink"),
            force=bool(getattr(args, "force", False)),
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json({"ok": True, **result})
    return 0


def cmd_model_list(_args: Namespace | None = None) -> int:
    rows = models_mod.list_models()
    claims_io.emit_json({"models": rows, "n": len(rows)})
    return 0


def cmd_model_resolve(args: Namespace) -> int:
    try:
        resolved = models_mod.resolve_model(str(args.model))
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json({"ok": True, "input": str(args.model), "resolved": resolved})
    return 0
