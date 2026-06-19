"""Export / import embedding runs between machines (vectors + index + run metadata only).

Bundles are zip archives or directories containing ``manifest.json`` plus the
artifact files from an embed run. Clustering, cluster names, and triplet eval are
not included.

Usage::

  python -m apps.claim_extractor.embedding_lab.transfer list
  python -m apps.claim_extractor.embedding_lab.transfer export --run-id 1 --out run.embed.zip
  python -m apps.claim_extractor.embedding_lab.transfer import run.embed.zip
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from apps.claim_extractor.embedding_lab import db, embed_runner

FORMAT_VERSION = 1
BUNDLE_FILES = (
    embed_runner.VECTORS_FILE,
    embed_runner.INDEX_FILE,
    embed_runner.METRICS_FILE,
)

ConflictAction = Literal["overwrite", "rename", "abort"]


@dataclass(frozen=True)
class ProfileSpec:
    name: str
    model_id: str
    doc_instruction: str
    query_instruction: str
    normalize: bool

    @classmethod
    def from_profile(cls, profile: db.EmbedProfile) -> ProfileSpec:
        return cls(
            name=profile.name,
            model_id=profile.model_id,
            doc_instruction=profile.doc_instruction,
            query_instruction=profile.query_instruction,
            normalize=profile.normalize,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProfileSpec:
        return cls(
            name=str(data["name"]),
            model_id=str(data["model_id"]),
            doc_instruction=str(data.get("doc_instruction") or ""),
            query_instruction=str(data.get("query_instruction") or ""),
            normalize=bool(data.get("normalize", True)),
        )


def profiles_match(a: ProfileSpec, b: ProfileSpec) -> bool:
    return (
        a.model_id == b.model_id
        and a.doc_instruction == b.doc_instruction
        and a.query_instruction == b.query_instruction
        and a.normalize == b.normalize
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _artifact_dir_for(profile_id: int, source_hash: str) -> Path:
    return db.artifacts_root() / f"profile_{profile_id}" / f"run_{source_hash[:12]}"


def _run_label(run: dict[str, Any]) -> str:
    unique = run.get("claim_count", 0)
    total = run.get("source_claim_count")
    counts = f"{unique:,} unique"
    if total and int(total) != int(unique):
        counts = f"{unique:,} unique / {int(total):,} source"
    return (
        f"run {run['id']}: {run.get('profile_name', '?')} · {counts} · "
        f"{run.get('model_id') or run.get('device', '')} · {run.get('created_at', '')}"
    )


def _resolve_run(conn, *, run_id: int | None, profile_name: str | None) -> tuple[dict[str, Any], db.EmbedProfile]:
    if run_id is not None:
        run = db.get_embed_run(conn, run_id)
        if run is None:
            raise SystemExit(f"Embed run id={run_id} not found.")
        profile = db.get_embed_profile(conn, int(run["profile_id"]))
        if profile is None:
            raise SystemExit(f"Profile id={run['profile_id']} not found for run {run_id}.")
        return run, profile

    if not profile_name:
        raise SystemExit("Specify --run-id or --profile-name.")

    profile = db.get_embed_profile_by_name(conn, profile_name)
    if profile is None:
        raise SystemExit(f"Profile {profile_name!r} not found.")

    runs = db.list_embed_runs(conn, profile_id=profile.id)
    if not runs:
        raise SystemExit(f"No runs for profile {profile_name!r}.")
    if len(runs) > 1:
        raise SystemExit(
            f"Profile {profile_name!r} has {len(runs)} runs; use --run-id. "
            f"Candidates: {', '.join(str(r['id']) for r in runs)}"
        )
    return runs[0], profile


def _build_manifest(*, profile: db.EmbedProfile, run: dict[str, Any]) -> dict[str, Any]:
    metrics_path = Path(str(run["artifact_dir"])) / embed_runner.METRICS_FILE
    metrics: dict[str, Any] = {}
    if metrics_path.is_file():
        try:
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            metrics = {}

    return {
        "format_version": FORMAT_VERSION,
        "exported_at": _utc_now_iso(),
        "profile": asdict(ProfileSpec.from_profile(profile)),
        "run": {
            "source_hash": str(run["source_hash"]),
            "source_path": str(run.get("source_path") or ""),
            "claim_count": int(run.get("claim_count") or 0),
            "source_claim_count": run.get("source_claim_count"),
            "vector_dim": run.get("vector_dim"),
            "dtype": str(run.get("dtype") or "float32"),
            "device": run.get("device"),
            "wall_seconds": run.get("wall_seconds"),
            "claims_per_sec": run.get("claims_per_sec"),
            "peak_ram_mb": run.get("peak_ram_mb"),
            "ram_delta_mb": run.get("ram_delta_mb"),
            "peak_gpu_mb": run.get("peak_gpu_mb"),
            "artifact_bytes": run.get("artifact_bytes"),
        },
        "metrics": metrics,
    }


def export_run(
    *,
    conn,
    run_id: int | None = None,
    profile_name: str | None = None,
    out_path: Path,
) -> Path:
    run, profile = _resolve_run(conn, run_id=run_id, profile_name=profile_name)
    artifact_dir = Path(str(run["artifact_dir"]))
    if not artifact_dir.is_dir():
        raise SystemExit(f"Artifact directory missing: {artifact_dir}")

    missing = [name for name in BUNDLE_FILES if not (artifact_dir / name).is_file()]
    if embed_runner.VECTORS_FILE in missing or embed_runner.INDEX_FILE in missing:
        raise SystemExit(f"Required artifact file(s) missing in {artifact_dir}: {missing}")

    manifest = _build_manifest(profile=profile, run=run)
    out_path = out_path.expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.suffix == ".zip" or str(out_path).endswith(".embed.zip"):
        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
            for name in BUNDLE_FILES:
                src = artifact_dir / name
                if src.is_file():
                    zf.write(src, arcname=name)
    else:
        out_path.mkdir(parents=True, exist_ok=True)
        (out_path / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        for name in BUNDLE_FILES:
            src = artifact_dir / name
            if src.is_file():
                shutil.copy2(src, out_path / name)

    return out_path


def _load_bundle_dir(bundle_dir: Path) -> tuple[dict[str, Any], Path]:
    bundle_dir = bundle_dir.resolve()
    manifest_path = bundle_dir / "manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(f"manifest.json not found in {bundle_dir}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if int(manifest.get("format_version", 0)) != FORMAT_VERSION:
        raise SystemExit(f"Unsupported bundle format version: {manifest.get('format_version')}")
    for name in (embed_runner.VECTORS_FILE, embed_runner.INDEX_FILE):
        if not (bundle_dir / name).is_file():
            raise SystemExit(f"Required bundle file missing: {name}")
    return manifest, bundle_dir


def _extract_zip(bundle_path: Path) -> Path:
    tmp = bundle_path.parent / f".{bundle_path.stem}_import_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    with zipfile.ZipFile(bundle_path, "r") as zf:
        zf.extractall(tmp)
    return tmp


def _cleanup_tmp(tmp: Path | None) -> None:
    if tmp is not None and tmp.name.endswith("_import_tmp") and tmp.is_dir():
        shutil.rmtree(tmp, ignore_errors=True)


def _prompt_conflict(message: str, *, allow_rename: bool = True) -> ConflictAction:
    if not sys.stdin.isatty():
        raise SystemExit(f"{message} Re-run with --on-conflict overwrite|rename|abort.")

    print(message)
    print("  [o] Overwrite")
    if allow_rename:
        print("  [r] Rename profile")
    print("  [a] Abort")
    while True:
        choice = input("Choice [o/r/a]: ").strip().lower() or "a"
        if choice in ("o", "overwrite"):
            return "overwrite"
        if allow_rename and choice in ("r", "rename"):
            return "rename"
        if choice in ("a", "abort", "q"):
            return "abort"
        print("Invalid choice.")


def _unique_profile_name(conn, base: str) -> str:
    base = base.strip() or "imported"
    if db.get_embed_profile_by_name(conn, base) is None:
        return base
    n = 2
    while True:
        candidate = f"{base}-{n}"
        if db.get_embed_profile_by_name(conn, candidate) is None:
            return candidate
        n += 1


def _resolve_import_profile(
    conn,
    spec: ProfileSpec,
    *,
    on_conflict: ConflictAction | None,
    profile_name: str | None,
) -> tuple[int, ProfileSpec]:
    target_name = (profile_name or spec.name).strip()
    existing = db.get_embed_profile_by_name(conn, target_name)
    imported = ProfileSpec(
        name=target_name,
        model_id=spec.model_id,
        doc_instruction=spec.doc_instruction,
        query_instruction=spec.query_instruction,
        normalize=spec.normalize,
    )

    if existing is None:
        pid = db.create_embed_profile(
            conn,
            name=imported.name,
            model_id=imported.model_id,
            doc_instruction=imported.doc_instruction,
            query_instruction=imported.query_instruction,
            normalize=imported.normalize,
        )
        return pid, imported

    existing_spec = ProfileSpec.from_profile(existing)
    if profiles_match(existing_spec, imported):
        return existing.id, imported

    action = on_conflict or _prompt_conflict(
        f"Profile {target_name!r} exists with different settings.",
        allow_rename=True,
    )
    if action == "abort":
        raise SystemExit("Import aborted.")
    if action == "rename":
        new_name = profile_name
        if not new_name or new_name == target_name:
            if sys.stdin.isatty():
                new_name = input(f"New profile name [{target_name}-imported]: ").strip()
            if not new_name:
                new_name = _unique_profile_name(conn, f"{target_name}-imported")
        imported = ProfileSpec(
            name=new_name,
            model_id=spec.model_id,
            doc_instruction=spec.doc_instruction,
            query_instruction=spec.query_instruction,
            normalize=spec.normalize,
        )
        pid = db.create_embed_profile(
            conn,
            name=imported.name,
            model_id=imported.model_id,
            doc_instruction=imported.doc_instruction,
            query_instruction=imported.query_instruction,
            normalize=imported.normalize,
        )
        return pid, imported

    # overwrite profile settings
    db.update_embed_profile(
        conn,
        existing.id,
        name=target_name,
        model_id=imported.model_id,
        doc_instruction=imported.doc_instruction,
        query_instruction=imported.query_instruction,
        normalize=imported.normalize,
    )
    return existing.id, imported


def import_run(
    *,
    conn,
    bundle_path: Path,
    on_conflict: ConflictAction | None = None,
    profile_name: str | None = None,
) -> int:
    bundle_path = bundle_path.expanduser().resolve()
    if not bundle_path.exists():
        raise SystemExit(f"Bundle not found: {bundle_path}")

    tmp: Path | None = None
    try:
        if bundle_path.is_file() and zipfile.is_zipfile(bundle_path):
            tmp = _extract_zip(bundle_path)
            manifest, bundle_dir = _load_bundle_dir(tmp)
        elif bundle_path.is_dir():
            manifest, bundle_dir = _load_bundle_dir(bundle_path)
        else:
            raise SystemExit(f"Bundle must be a .zip file or directory: {bundle_path}")

        spec = ProfileSpec.from_dict(manifest["profile"])
        run_meta = manifest.get("run") or {}
        source_hash = str(run_meta["source_hash"])
        metrics = dict(manifest.get("metrics") or {})

        profile_id, resolved_spec = _resolve_import_profile(
            conn,
            spec,
            on_conflict=on_conflict,
            profile_name=profile_name,
        )

        existing_run = db.get_embed_run_for(conn, profile_id, source_hash)
        if existing_run is not None:
            action = on_conflict or _prompt_conflict(
                f"Run already exists for profile {resolved_spec.name!r} and source_hash {source_hash[:12]}…",
                allow_rename=True,
            )
            if action == "abort":
                raise SystemExit("Import aborted.")
            if action == "rename":
                new_name = profile_name
                if not new_name:
                    new_name = _unique_profile_name(conn, f"{resolved_spec.name}-imported")
                profile_id, resolved_spec = _resolve_import_profile(
                    conn,
                    spec,
                    on_conflict="rename",
                    profile_name=new_name,
                )
            # overwrite: keep profile_id, upsert will replace run row + we replace files

        dest_dir = _artifact_dir_for(profile_id, source_hash)
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        for name in BUNDLE_FILES:
            src = bundle_dir / name
            if src.is_file():
                shutil.copy2(src, dest_dir / name)

        if not metrics:
            metrics_path = dest_dir / embed_runner.METRICS_FILE
            if metrics_path.is_file():
                try:
                    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    metrics = {}

        run_id = db.upsert_embed_run(
            conn,
            profile_id=profile_id,
            source_hash=source_hash,
            source_path=str(run_meta.get("source_path") or ""),
            claim_count=int(run_meta.get("claim_count") or 0),
            source_claim_count=run_meta.get("source_claim_count"),
            vector_dim=int(run_meta.get("vector_dim") or 0),
            dtype=str(run_meta.get("dtype") or "float32"),
            artifact_dir=str(dest_dir),
            metrics={
                "device": run_meta.get("device"),
                "wall_seconds": run_meta.get("wall_seconds"),
                "claims_per_sec": run_meta.get("claims_per_sec"),
                "peak_ram_mb": run_meta.get("peak_ram_mb"),
                "ram_delta_mb": run_meta.get("ram_delta_mb"),
                "peak_gpu_mb": run_meta.get("peak_gpu_mb"),
                "artifact_bytes": run_meta.get("artifact_bytes"),
                **metrics,
            },
        )
        return run_id
    finally:
        _cleanup_tmp(tmp)


def cmd_list(conn) -> None:
    runs = db.list_embed_runs(conn)
    if not runs:
        print("No embedding runs.")
        return
    for run in runs:
        print(_run_label(run))
        print(f"  artifact_dir: {run.get('artifact_dir')}")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="Export/import embedding lab runs (vectors + index + metadata only)."
    )
    ap.add_argument(
        "--db",
        type=Path,
        default=None,
        help=f"SQLite path (default: {db.default_db_path()})",
    )
    sub = ap.add_subparsers(dest="command", required=True)

    list_p = sub.add_parser("list", help="List embedding runs available for export")
    list_p.set_defaults(func="list")

    export_p = sub.add_parser("export", help="Export one embedding run to a zip or directory")
    export_p.add_argument("--run-id", type=int, default=None)
    export_p.add_argument("--profile-name", type=str, default=None)
    export_p.add_argument("--out", type=Path, required=True, help="Output .zip or directory")
    export_p.set_defaults(func="export")

    import_p = sub.add_parser("import", help="Import an embedding bundle into this lab DB")
    import_p.add_argument("bundle", type=Path, help=".zip bundle or directory")
    import_p.add_argument(
        "--on-conflict",
        choices=("overwrite", "rename", "abort"),
        default=None,
        help="When profile/run already exists (default: prompt if TTY else abort)",
    )
    import_p.add_argument(
        "--profile-name",
        type=str,
        default=None,
        help="Destination profile name (for rename / explicit naming)",
    )
    import_p.set_defaults(func="import")

    args = ap.parse_args(argv)
    db_path = args.db or db.default_db_path()
    conn = db.connect(db_path)
    db.init_lab(conn)
    try:
        if args.func == "list":
            cmd_list(conn)
            return
        if args.func == "export":
            out = export_run(
                conn=conn,
                run_id=args.run_id,
                profile_name=args.profile_name,
                out_path=args.out,
            )
            print(f"Exported to {out}")
            return
        if args.func == "import":
            run_id = import_run(
                conn=conn,
                bundle_path=args.bundle,
                on_conflict=args.on_conflict,
                profile_name=args.profile_name,
            )
            run = db.get_embed_run(conn, run_id)
            print(f"Imported as run id={run_id}")
            if run:
                print(f"  profile_id={run['profile_id']}")
                print(f"  artifact_dir={run['artifact_dir']}")
            return
        raise SystemExit(f"Unknown command: {args.func}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
