from __future__ import annotations

import shutil
from argparse import Namespace
from pathlib import Path

from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io
from apps.claims import notes as notes_mod


def cmd_corpus_create(args: Namespace) -> int:
    try:
        corpus = corpus_mod.create_corpus(str(args.name), notes=args.notes)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(
        {
            "ok": True,
            "slug": corpus.slug,
            "root": str(corpus.root),
            "notes": str(corpus.notes),
            "expected": {
                "posts": str(corpus.posts),
                "claims": str(corpus.claims),
                "groups": str(corpus.groups),
            },
        }
    )
    return 0


def cmd_corpus_list(_args: Namespace | None = None) -> int:
    rows = corpus_mod.list_corpora()
    claims_io.emit_json({"corpora": rows, "n": len(rows)})
    return 0


def cmd_corpus_status(args: Namespace) -> int:
    try:
        corpus = corpus_mod.get_corpus(str(args.name))
        if not corpus.root.is_dir():
            raise FileNotFoundError(f"Corpus not found: {corpus.root}")
        status = corpus.status()
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json({"ok": True, **status})
    if getattr(args, "human", False):
        _print_status_human(status)
    return 0


def _print_status_human(status: dict) -> None:
    stages = status.get("stages") or {}

    def _yn(flag: bool) -> str:
        return "yes" if flag else "no"

    def _count_suffix(key: str) -> str:
        n = stages.get(key)
        return f" ({n})" if n is not None else ""

    print(status.get("slug") or "")
    print(f"  claims:    {_yn(bool(stages.get('claims')))}{_count_suffix('claim_count')}")
    print(f"  grouped:   {_yn(bool(stages.get('grouped')))}{_count_suffix('group_count')}")
    print(
        f"  embedded:  {_yn(bool(stages.get('embedded')))}"
        f" ({int(stages.get('n_runs') or 0)} runs)"
    )
    print(
        f"  clustered: {_yn(bool(stages.get('clustered')))}"
        f" ({int(stages.get('n_experiments') or 0)} experiments)"
    )
    anns = status.get("annotations") or []
    if anns:
        print(f"  annotations: {len(anns)}")
        for a in anns[:10]:
            print(f"    - {a.get('name')} (n={a.get('count')})")
    runs = status.get("runs") or []
    if runs:
        print("  runs:")
        for r in runs[:10]:
            tag = str(r.get("tag") or str(r.get("name") or "").split("/")[-1])
            bits = [tag]
            if r.get("claim_count") is not None:
                bits.append(f"n={r['claim_count']}")
            if r.get("model_id"):
                bits.append(str(r["model_id"]))
            print(f"    - {' | '.join(bits)}")
    latest = status.get("latest_experiment")
    if latest:
        print(f"  latest experiment: {latest.get('run')}/{latest.get('name')}")


def _load_terms_file(path: Path) -> list[str]:
    terms: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if s and not s.startswith("#"):
            terms.append(s)
    return terms


def _collect_terms(cli_terms: list[str] | None, terms_file: Path | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for t in cli_terms or []:
        key = str(t).strip()
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    if terms_file is not None:
        for t in _load_terms_file(Path(terms_file)):
            if t not in seen:
                seen.add(t)
                out.append(t)
    return out


def cmd_corpus_seed(args: Namespace) -> int:
    try:
        from scripts.get_posts_for_search_term import fetch_and_write, parse_utc_datetime

        slug = str(args.name)
        terms = _collect_terms(getattr(args, "terms", None), getattr(args, "terms_file", None))
        if not terms:
            raise ValueError("Provide --terms and/or --terms-file")

        since = parse_utc_datetime(getattr(args, "since", None))
        until = parse_utc_datetime(getattr(args, "until", None))
        if since is not None and until is not None and since >= until:
            raise ValueError("since must be before until")

        if bool(getattr(args, "create", False)):
            try:
                corpus_mod.create_corpus(slug, notes=getattr(args, "notes", None))
            except FileExistsError:
                pass

        corpus = corpus_mod.get_corpus(slug)
        if not corpus.root.is_dir():
            raise FileNotFoundError(
                f"Corpus {slug!r} not found at {corpus.root}. "
                f"Run: python -m apps.claims corpus create --name {slug} "
                f"(or pass --create)"
            )

        if corpus.posts.is_file() and not bool(getattr(args, "force", False)):
            raise FileExistsError(f"{corpus.posts} already exists; pass --force to overwrite")

        if bool(getattr(args, "dry_run", False)):
            claims_io.emit_json(
                {
                    "ok": True,
                    "dry_run": True,
                    "slug": corpus.slug,
                    "out": str(corpus.posts),
                    "terms": terms,
                    "since": since.isoformat() if since else None,
                    "until": until.isoformat() if until else None,
                    "limit": getattr(args, "limit", None),
                    "use_prod": bool(getattr(args, "prod", False)),
                    "note": "No DB call; would write posts.json on a real run",
                }
            )
            return 0

        summary = fetch_and_write(
            terms=terms,
            out_path=corpus.posts,
            use_prod=bool(getattr(args, "prod", False)),
            since=since,
            until=until,
            limit=getattr(args, "limit", None),
            count_first=bool(getattr(args, "count_first", False)),
        )
        notes_mod.append_note(
            corpus.notes,
            "Seeded",
            notes_mod.fmt_kv(
                {
                    "terms": ", ".join(terms),
                    "since": summary.get("since") or "(none)",
                    "until": (summary.get("until") or "(none)") + " (exclusive)",
                    "posts": summary.get("post_count"),
                    "db": "prod" if summary.get("use_prod") else "dev",
                }
            ),
        )
        claims_io.emit_json({"ok": True, "slug": corpus.slug, **summary})
        return 0
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1


def cmd_corpus_copy_posts(args: Namespace) -> int:
    """Import an existing posts JSON into a corpus (no DB)."""
    try:
        slug = str(args.name)
        src = Path(args.source).expanduser()
        if not src.is_file():
            raise FileNotFoundError(f"Source not found: {src}")

        if bool(getattr(args, "create", False)):
            try:
                corpus_mod.create_corpus(slug, notes=getattr(args, "notes", None))
            except FileExistsError:
                pass

        corpus = corpus_mod.get_corpus(slug)
        if not corpus.root.is_dir():
            raise FileNotFoundError(
                f"Corpus {slug!r} not found. Pass --create or run corpus create first."
            )
        if corpus.posts.is_file() and not bool(getattr(args, "force", False)):
            raise FileExistsError(f"{corpus.posts} already exists; pass --force to overwrite")

        data = claims_io.read_json(src)
        if not isinstance(data, dict) or not isinstance(data.get("posts"), list):
            raise ValueError("Source JSON must be an object with a top-level 'posts' array")

        post_count = len(data["posts"])
        corpus.root.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, corpus.posts)
        notes_mod.append_note(
            corpus.notes,
            "Copied posts",
            notes_mod.fmt_kv(
                {
                    "from": str(src.resolve()),
                    "posts": post_count,
                    "terms": data.get("terms"),
                }
            ),
        )
        claims_io.emit_json(
            {
                "ok": True,
                "slug": corpus.slug,
                "out": str(corpus.posts),
                "from": str(src.resolve()),
                "post_count": post_count,
            }
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1


def cmd_corpus_import_claims(args: Namespace) -> int:
    """Import nested posts→chunks→claims JSON as corpus claims.json."""
    try:
        slug = str(args.name)
        src = Path(args.source).expanduser()
        if not src.is_file():
            raise FileNotFoundError(f"Source not found: {src}")

        if bool(getattr(args, "create", False)):
            try:
                corpus_mod.create_corpus(slug, notes=getattr(args, "notes", None))
            except FileExistsError:
                pass

        corpus = corpus_mod.get_corpus(slug)
        if not corpus.root.is_dir():
            raise FileNotFoundError(
                f"Corpus {slug!r} not found. Pass --create or run corpus create first."
            )
        if corpus.claims.is_file() and not bool(getattr(args, "force", False)):
            raise FileExistsError(f"{corpus.claims} already exists; pass --force to overwrite")

        data = claims_io.read_json(src)
        if not isinstance(data, dict) or not isinstance(data.get("posts"), list):
            raise ValueError("Source JSON must be an object with a top-level 'posts' array")

        # Light shape check: at least one post should look nested (or empty corpus).
        posts = [p for p in data["posts"] if isinstance(p, dict)]
        if posts and not any(isinstance(p.get("chunks"), list) for p in posts):
            raise ValueError(
                "Expected nested claims JSON (posts[].chunks[].claims). "
                "Use scripts/get_posts_extract_upload.py to produce this shape."
            )

        from apps.claims.claims_data import count_nested_claims

        post_count, claim_count = count_nested_claims(posts)
        corpus.root.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, corpus.claims)
        notes_mod.append_note(
            corpus.notes,
            "Imported claims",
            notes_mod.fmt_kv(
                {
                    "from": str(src.resolve()),
                    "posts": post_count,
                    "claims": claim_count,
                    "terms": data.get("terms"),
                }
            ),
        )
        claims_io.emit_json(
            {
                "ok": True,
                "slug": corpus.slug,
                "out": str(corpus.claims),
                "from": str(src.resolve()),
                "post_count": post_count,
                "claim_count": claim_count,
            }
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1


def cmd_corpus_derive(args: Namespace) -> int:
    """Derive a Reddit-deweighted corpus (claim-balanced) from a parent corpus."""
    try:
        from apps.claims import derive as derive_mod

        src_slug = str(args.from_corpus)
        dst_slug = str(args.name)
        seed = int(getattr(args, "seed", 0) or 0)
        target_ratio = float(getattr(args, "target_ratio", 1.0) or 1.0)
        also_group = bool(getattr(args, "group", False))

        src = corpus_mod.get_corpus(src_slug)
        if not src.claims.is_file():
            raise FileNotFoundError(f"Missing parent claims.json at {src.claims}")

        try:
            corpus_mod.create_corpus(
                dst_slug,
                notes=getattr(args, "notes", None)
                or (
                    f"Derived from {src_slug}: Reddit downsampled so Reddit claims "
                    f"≈ {target_ratio:g}× non-Reddit claims (seed={seed})."
                ),
            )
        except FileExistsError:
            pass

        dst = corpus_mod.get_corpus(dst_slug)
        if not dst.root.is_dir():
            raise FileNotFoundError(
                f"Corpus {dst_slug!r} not found. Pass --create or run corpus create first."
            )
        if dst.claims.is_file() and not bool(getattr(args, "force", False)):
            raise FileExistsError(f"{dst.claims} already exists; pass --force to overwrite")

        parent = claims_io.read_json(src.claims)
        if not isinstance(parent, dict) or not isinstance(parent.get("posts"), list):
            raise ValueError("Parent claims.json must be an object with a top-level 'posts' array")

        posts = [p for p in parent["posts"] if isinstance(p, dict)]
        derived_posts, stats = derive_mod.derive_reddit_balanced_posts(
            posts,
            seed=seed,
            target_ratio=target_ratio,
        )
        payload = derive_mod.build_derived_payload(
            parent,
            derived_posts,
            derived_from=src_slug,
            stats=stats,
        )
        claims_io.write_json(dst.claims, payload)
        notes_mod.append_note(
            dst.notes,
            "Derived (reddit claim-balance)",
            notes_mod.fmt_kv(
                {
                    "from": src_slug,
                    "seed": seed,
                    "target_ratio": target_ratio,
                    "other_claims": stats["other_claims"],
                    "reddit_claims_kept": stats["reddit_claims_kept"],
                    "reddit_claims_all": stats["reddit_claims_all"],
                    "posts_out": stats["posts_out"],
                    "claims_out": stats["claims_out"],
                }
            ),
        )

        grouped = False
        if also_group:
            from apps.claims.grouping import group as grouping

            bundle = grouping.run(dst.claims)
            claims_io.write_json(dst.groups, grouping.bundle_to_dict(bundle))
            notes_mod.append_note(
                dst.notes,
                "Grouped",
                notes_mod.fmt_kv(
                    {
                        "claim_count": bundle.claim_count,
                        "source_claim_count": bundle.source_claim_count,
                        "source_hash": bundle.source_hash[:16] if bundle.source_hash else None,
                        "out": str(dst.groups),
                    }
                ),
            )
            grouped = True
            stats = {
                **stats,
                "group_claim_count": bundle.claim_count,
                "group_source_claim_count": bundle.source_claim_count,
            }

        claims_io.emit_json(
            {
                "ok": True,
                "slug": dst.slug,
                "from": src_slug,
                "out": str(dst.claims),
                "grouped": grouped,
                "groups": str(dst.groups) if grouped else None,
                **stats,
            }
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
