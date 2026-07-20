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
    return 0


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
