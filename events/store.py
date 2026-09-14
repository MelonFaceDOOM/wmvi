"""JSONL catalog store for the curated event history.

The catalog is tracked in git (unlike ``apps/claims/data/``, which is ignored)
because human corrections to an event's dates, scope, or title exist nowhere
else. Reads validate; writes are atomic and deterministically ordered so a new
event is a one-line diff.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from events.schema import (
    EventRecord,
    EventSource,
    Subject,
    slugify,
    utc_now,
    validate_event,
    validate_subject,
)

EVENTS_FILE = "events.jsonl"
SUBJECTS_FILE = "subjects.jsonl"


class CatalogError(RuntimeError):
    """Catalog is unreadable, invalid, or an operation would corrupt identity."""


def catalog_dir() -> Path:
    return Path(__file__).resolve().parent / "catalog"


def events_path() -> Path:
    return catalog_dir() / EVENTS_FILE


def subjects_path() -> Path:
    return catalog_dir() / SUBJECTS_FILE


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line or line.startswith("//"):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise CatalogError(f"{path}:{lineno}: invalid JSON ({exc})") from exc
            if not isinstance(row, dict):
                raise CatalogError(f"{path}:{lineno}: expected a JSON object")
            out.append(row)
    return out


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=False) + "\n")
    tmp.replace(path)


def _sort_key(record: EventRecord) -> tuple[int, str, str]:
    """Chronological, with undated events last, then by key for determinism."""
    start = record.start_date or ""
    return (0 if start else 1, start, record.event_key)


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


def load_events(path: Path | None = None, *, validate: bool = True) -> list[EventRecord]:
    """Read the catalog. Raises CatalogError on duplicate keys or invalid records."""
    target = Path(path) if path is not None else events_path()
    records = [EventRecord.from_dict(row) for row in _read_jsonl(target)]

    seen: set[str] = set()
    problems: list[str] = []
    for rec in records:
        if rec.event_key in seen:
            problems.append(f"{rec.event_key}: duplicate event_key")
        seen.add(rec.event_key)
        if validate:
            problems.extend(validate_event(rec))

    if validate:
        for rec in records:
            if rec.merged_into and rec.merged_into not in seen:
                problems.append(f"{rec.event_key}: merged_into {rec.merged_into!r} is not in the catalog")

    if problems:
        raise CatalogError(f"{target}: {len(problems)} problem(s):\n  " + "\n  ".join(problems))
    return records


def write_events(records: Iterable[EventRecord], path: Path | None = None) -> Path:
    """Rewrite the catalog in canonical order. Validates before touching disk."""
    target = Path(path) if path is not None else events_path()
    ordered = sorted(records, key=_sort_key)

    problems: list[str] = []
    seen: set[str] = set()
    for rec in ordered:
        if rec.event_key in seen:
            problems.append(f"{rec.event_key}: duplicate event_key")
        seen.add(rec.event_key)
        problems.extend(validate_event(rec))
    if problems:
        raise CatalogError(f"refusing to write {target}:\n  " + "\n  ".join(problems))

    _write_jsonl(target, (rec.to_dict() for rec in ordered))
    return target


def index_by_key(records: Iterable[EventRecord]) -> dict[str, EventRecord]:
    return {rec.event_key: rec for rec in records}


def mint_event_key(
    title: str,
    *,
    existing: Iterable[str],
    start_date: str | None = None,
) -> str:
    """Derive a unique, readable event_key from a title (+ year when known)."""
    taken = set(existing)
    base = slugify(title)
    if not base:
        base = "event"

    year = (start_date or "")[:4]
    if year.isdigit() and year not in base.split("-"):
        base = f"{base}-{year}"

    if base not in taken:
        return base
    for n in range(2, 1000):
        candidate = f"{base}-{n}"
        if candidate not in taken:
            return candidate
    raise CatalogError(f"could not mint a unique event_key from {title!r}")


def add_source(record: EventRecord, source: EventSource, *, touch: bool = True) -> bool:
    """Append provenance unless an entry with the same locator already exists."""
    ident = source.identity()
    if any(existing.identity() == ident for existing in record.provenance):
        return False
    if not source.recorded_at:
        source.recorded_at = utc_now()
    if source.is_discovery and any(s.is_discovery for s in record.provenance):
        source.is_discovery = False
    record.provenance.append(source)
    if touch:
        record.updated_at = utc_now()
    return True


def upsert_event(records: list[EventRecord], record: EventRecord) -> list[EventRecord]:
    """Replace the record with a matching event_key, or append it.

    ``created_at`` on an existing record is preserved; identity is never changed.
    """
    now = utc_now()
    if not record.created_at:
        record.created_at = now
    record.updated_at = now

    for i, existing in enumerate(records):
        if existing.event_key == record.event_key:
            record.created_at = existing.created_at or record.created_at
            records[i] = record
            return records
    records.append(record)
    return records


def resolve_key(index: dict[str, EventRecord], key: str) -> str:
    """Follow ``merged_into`` to the surviving event_key. Cycle-safe."""
    seen: set[str] = set()
    current = key
    while True:
        rec = index.get(current)
        if rec is None or not rec.merged_into:
            return current
        if current in seen:
            raise CatalogError(f"merge cycle involving event_key {key!r}")
        seen.add(current)
        current = rec.merged_into


def merge_events(
    records: list[EventRecord],
    *,
    source_key: str,
    into_key: str,
    move_provenance: bool = True,
    note: str | None = None,
) -> list[EventRecord]:
    """Mark ``source_key`` merged into ``into_key``, keeping the losing record.

    The merged record stays in the catalog so links that reference it still
    resolve, matching ``events.event.merged_into_event_id``.
    """
    index = index_by_key(records)
    if source_key not in index:
        raise CatalogError(f"unknown event_key {source_key!r}")
    if into_key not in index:
        raise CatalogError(f"unknown event_key {into_key!r}")

    target_key = resolve_key(index, into_key)
    if target_key == source_key:
        raise CatalogError(f"merging {source_key!r} into {into_key!r} would create a cycle")

    loser = index[source_key]
    winner = index[target_key]

    if move_provenance:
        for src in loser.provenance:
            moved = EventSource.from_dict(src.to_dict())
            moved.is_discovery = False
            add_source(winner, moved, touch=False)
        loser.provenance = []

    loser.status = "merged"
    loser.merged_into = target_key
    if note:
        loser.review.notes = note
    loser.updated_at = utc_now()
    winner.updated_at = utc_now()
    return records


def active_events(records: Iterable[EventRecord]) -> list[EventRecord]:
    return [rec for rec in records if rec.status != "merged"]


def claim_key_links(
    records: Iterable[EventRecord],
    *,
    corpus: str,
    resolve_merges: bool = True,
) -> dict[str, str]:
    """Build ``claim_key -> event_key`` for one corpus.

    Shape is ready for ``apps.claims.annotations.write_annotation`` with
    ``scope="group"``. Later provenance wins on conflict.
    """
    records = list(records)
    index = index_by_key(records)
    out: dict[str, str] = {}
    for rec in records:
        key = resolve_key(index, rec.event_key) if resolve_merges else rec.event_key
        for claim in rec.claim_keys(corpus=corpus):
            out[claim] = key
    return out


# ---------------------------------------------------------------------------
# Subjects
# ---------------------------------------------------------------------------


def load_subjects(path: Path | None = None, *, validate: bool = True) -> list[Subject]:
    target = Path(path) if path is not None else subjects_path()
    subjects = [Subject.from_dict(row) for row in _read_jsonl(target)]

    problems: list[str] = []
    seen: set[str] = set()
    for subj in subjects:
        if subj.slug in seen:
            problems.append(f"{subj.slug}: duplicate slug")
        seen.add(subj.slug)
        if validate:
            problems.extend(validate_subject(subj))
    if problems:
        raise CatalogError(f"{target}: {len(problems)} problem(s):\n  " + "\n  ".join(problems))
    return subjects


def write_subjects(subjects: Iterable[Subject], path: Path | None = None) -> Path:
    target = Path(path) if path is not None else subjects_path()
    ordered = sorted(subjects, key=lambda s: s.slug)

    problems: list[str] = []
    for subj in ordered:
        problems.extend(validate_subject(subj))
    if problems:
        raise CatalogError(f"refusing to write {target}:\n  " + "\n  ".join(problems))

    _write_jsonl(target, (subj.to_dict() for subj in ordered))
    return target


def unknown_subject_slugs(
    records: Iterable[EventRecord],
    subjects: Iterable[Subject] | None = None,
) -> list[str]:
    """Subject slugs referenced by events but absent from the subject catalog."""
    known = {s.slug for s in (subjects if subjects is not None else load_subjects())}
    out: list[str] = []
    for rec in records:
        for subj in rec.subjects:
            if subj.slug not in known and subj.slug not in out:
                out.append(subj.slug)
    return out
