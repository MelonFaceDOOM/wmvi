"""Migrate taxonomy.vaccine_term (+ subset membership) from a JSON source.

Source shape (``data/new_terms.json``)::

    {
      "subset_a": ["Term One", "Term Two"],
      "subset_b": ["Term Two"],
      "no subset": ["orphan term"]
    }

All names are normalized to ``strip().lower()``. ``"no subset"`` is not a real
subset — those terms stay in the keep-set with zero memberships.

Read-only::

    python -m scripts.migrate_vaccine_terms --prod preview
    python -m scripts.migrate_vaccine_terms --prod list-deletes
    python -m scripts.migrate_vaccine_terms --prod delete-impact
    python -m scripts.migrate_vaccine_terms --prod export-delete-contexts --out contexts.jsonl

Apply (transactional)::

    python -m scripts.migrate_vaccine_terms --prod apply
    python -m scripts.migrate_vaccine_terms --prod apply --yes
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

from dotenv import load_dotenv

from db.db import close_pool, getcursor, init_pool

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = REPO_ROOT / "data" / "new_terms.json"
NO_SUBSET_KEY = "no subset"
CONTEXT_PAD = 100
TERM_TYPE = "search"

# FKs that CASCADE on taxonomy.vaccine_term delete (must stay in sync with migrations).
KNOWN_TERM_FK_TABLES: frozenset[tuple[str, str]] = frozenset(
    {
        ("matches", "post_term_hit"),
        ("matches", "post_term_match"),
        ("matches", "term_match_state"),
        ("taxonomy", "vaccine_term_subset_member"),
        ("youtube", "search_status"),
        ("sm", "reddit_submission_search_status"),
    }
)


@dataclass(frozen=True)
class DesiredState:
    """Normalized desired taxonomy from JSON."""

    terms: frozenset[str]  # all keep names (lower)
    subsets: frozenset[str]  # real subset names (excludes "no subset")
    memberships: dict[str, frozenset[str]]  # term -> subset names (may be empty)
    collisions: tuple[str, ...]  # lower names that collided during normalize


@dataclass(frozen=True)
class CaseRename:
    term_id: int
    old_name: str
    new_name: str


@dataclass(frozen=True)
class MembershipChange:
    term_name: str
    subset_name: str


@dataclass(frozen=True)
class CaseCollision:
    """Multiple DB rows whose names collide after lowercasing."""

    lower_name: str
    variants: tuple[tuple[int, str], ...]  # (term_id, name)


@dataclass
class MigrationPlan:
    adds: list[str] = field(default_factory=list)
    case_renames: list[CaseRename] = field(default_factory=list)
    deletes: list[tuple[int, str]] = field(default_factory=list)  # (id, name)
    membership_adds: list[MembershipChange] = field(default_factory=list)
    membership_removes: list[MembershipChange] = field(default_factory=list)
    subsets_to_create: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    case_collisions: list[CaseCollision] = field(default_factory=list)
    blocking_errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "counts": {
                "adds": len(self.adds),
                "case_renames": len(self.case_renames),
                "deletes": len(self.deletes),
                "membership_adds": len(self.membership_adds),
                "membership_removes": len(self.membership_removes),
                "subsets_to_create": len(self.subsets_to_create),
                "case_collisions": len(self.case_collisions),
            },
            "adds": sorted(self.adds),
            "case_renames": [asdict(r) for r in self.case_renames],
            "deletes": [{"term_id": i, "name": n} for i, n in self.deletes],
            "membership_adds": [asdict(m) for m in self.membership_adds],
            "membership_removes": [asdict(m) for m in self.membership_removes],
            "subsets_to_create": sorted(self.subsets_to_create),
            "warnings": list(self.warnings),
            "case_collisions": [
                {
                    "lower_name": c.lower_name,
                    "variants": [
                        {"term_id": tid, "name": name} for tid, name in c.variants
                    ],
                }
                for c in self.case_collisions
            ],
            "blocking_errors": list(self.blocking_errors),
            "safe_to_apply": not self.blocking_errors,
        }


def assert_safe_to_apply(plan: MigrationPlan) -> None:
    """Raise if the plan must not be applied (e.g. ambiguous case-variant rows)."""
    if plan.blocking_errors:
        raise RuntimeError(
            "Refusing to apply — resolve blocking errors first:\n  - "
            + "\n  - ".join(plan.blocking_errors)
        )


def normalize_term(raw: str) -> str:
    return str(raw).strip().lower()


def load_desired_state(source: Path | dict[str, Any]) -> DesiredState:
    """Load and normalize JSON; return desired terms + memberships."""
    if isinstance(source, Path):
        payload = json.loads(source.read_text(encoding="utf-8"))
    else:
        payload = source
    if not isinstance(payload, dict):
        raise ValueError("Source JSON must be an object of subset -> [terms]")

    memberships: dict[str, set[str]] = {}
    subsets: set[str] = set()
    collisions: list[str] = []
    seen_in_bucket: dict[str, set[str]] = {}

    for key, values in payload.items():
        if not isinstance(key, str):
            raise ValueError(f"Subset key must be str, got {type(key)}")
        if not isinstance(values, list):
            raise ValueError(f"Subset {key!r} value must be a list")
        is_no_subset = key == NO_SUBSET_KEY
        if not is_no_subset:
            subsets.add(key)
        bucket_seen = seen_in_bucket.setdefault(key, set())
        for raw in values:
            name = normalize_term(raw)
            if not name:
                continue
            if name in bucket_seen:
                collisions.append(name)
                continue
            bucket_seen.add(name)
            memberships.setdefault(name, set())
            if not is_no_subset:
                memberships[name].add(key)

    # Terms only listed under "no subset" already have empty set from setdefault.
    # Terms listed under both a subset and "no subset" keep subset memberships.
    return DesiredState(
        terms=frozenset(memberships.keys()),
        subsets=frozenset(subsets),
        memberships={k: frozenset(v) for k, v in memberships.items()},
        collisions=tuple(sorted(set(collisions))),
    )


def slice_hit_context(text: str, match_start: int, match_end: int, *, pad: int = CONTEXT_PAD) -> str:
    """Extract text around a hit span, clamped to string bounds."""
    if text is None:
        return ""
    s = max(0, int(match_start) - pad)
    e = min(len(text), int(match_end) + pad)
    if e < s:
        s, e = e, s
    return text[s:e]


def _group_db_terms_by_lower(
    db_terms: Sequence[tuple[int, str]],
) -> tuple[dict[str, tuple[int, str]], list[CaseCollision]]:
    """Split unambiguous lower→(id,name) map from multi-row case collisions."""
    groups: dict[str, list[tuple[int, str]]] = {}
    for term_id, name in db_terms:
        key = normalize_term(name)
        groups.setdefault(key, []).append((int(term_id), str(name)))

    by_lower: dict[str, tuple[int, str]] = {}
    collisions: list[CaseCollision] = []
    for key, variants in sorted(groups.items()):
        # Dedupe identical (id, name) if caller passed duplicates.
        uniq: dict[int, str] = {}
        for tid, nm in variants:
            uniq[tid] = nm
        items = tuple(sorted(uniq.items(), key=lambda x: x[0]))
        if len(items) > 1:
            collisions.append(CaseCollision(lower_name=key, variants=items))
        else:
            by_lower[key] = items[0]
    return by_lower, collisions


def compute_plan(
    desired: DesiredState,
    *,
    db_terms: Sequence[tuple[int, str]],
    db_memberships: Sequence[tuple[str, str]],  # (term_name, subset_name)
    db_subsets: Sequence[str],
) -> MigrationPlan:
    """Diff desired vs current DB state (names compared case-insensitively)."""
    plan = MigrationPlan()
    if desired.collisions:
        plan.warnings.append(
            f"Dropped {len(desired.collisions)} duplicate name(s) within the same "
            f"JSON bucket after lowercasing: {', '.join(desired.collisions[:20])}"
            + ("…" if len(desired.collisions) > 20 else "")
        )

    by_lower, collisions = _group_db_terms_by_lower(db_terms)
    plan.case_collisions = collisions
    for col in collisions:
        variants_txt = ", ".join(f"id={tid} {name!r}" for tid, name in col.variants)
        plan.blocking_errors.append(
            f"DB has multiple terms that lower() to {col.lower_name!r}: {variants_txt}. "
            "Resolve manually (merge/delete extras) before apply — otherwise renames, "
            "membership sync, and deletes are ambiguous."
        )

    # Ambiguous keys must not participate in rename/delete/membership identity.
    ambiguous_lowers = {c.lower_name for c in collisions}

    existing_subsets = {str(s) for s in db_subsets}
    plan.subsets_to_create = sorted(desired.subsets - existing_subsets)

    for name in sorted(desired.terms):
        if name in ambiguous_lowers:
            continue
        if name not in by_lower:
            plan.adds.append(name)
        else:
            term_id, current = by_lower[name]
            if current != name:
                plan.case_renames.append(
                    CaseRename(term_id=term_id, old_name=current, new_name=name)
                )

    for lower, (term_id, current) in sorted(by_lower.items(), key=lambda x: x[0]):
        if lower not in desired.terms:
            plan.deletes.append((term_id, current))

    # Memberships: after renames/adds, identity is lower name.
    # Skip ambiguous lowers so we do not silently touch the wrong row.
    current_mem: set[tuple[str, str]] = set()
    for term_name, subset_name in db_memberships:
        key = normalize_term(term_name)
        if key in ambiguous_lowers:
            continue
        current_mem.add((key, str(subset_name)))

    desired_mem: set[tuple[str, str]] = set()
    for term, subsets in desired.memberships.items():
        if term in ambiguous_lowers:
            continue
        for subset in subsets:
            desired_mem.add((term, subset))

    for term, subset in sorted(desired_mem - current_mem):
        plan.membership_adds.append(MembershipChange(term_name=term, subset_name=subset))
    for term, subset in sorted(current_mem - desired_mem):
        # Only remove memberships for terms we keep; deletes cascade memberships.
        if term in desired.terms:
            plan.membership_removes.append(
                MembershipChange(term_name=term, subset_name=subset)
            )

    return plan


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def fetch_db_terms() -> list[tuple[int, str]]:
    with getcursor(commit=False) as cur:
        cur.execute("SELECT id, name FROM taxonomy.vaccine_term ORDER BY name")
        return [(int(r[0]), str(r[1])) for r in cur.fetchall()]


def fetch_db_subsets() -> list[str]:
    with getcursor(commit=False) as cur:
        cur.execute("SELECT name FROM taxonomy.vaccine_term_subset ORDER BY name")
        return [str(r[0]) for r in cur.fetchall()]


def fetch_db_memberships() -> list[tuple[str, str]]:
    with getcursor(commit=False) as cur:
        cur.execute(
            """
            SELECT t.name, s.name
            FROM taxonomy.vaccine_term_subset_member m
            JOIN taxonomy.vaccine_term t ON t.id = m.term_id
            JOIN taxonomy.vaccine_term_subset s ON s.id = m.subset_id
            ORDER BY t.name, s.name
            """
        )
        return [(str(r[0]), str(r[1])) for r in cur.fetchall()]


def build_plan_from_db(desired: DesiredState) -> MigrationPlan:
    return compute_plan(
        desired,
        db_terms=fetch_db_terms(),
        db_memberships=fetch_db_memberships(),
        db_subsets=fetch_db_subsets(),
    )


def fetch_referring_fk_tables() -> list[tuple[str, str]]:
    """Return (schema, table) that FK to taxonomy.vaccine_term(id)."""
    with getcursor(commit=False) as cur:
        cur.execute(
            """
            SELECT
                nsp_src.nspname AS schema_name,
                rel_src.relname AS table_name
            FROM pg_constraint c
            JOIN pg_class rel_src ON rel_src.oid = c.conrelid
            JOIN pg_namespace nsp_src ON nsp_src.oid = rel_src.relnamespace
            JOIN pg_class rel_tgt ON rel_tgt.oid = c.confrelid
            JOIN pg_namespace nsp_tgt ON nsp_tgt.oid = rel_tgt.relnamespace
            WHERE c.contype = 'f'
              AND nsp_tgt.nspname = 'taxonomy'
              AND rel_tgt.relname = 'vaccine_term'
            ORDER BY 1, 2
            """
        )
        return [(str(r[0]), str(r[1])) for r in cur.fetchall()]


def assert_known_term_fks() -> list[tuple[str, str]]:
    found = frozenset(fetch_referring_fk_tables())
    unexpected = found - KNOWN_TERM_FK_TABLES
    missing = KNOWN_TERM_FK_TABLES - found
    if unexpected:
        raise RuntimeError(
            "Unexpected FK(s) to taxonomy.vaccine_term — refusing to proceed: "
            + ", ".join(f"{s}.{t}" for s, t in sorted(unexpected))
        )
    if missing:
        # Table might not exist on older DBs; warn but allow if empty in impact.
        warnings.warn(
            "Expected FK table(s) not found (may be absent on this DB): "
            + ", ".join(f"{s}.{t}" for s, t in sorted(missing)),
            stacklevel=2,
        )
    return sorted(found)


def count_rows_for_terms(schema: str, table: str, term_ids: Sequence[int]) -> int:
    if not term_ids:
        return 0
    # All known tables use column term_id.
    sql = f'SELECT count(*) FROM "{schema}"."{table}" WHERE term_id = ANY(%s)'
    with getcursor(commit=False) as cur:
        cur.execute(sql, (list(term_ids),))
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def delete_impact_report(plan: MigrationPlan) -> dict[str, Any]:
    fk_tables = assert_known_term_fks()
    term_ids = [tid for tid, _ in plan.deletes]
    per_table: dict[str, int] = {}
    for schema, table in fk_tables:
        key = f"{schema}.{table}"
        per_table[key] = count_rows_for_terms(schema, table, term_ids)
    return {
        "delete_term_count": len(plan.deletes),
        "deletes": [{"term_id": i, "name": n} for i, n in plan.deletes],
        "cascade_row_counts": per_table,
        "cascade_row_counts_total": sum(per_table.values()),
        "fk_tables_checked": [f"{s}.{t}" for s, t in fk_tables],
    }


def iter_delete_hit_contexts(
    plan: MigrationPlan,
    *,
    pad: int = CONTEXT_PAD,
) -> Iterable[dict[str, Any]]:
    term_ids = [tid for tid, _ in plan.deletes]
    if not term_ids:
        return
    with getcursor(commit=False) as cur:
        cur.execute(
            """
            SELECT
                ph.term_id,
                t.name AS term_name,
                ph.post_id,
                p.platform,
                ph.match_start,
                ph.match_end,
                p.text
            FROM matches.post_term_hit ph
            JOIN taxonomy.vaccine_term t ON t.id = ph.term_id
            JOIN sm.posts_all p ON p.post_id = ph.post_id
            WHERE ph.term_id = ANY(%s)
            ORDER BY ph.term_id, ph.post_id, ph.match_start, ph.match_end
            """,
            (term_ids,),
        )
        rows = cur.fetchall()
    for row in rows:
        (
            term_id,
            term_name,
            post_id,
            platform,
            match_start,
            match_end,
            text,
        ) = row
        yield {
            "term_id": int(term_id),
            "term_name": str(term_name),
            "post_id": int(post_id),
            "platform": platform,
            "match_start": int(match_start),
            "match_end": int(match_end),
            "context": slice_hit_context(
                text if isinstance(text, str) else "",
                int(match_start),
                int(match_end),
                pad=pad,
            ),
        }


def export_delete_contexts(plan: MigrationPlan, out_path: Path, *, pad: int = CONTEXT_PAD) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with out_path.open("w", encoding="utf-8") as f:
        for row in iter_delete_hit_contexts(plan, pad=pad):
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
    return n


def apply_plan(plan: MigrationPlan, desired: DesiredState) -> dict[str, Any]:
    """Apply migration in one transaction.

    Raises ``RuntimeError`` if ``plan.blocking_errors`` is non-empty.
    """
    assert_safe_to_apply(plan)
    with getcursor(commit=True) as cur:
        # 1) Ensure subsets
        for subset_name in plan.subsets_to_create:
            cur.execute(
                """
                INSERT INTO taxonomy.vaccine_term_subset (name, description)
                VALUES (%s, %s)
                ON CONFLICT (name) DO NOTHING
                """,
                (subset_name, None),
            )

        # 2) Case renames (preserve ids)
        for ren in plan.case_renames:
            cur.execute(
                """
                UPDATE taxonomy.vaccine_term
                SET name = %s
                WHERE id = %s
                """,
                (ren.new_name, ren.term_id),
            )

        # 3) Insert missing terms
        if plan.adds:
            cur.execute(
                """
                INSERT INTO taxonomy.vaccine_term (name, type)
                SELECT v.term, %s
                FROM unnest(%s::text[]) AS v(term)
                ON CONFLICT (name) DO NOTHING
                """,
                (TERM_TYPE, plan.adds),
            )

        # 4) Membership removes (kept terms only)
        for mem in plan.membership_removes:
            cur.execute(
                """
                DELETE FROM taxonomy.vaccine_term_subset_member m
                USING taxonomy.vaccine_term t, taxonomy.vaccine_term_subset s
                WHERE m.term_id = t.id
                  AND m.subset_id = s.id
                  AND lower(t.name) = %s
                  AND s.name = %s
                """,
                (mem.term_name, mem.subset_name),
            )

        # 5) Membership adds
        for mem in plan.membership_adds:
            cur.execute(
                """
                INSERT INTO taxonomy.vaccine_term_subset_member (subset_id, term_id)
                SELECT s.id, t.id
                FROM taxonomy.vaccine_term_subset s
                JOIN taxonomy.vaccine_term t
                  ON lower(t.name) = %s
                WHERE s.name = %s
                ON CONFLICT DO NOTHING
                """,
                (mem.term_name, mem.subset_name),
            )

        # 6) Deletes (CASCADE)
        doomed_ids = [tid for tid, _ in plan.deletes]
        deleted = 0
        if doomed_ids:
            cur.execute(
                "DELETE FROM taxonomy.vaccine_term WHERE id = ANY(%s)",
                (doomed_ids,),
            )
            deleted = cur.rowcount

    return {
        "ok": True,
        "added": len(plan.adds),
        "case_renamed": len(plan.case_renames),
        "membership_added": len(plan.membership_adds),
        "membership_removed": len(plan.membership_removes),
        "subsets_created": len(plan.subsets_to_create),
        "deleted": deleted,
        "desired_term_count": len(desired.terms),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_json(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2), flush=True)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(
        description="Migrate vaccine terms / subsets from JSON (lowercase names)"
    )
    ap.add_argument("--prod", action="store_true", help="Use prod DB pool")
    ap.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
        help=f"JSON source (default: {DEFAULT_SOURCE})",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("preview", help="Print full migration plan as JSON")
    sub.add_parser("list-deletes", help="Print terms that would be deleted")
    sub.add_parser(
        "delete-impact",
        help="Row counts in CASCADE tables for terms that would be deleted",
    )
    p_export = sub.add_parser(
        "export-delete-contexts",
        help="Write ±100 char hit contexts for doomed terms (JSONL)",
    )
    p_export.add_argument("--out", type=Path, required=True)
    p_export.add_argument("--pad", type=int, default=CONTEXT_PAD)

    p_apply = sub.add_parser("apply", help="Apply migration in one transaction")
    p_apply.add_argument(
        "--yes",
        action="store_true",
        help="Skip interactive confirmation (still required for --prod unless set)",
    )

    args = ap.parse_args(argv)

    if not args.source.is_file():
        print(f"error: source not found: {args.source}", file=sys.stderr)
        return 2

    desired = load_desired_state(args.source)
    init_pool(prefix="prod" if args.prod else "dev")
    try:
        plan = build_plan_from_db(desired)

        if args.cmd == "preview":
            out = plan.to_dict()
            out["desired_term_count"] = len(desired.terms)
            out["desired_subset_count"] = len(desired.subsets)
            out["source"] = str(args.source)
            out["use_prod"] = bool(args.prod)
            _print_json(out)
            return 0

        if args.cmd == "list-deletes":
            _print_json(
                {
                    "delete_count": len(plan.deletes),
                    "deletes": [{"term_id": i, "name": n} for i, n in plan.deletes],
                }
            )
            return 0

        if args.cmd == "delete-impact":
            _print_json(delete_impact_report(plan))
            return 0

        if args.cmd == "export-delete-contexts":
            n = export_delete_contexts(plan, args.out, pad=max(0, int(args.pad)))
            _print_json(
                {
                    "out": str(args.out),
                    "context_rows": n,
                    "delete_term_count": len(plan.deletes),
                    "pad": int(args.pad),
                }
            )
            return 0

        if args.cmd == "apply":
            summary = plan.to_dict()["counts"]
            print(
                "About to apply migration: "
                f"adds={summary['adds']} renames={summary['case_renames']} "
                f"deletes={summary['deletes']} "
                f"mem+={summary['membership_adds']} mem-={summary['membership_removes']} "
                f"new_subsets={summary['subsets_to_create']} "
                f"db={'prod' if args.prod else 'dev'}",
                flush=True,
            )
            if plan.warnings:
                print("warnings:", flush=True)
                for w in plan.warnings:
                    print(f"  - {w}", flush=True)
            if plan.blocking_errors:
                print("blocking_errors (apply refused):", flush=True)
                for err in plan.blocking_errors:
                    print(f"  - {err}", flush=True)
                _print_json(
                    {
                        "ok": False,
                        "safe_to_apply": False,
                        "blocking_errors": plan.blocking_errors,
                        "case_collisions": plan.to_dict()["case_collisions"],
                    }
                )
                return 2
            if not args.yes:
                resp = input("Type 'yes' to proceed: ").strip().lower()
                if resp != "yes":
                    print("Aborted.", flush=True)
                    return 1
            result = apply_plan(plan, desired)
            _print_json(result)
            return 0

        ap.error(f"unknown command {args.cmd!r}")
        return 2
    finally:
        close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
