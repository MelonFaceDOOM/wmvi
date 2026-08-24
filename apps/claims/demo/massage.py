"""LLM titles + leaf→narrative reassignment (personal OpenAI, gpt-5.6-luna)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from apps.claims.demo import DEFAULT_MODEL
from apps.claims.demo.catalog import (
    DEFAULT_EXP_DIR,
    load_catalog,
    load_membership,
    load_names,
    names_path,
    save_membership,
    save_names,
)
from apps.claims.demo.schemas import LEAF_NAMES_SCHEMA, NARRATIVE_NAMES_SCHEMA, REASSIGN_SCHEMA

CompleteFn = Callable[..., dict[str, Any]]

NARRATIVE_SYSTEM = (
    "You name clusters of measles/vaccine claims for a public dashboard. "
    "Return JSON only. Titles are short topic names (not sentences), globally distinct. "
    "Blurbs are one line. Do not invent ids. Do not merge or split clusters."
)
LEAF_SYSTEM = (
    "You name paraphrase-clusters of measles/vaccine claims. "
    "Each title is a short paraphrase of the medoid (what people are saying), "
    "not a broad topic. Blurb is one line. Return JSON only. Do not invent ids."
)
REASSIGN_SYSTEM = (
    "You assign claim-paraphrase leaves to existing narrative topics. "
    "Use narrative_id=-1 if the leaf does not belong on the dashboard "
    "(misc, off-topic, or mixed junk). Do not invent narrative ids. Return JSON only."
)


def _complete(
    *,
    model: str,
    system: str,
    user: str,
    schema: dict[str, Any],
    max_completion_tokens: int,
    complete: CompleteFn | None,
) -> dict[str, Any]:
    if complete is not None:
        return complete(
            model=model,
            system=system,
            user=user,
            schema=schema,
            max_completion_tokens=max_completion_tokens,
        )
    from nlp.claim_extraction.clients import openai_structured_completion

    return openai_structured_completion(
        model=model,
        system=system,
        user=user,
        schema=schema,
        max_completion_tokens=max_completion_tokens,
    )


def _chunks(items: list[Any], size: int) -> list[list[Any]]:
    n = max(1, int(size))
    return [items[i : i + n] for i in range(0, len(items), n)]


def _named_ids(rows: list[dict[str, Any]]) -> set[int]:
    return {int(r["id"]) for r in rows if "id" in r}


def name_narratives(
    exp_dir: Path | None = None,
    *,
    model: str = DEFAULT_MODEL,
    complete: CompleteFn | None = None,
) -> dict[str, Any]:
    catalog = load_catalog(exp_dir or DEFAULT_EXP_DIR)
    lines = []
    for nar in catalog.narratives:
        medoids = [lf.medoid for lf in nar.leaves[:5] if lf.medoid]
        lines.append(
            f"id={nar.narrative_id} size={nar.size} n_leaves={nar.n_leaves}\n"
            + "\n".join(f"  - {m}" for m in medoids)
        )
    out = _complete(
        model=model,
        system=NARRATIVE_SYSTEM,
        user=(
            "Name every narrative below. Keep ids unchanged. "
            "Titles must not duplicate each other.\n\n" + "\n\n".join(lines)
        ),
        schema=NARRATIVE_NAMES_SCHEMA,
        max_completion_tokens=16384,
        complete=complete,
    )
    wanted = {n.narrative_id for n in catalog.narratives}
    rows = [r for r in (out.get("narratives") or []) if int(r["id"]) in wanted]
    names = load_names(catalog.exp_dir)
    names["source"] = catalog.exp_dir.name
    names["model"] = model
    names["narratives"] = rows
    path = save_names(catalog.exp_dir, names)
    return {"n_narratives": len(rows), "path": str(path)}


def name_leaves(
    exp_dir: Path | None = None,
    *,
    model: str = DEFAULT_MODEL,
    batch_size: int = 20,
    limit: int | None = None,
    complete: CompleteFn | None = None,
) -> dict[str, Any]:
    catalog = load_catalog(exp_dir or DEFAULT_EXP_DIR)
    names = load_names(catalog.exp_dir)
    done = _named_ids(list(names.get("leaves") or []))
    pending = [lf for lf in catalog.leaves_by_id.values() if lf.leaf_id not in done]
    pending.sort(key=lambda x: x.leaf_id)
    if limit is not None:
        pending = pending[: max(0, int(limit))]
    n_new = 0
    for batch in _chunks(pending, batch_size):
        parts = []
        for lf in batch:
            samples = lf.samples[:4] or [lf.medoid]
            parts.append(
                f"id={lf.leaf_id} size={lf.size}\nmedoid: {lf.medoid}\nsamples:\n"
                + "\n".join(f"  - {s}" for s in samples)
            )
        out = _complete(
            model=model,
            system=LEAF_SYSTEM,
            user="Name each leaf. Keep ids unchanged.\n\n" + "\n\n".join(parts),
            schema=LEAF_NAMES_SCHEMA,
            max_completion_tokens=8192,
            complete=complete,
        )
        allowed = {lf.leaf_id for lf in batch}
        fresh = [r for r in (out.get("leaves") or []) if int(r["id"]) in allowed]
        names.setdefault("leaves", []).extend(fresh)
        names["model"] = model
        save_names(catalog.exp_dir, names)
        n_new += len(fresh)
    return {
        "n_new": n_new,
        "n_leaves": len(names.get("leaves") or []),
        "path": str(names_path(catalog.exp_dir)),
    }


def reassign_leaves(
    exp_dir: Path | None = None,
    *,
    model: str = DEFAULT_MODEL,
    batch_size: int = 20,
    limit: int | None = None,
    complete: CompleteFn | None = None,
) -> dict[str, Any]:
    catalog = load_catalog(exp_dir or DEFAULT_EXP_DIR)
    names = load_names(catalog.exp_dir)
    nar_titles = {int(r["id"]): str(r.get("title") or "") for r in names.get("narratives") or []}
    if not nar_titles:
        raise ValueError("names.json has no narratives; run name-narratives first")
    leaf_titles = {int(r["id"]): str(r.get("title") or "") for r in names.get("leaves") or []}
    mapping = load_membership(catalog.exp_dir)
    pending = [i for i in sorted(catalog.leaves_by_id) if i not in mapping]
    if limit is not None:
        pending = pending[: max(0, int(limit))]
    nar_block = "\n".join(f"{nid}: {title}" for nid, title in sorted(nar_titles.items()))
    n_new = 0
    for batch in _chunks(pending, batch_size):
        parts = []
        for lid in batch:
            lf = catalog.leaves_by_id[lid]
            title = leaf_titles.get(lid) or lf.medoid
            parts.append(
                f"leaf_id={lid} current_narrative={lf.narrative_id} title={title}\n"
                f"medoid: {lf.medoid}"
            )
        out = _complete(
            model=model,
            system=REASSIGN_SYSTEM,
            user=(
                "Narratives (id: title):\n"
                f"{nar_block}\n\n"
                "Assign each leaf to one narrative_id from the list, or -1.\n\n"
                + "\n\n".join(parts)
            ),
            schema=REASSIGN_SCHEMA,
            max_completion_tokens=4096,
            complete=complete,
        )
        allowed = set(batch)
        allowed_nar = set(nar_titles) | {-1}
        for row in out.get("assignments") or []:
            lid = int(row["leaf_id"])
            nid = int(row["narrative_id"])
            if lid not in allowed or nid not in allowed_nar:
                continue
            mapping[lid] = nid
            if nid != catalog.leaves_by_id[lid].narrative_id:
                n_new += 1
        save_membership(catalog.exp_dir, mapping)
    n_over = sum(
        1
        for lid, nid in mapping.items()
        if lid in catalog.leaves_by_id and nid != catalog.leaves_by_id[lid].narrative_id
    )
    return {
        "n_overrides": n_over,
        "n_written": len(mapping),
        "n_new_this_run": n_new,
        "path": str(catalog.exp_dir / "membership.json"),
    }
