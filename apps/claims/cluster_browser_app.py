"""
Cluster browser — inspect one ``cluster`` / ``hierarchy`` output.

From the repository root::

  python -m apps.claims cluster-browse --from <experiment-dir>
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from apps.claims.clustering.browse import (
    BrowseBundle,
    ClusterRow,
    load_browse_bundle,
    load_occurrence_index,
    members_for_cluster,
    occurrences_for_member,
)

_PAGE_SIZE = 80


def _script_argv() -> list[str]:
    """Args for this app. Streamlit rewrites argv to ``[script, *user_args]`` (no ``--``)."""
    argv = list(sys.argv[1:])
    if "--" in argv:
        argv = argv[argv.index("--") + 1 :]
    skip_prefixes = ("--server.", "--browser.", "--global.", "--logger.", "--client.")
    out: list[str] = []
    skip_next = False
    for a in argv:
        if skip_next:
            skip_next = False
            continue
        if a in {"streamlit", "run"} or a.endswith("cluster_browser_app.py"):
            continue
        if a.startswith(skip_prefixes):
            continue
        out.append(a)
    return out


def parse_app_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--from", dest="from_path", type=Path, default=None)
    ap.add_argument("--labels", type=Path, default=None)
    ap.add_argument("--parent-labels", dest="parent_labels", type=Path, default=None)
    ap.add_argument("--corpus", type=str, default=None)
    ap.add_argument("--model-tag", dest="model_tag", type=str, default=None)
    ap.add_argument("--run-dir", dest="run_dir", type=Path, default=None)
    ap.add_argument("--claims", type=Path, default=None)
    ap.add_argument("--selection", type=str, default=None)
    ap.add_argument("--filter", action="append", default=None)
    ap.add_argument("--where-annotation", dest="where_annotation", type=str, default=None)
    ap.add_argument("--eq", type=str, default=None)
    ap.add_argument("--low", type=float, default=None)
    ap.add_argument("--high", type=float, default=None)
    args, _unknown = ap.parse_known_args(argv if argv is not None else _script_argv())
    if args.from_path is None:
        env_from = os.environ.get("WMVI_CLUSTER_BROWSE_FROM")
        if env_from:
            args.from_path = Path(env_from)
    specs = list(args.filter or [])
    if args.where_annotation:
        bits = []
        if args.eq is not None:
            bits.append(f"eq={args.eq}")
        if args.low is not None:
            bits.append(f"low={args.low}")
        if args.high is not None:
            bits.append(f"high={args.high}")
        if bits:
            specs.append(f"{args.where_annotation}:{','.join(bits)}")
    args.filter_specs = specs
    return args


@st.cache_resource(show_spinner="Loading cluster output…")
def _cached_bundle(
    source: str,
    corpus: str | None,
    model_tag: str | None,
    run_dir: str | None,
    labels: str | None,
    parent_labels: str | None,
    selection: str | None,
    filter_specs: tuple[str, ...],
    claims: str | None,
) -> BrowseBundle:
    return load_browse_bundle(
        Path(source),
        corpus=corpus,
        model_tag=model_tag,
        run_dir=Path(run_dir) if run_dir else None,
        labels=Path(labels) if labels else None,
        parent_labels=Path(parent_labels) if parent_labels else None,
        selection=selection,
        filter_specs=list(filter_specs) or None,
        claims_path=Path(claims) if claims else None,
    )


@st.cache_resource(show_spinner="Indexing post-chunks…")
def _cached_occ(claims_path: str) -> dict:
    return load_occurrence_index(Path(claims_path))


def _clip(text: str, n: int = 140) -> str:
    t = " ".join((text or "").split())
    return t if len(t) <= n else t[: n - 1] + "…"


def _pick_row(label: str, rows: list[ClusterRow], *, prefix: str, key: str) -> int:
    fmt = lambda i: _fmt_cluster(rows[i], prefix=prefix)
    if len(rows) > 40:
        return int(st.selectbox(label, range(len(rows)), format_func=fmt, key=key))
    return int(
        st.radio(label, range(len(rows)), format_func=fmt, key=key, label_visibility="collapsed")
    )


def _fmt_cluster(row: ClusterRow, *, prefix: str) -> str:
    tight = f"{row.mean_intra_cosine:.3f}" if row.mean_intra_cosine is not None else "—"
    extra = f" · {row.n_children} leaves" if row.n_children is not None else ""
    return f"{prefix} {row.cluster_id}  ·  n={row.size}{extra}  ·  tight={tight}  ·  {_clip(row.medoid_text, 80)}"


def _render_occurrences(member, occ_index: dict | None) -> None:
    if occ_index is None:
        st.info("No claims.json found — occurrence pointers only.")
        for src in member.sources:
            st.code(str(src), language="json")
        return
    rows = occurrences_for_member(member, occ_index)
    if not rows:
        st.caption("No source occurrences on this group.")
        return
    for i, occ in enumerate(rows):
        plat = occ.get("platform") or "unknown"
        metric = occ.get("primary_metric")
        metric_s = f" · engagement={metric}" if metric is not None else ""
        date = occ.get("created_at_ts") or ""
        title = (
            occ.get("youtube_video_title")
            or occ.get("reddit_submission_title")
            or occ.get("reddit_comment_submission_title")
            or occ.get("podcast_name")
            or occ.get("telegram_channel")
            or ""
        )
        head = f"{plat}{metric_s}"
        if date:
            head += f" · {date}"
        if not occ.get("found"):
            head += " · (chunk not found in claims.json)"
        with st.expander(head, expanded=(i == 0 and len(rows) <= 4)):
            if title:
                st.caption(str(title))
            url = occ.get("url")
            if url:
                st.markdown(f"[open post]({url})")
            meta_bits = []
            if occ.get("post_id") is not None:
                meta_bits.append(f"post_id={occ['post_id']}")
            if occ.get("chunk_index") is not None:
                meta_bits.append(f"chunk={occ['chunk_index']}")
            if occ.get("task_id"):
                meta_bits.append(f"task_id={occ['task_id']}")
            if meta_bits:
                st.caption(" · ".join(str(b) for b in meta_bits))
            st.markdown("**Chunk**")
            st.text(occ.get("chunk_text") or "(empty)")
            st.markdown("**Extracted claim in this chunk**")
            st.write(occ.get("claim_in_chunk") or member.claim_text)


def _render_members(bundle: BrowseBundle, cluster_id: int, *, level: str, occ_index: dict | None) -> None:
    members = members_for_cluster(bundle, cluster_id, level=level)
    st.caption(f"{len(members)} member claims")
    q = st.text_input("Filter claims", key=f"q_{level}_{cluster_id}").strip().casefold()
    if q:
        members = [m for m in members if q in m.claim_text.casefold() or q in m.claim_key.casefold()]
        st.caption(f"{len(members)} after filter")
    if not members:
        st.write("No members.")
        return
    page_key = f"page_{level}_{cluster_id}"
    if page_key not in st.session_state:
        st.session_state[page_key] = 0
    n_pages = max(1, (len(members) + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = min(int(st.session_state[page_key]), n_pages - 1)
    if n_pages > 1:
        c1, c2, c3 = st.columns([1, 2, 1])
        if c1.button("Prev", key=f"prev_{level}_{cluster_id}") and page > 0:
            st.session_state[page_key] = page - 1
            st.rerun()
        c2.caption(f"page {page + 1} / {n_pages}")
        if c3.button("Next", key=f"next_{level}_{cluster_id}") and page + 1 < n_pages:
            st.session_state[page_key] = page + 1
            st.rerun()
    slice_rows = members[page * _PAGE_SIZE : (page + 1) * _PAGE_SIZE]
    labels = [f"{m.claim_key}  ·  ×{m.occurrence_count}  ·  {_clip(m.claim_text, 110)}" for m in slice_rows]
    pick = st.selectbox("Claim", range(len(slice_rows)), format_func=lambda i: labels[i], key=f"claim_{level}_{cluster_id}_{page}")
    member = slice_rows[int(pick)]
    st.markdown("**Claim text**")
    st.write(member.claim_text)
    st.caption(f"claim_key={member.claim_key}  ·  {member.occurrence_count} source post-chunk(s)")
    _render_occurrences(member, occ_index)


def main() -> None:
    st.set_page_config(page_title="Cluster browser", layout="wide")
    try:
        args = parse_app_args()
    except SystemExit:
        st.error("Could not parse arguments. Pass `--from <experiment-dir>`.")
        return

    st.sidebar.title("Cluster browser")
    default_from = str(args.from_path) if args.from_path else ""
    source_s = st.sidebar.text_input("Experiment dir / JSON / labels npy", value=default_from)
    if not source_s.strip() and args.labels is None:
        st.info("Pass `--from <cluster-or-hierarchy-dir>` (or `--labels`) to open an output.")
        st.code(
            "python -m apps.claims cluster-browse --from "
            "apps/claims/data/experiments/clustering/<corpus>/<tag>/<exp>"
        )
        return

    source = Path(source_s.strip()) if source_s.strip() else Path(args.labels)
    try:
        bundle = _cached_bundle(
            str(source),
            args.corpus,
            args.model_tag,
            str(args.run_dir) if args.run_dir else None,
            str(args.labels) if args.labels else None,
            str(args.parent_labels) if args.parent_labels else None,
            args.selection,
            tuple(args.filter_specs or ()),
            str(args.claims) if args.claims else None,
        )
    except Exception as exc:  # noqa: BLE001
        st.error(str(exc))
        return

    out = bundle.output
    st.sidebar.markdown(f"**Kind:** `{out.kind}`")
    if out.meta_path:
        st.sidebar.caption(str(out.meta_path))
    st.sidebar.markdown(f"**Claims:** {int(bundle.labels.shape[0])}")
    if bundle.applied_selection:
        st.sidebar.markdown(f"**Selection:** `{bundle.applied_selection}`")
    elif out.hint.raw:
        st.sidebar.markdown(f"**Selection (from JSON):** `{out.hint.raw}`")
    if bundle.applied_filters:
        st.sidebar.markdown("**Filters:** " + ", ".join(f"`{s}`" for s in bundle.applied_filters))
    if bundle.corpus:
        st.sidebar.markdown(f"**Corpus:** `{bundle.corpus}`")
    payload = out.payload or {}
    if payload.get("preset"):
        st.sidebar.caption(f"preset={payload.get('preset')}  leaf={payload.get('leaf_algorithm')}  narrative={payload.get('narrative_algorithm')}")
    elif payload.get("algorithm"):
        st.sidebar.caption(f"algorithm={payload.get('algorithm')}")

    occ_index = None
    if bundle.claims_path and Path(bundle.claims_path).is_file():
        occ_index = _cached_occ(str(bundle.claims_path))
        st.sidebar.caption(f"{len(occ_index)} chunks indexed")
    else:
        st.sidebar.warning("No claims.json — post metadata unavailable")

    st.title("Cluster browser")
    if out.kind == "hierarchy" and bundle.narratives:
        nars = bundle.narratives
        nar_i = st.selectbox(
            "Narrative",
            range(len(nars)),
            format_func=lambda i: _fmt_cluster(nars[i], prefix="narrative"),
        )
        nar = nars[int(nar_i)]
        leaves = [c for c in bundle.clusters if c.parent_id == nar.cluster_id]
        if not leaves:
            leaves = bundle.clusters
        left, right = st.columns([1, 2])
        with left:
            st.subheader("Leaves")
            leaf_i = _pick_row("Leaf", leaves, prefix="leaf", key=f"leaf_{nar.cluster_id}")
        with right:
            leaf = leaves[int(leaf_i)]
            st.subheader(f"Leaf {leaf.cluster_id}")
            tight = f"{leaf.mean_intra_cosine:.4f}" if leaf.mean_intra_cosine is not None else "—"
            st.caption(f"size={leaf.size}  ·  mean_intra_cosine={tight}")
            if leaf.medoid_text:
                st.markdown("**Medoid**")
                st.write(leaf.medoid_text)
            _render_members(bundle, leaf.cluster_id, level="leaf", occ_index=occ_index)
    else:
        clusters = bundle.clusters
        if not clusters:
            st.warning("No clusters in labels.")
            return
        left, right = st.columns([1, 2])
        with left:
            st.subheader("Clusters")
            ci = _pick_row("Cluster", clusters, prefix="cluster", key="cluster_flat")
        with right:
            row = clusters[int(ci)]
            st.subheader(f"Cluster {row.cluster_id}")
            tight = f"{row.mean_intra_cosine:.4f}" if row.mean_intra_cosine is not None else "—"
            st.caption(f"size={row.size}  ·  mean_intra_cosine={tight}")
            if row.medoid_text:
                st.markdown("**Medoid**")
                st.write(row.medoid_text)
            _render_members(bundle, row.cluster_id, level="leaf", occ_index=occ_index)


if __name__ == "__main__":
    main()
