"""
Cluster browser — inspect one ``cluster`` output.

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
    DEFAULT_ANTI_HIGH,
    LEAF_NEIGHBOR_K,
    MIN_TWOMEANS_SIZE,
    BrowseBundle,
    ClusterRow,
    attach_leaf_alignment,
    filter_sort_clusters,
    load_browse_bundle,
    load_occurrence_index,
    members_for_cluster,
    neighbors_for_cluster,
    occurrences_for_member,
)

_PAGE_SIZE = 80
_HOME_PAGE_SIZE = 50
_SORT_LABELS = {
    "n": "n (grouped texts)",
    "intra": "mean intra cosine",
    "nn_cosine": "nearest leaf cosine",
    "split_gain": "2-means split gain",
    "alignment": "mean alignment (0 anti … 1 pro)",
}


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


def _fmt_num(val: float | None, *, digits: int = 4) -> str:
    if val is None:
        return "—"
    return f"{val:.{digits}f}"


def _child_tag(child: int | None) -> str:
    if child == 0:
        return "A"
    if child == 1:
        return "B"
    return "—"


def _cluster_by_id(bundle: BrowseBundle, cluster_id: int) -> ClusterRow | None:
    return next((c for c in bundle.clusters if c.cluster_id == int(cluster_id)), None)


def _query_get(name: str, default: str = "") -> str:
    try:
        val = st.query_params.get(name, default)
    except Exception:
        return default
    if isinstance(val, list):
        val = val[0] if val else default
    return default if val is None else str(val)


_HOME_NAV_KEYS = (
    "home_sort",
    "home_min_n",
    "home_desc",
    "home_leaf_page",
    "home_max_align",
)


def _save_home_nav() -> None:
    """Copy home-list widgets into a snapshot that survives leaving the page."""
    snap = st.session_state.setdefault("_home_nav", {})
    for key in _HOME_NAV_KEYS:
        if key in st.session_state:
            snap[key] = st.session_state[key]
    if "_home_filt" in st.session_state:
        snap["_home_filt"] = st.session_state["_home_filt"]


def _restore_home_nav() -> None:
    """Re-apply snapshot only for keys Streamlit dropped after leaving home."""
    snap = st.session_state.get("_home_nav") or {}
    for key in _HOME_NAV_KEYS:
        if key not in st.session_state and key in snap:
            st.session_state[key] = snap[key]
    if "_home_filt" not in st.session_state and "_home_filt" in snap:
        st.session_state["_home_filt"] = snap["_home_filt"]
    st.session_state.setdefault("home_sort", "n")
    st.session_state.setdefault("home_min_n", MIN_TWOMEANS_SIZE)
    st.session_state.setdefault("home_desc", True)
    st.session_state.setdefault("home_leaf_page", 0)
    st.session_state.setdefault("home_max_align", 1.0)


def _goto(*, view: str, cid: int | None = None) -> None:
    _save_home_nav()
    params: dict[str, str] = {"view": view}
    if cid is not None:
        params["cid"] = str(int(cid))
    st.query_params.clear()
    st.query_params.update(params)
    st.rerun()


def _nav_state(bundle: BrowseBundle) -> tuple[str, ClusterRow | None]:
    view = _query_get("view", "home") or "home"
    if view not in {"home", "cluster", "neighbors"}:
        view = "home"
    cid_raw = _query_get("cid", "")
    row = None
    if cid_raw:
        try:
            row = _cluster_by_id(bundle, int(cid_raw))
        except ValueError:
            row = None
    if view in {"cluster", "neighbors"} and row is None:
        view = "home"
    return view, row


def _leaf_caption(row: ClusterRow) -> str:
    bits = [
        f"n={row.size}",
        f"intra={_fmt_num(row.mean_intra_cosine)}",
        f"nn={_fmt_num(row.nn_cosine)}" + (f" (leaf {row.nn_id})" if row.nn_id is not None else ""),
        f"split_gain={_fmt_num(row.split_gain)}",
        f"align={_fmt_num(row.mean_alignment)}"
        + (f" ({int(round(100 * row.frac_anti))}% ≤{DEFAULT_ANTI_HIGH:g} anti)" if row.frac_anti is not None else ""),
    ]
    return "  ·  ".join(bits)


def _pager(*, n_items: int, page_size: int, state_key: str, btn_prefix: str) -> tuple[int, int]:
    n_pages = max(1, (int(n_items) + int(page_size) - 1) // int(page_size))
    if state_key not in st.session_state:
        st.session_state[state_key] = 0
    page = min(int(st.session_state[state_key]), n_pages - 1)
    st.session_state[state_key] = page
    if n_pages > 1:
        c1, c2, c3 = st.columns([1, 2, 1])
        if c1.button("Prev", key=f"{btn_prefix}_prev") and page > 0:
            st.session_state[state_key] = page - 1
            st.rerun()
        c2.caption(f"page {page + 1} / {n_pages}")
        if c3.button("Next", key=f"{btn_prefix}_next") and page + 1 < n_pages:
            st.session_state[state_key] = page + 1
            st.rerun()
    start = page * int(page_size)
    return start, start + int(page_size)


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


def _leaf_actions(row: ClusterRow, *, key_prefix: str, members: bool = False, neighbors: bool = True) -> None:
    labels: list[tuple[str, str, str, int | None]] = [("Back", f"{key_prefix}_back", "home", None)]
    if members:
        labels.append(("Members", f"{key_prefix}_members", "cluster", row.cluster_id))
    if neighbors:
        labels.append(("Neighbors", f"{key_prefix}_nn", "neighbors", row.cluster_id))
    cols = st.columns(len(labels))
    for i, (text, key, view, cid) in enumerate(labels):
        if cols[i].button(text, key=key):
            _goto(view=view, cid=cid)


def _render_home(bundle: BrowseBundle) -> None:
    _restore_home_nav()
    st.subheader("Leaves")
    st.caption(
        "Click a leaf name to open it. n = unique grouped texts. intra = tightness. "
        "nn_cosine = nearest other leaf medoid (merge hint). "
        "split_gain = 2-means weighted child intra − parent intra (split hint). "
        "align = mean extract claim_vaccine_alignment_score (0 anti-vaccine … 1 pro)."
    )
    c1, c2, c3, c4 = st.columns([2, 1, 1.2, 1])
    sort_by = c1.selectbox(
        "Sort by",
        list(_SORT_LABELS.keys()),
        format_func=lambda k: _SORT_LABELS[k],
        key="home_sort",
    )
    min_n = int(c2.number_input("Min n", min_value=1, step=1, key="home_min_n"))
    max_align = float(
        c3.number_input(
            "Max mean align",
            min_value=0.0,
            max_value=1.0,
            step=0.05,
            key="home_max_align",
            help=f"Keep leaves with mean alignment ≤ this. 0=anti, 1=pro. {DEFAULT_ANTI_HIGH:g} ≈ anti.",
        )
    )
    descending = c4.checkbox("Descending", key="home_desc")
    shown = filter_sort_clusters(
        bundle.clusters,
        min_n=min_n,
        sort_by=str(sort_by),
        descending=bool(descending),
        max_alignment=max_align,
    )
    st.caption(
        f"{len(shown)} of {len(bundle.clusters)} leaves (min n={min_n}, max align={max_align:g})"
    )
    if not shown:
        _save_home_nav()
        st.info("No leaves pass the min-n / alignment filters.")
        return
    filt_key = (str(sort_by), min_n, bool(descending), round(max_align, 4), len(shown))
    if st.session_state.get("_home_filt") != filt_key:
        st.session_state["home_leaf_page"] = 0
        st.session_state["_home_filt"] = filt_key
    start, end = _pager(
        n_items=len(shown),
        page_size=_HOME_PAGE_SIZE,
        state_key="home_leaf_page",
        btn_prefix="home_leaf",
    )
    _save_home_nav()
    page_rows = shown[start:end]
    h1, h2, h3, h4, h5, h6 = st.columns([3.6, 0.6, 0.9, 0.9, 1.0, 0.8])
    h1.caption("leaf")
    h2.caption("n")
    h3.caption("intra")
    h4.caption("nn_cosine")
    h5.caption("split_gain")
    h6.caption("align")
    for row in page_rows:
        b1, b2, b3, b4, b5, b6 = st.columns([3.6, 0.6, 0.9, 0.9, 1.0, 0.8])
        name = f"{row.cluster_id}: {_clip(row.medoid_text, 80) or '(empty medoid)'}"
        if b1.button(name, key=f"leaf_open_{row.cluster_id}", width="stretch"):
            _goto(view="cluster", cid=row.cluster_id)
        b2.write(str(row.size))
        b3.write(_fmt_num(row.mean_intra_cosine, digits=3))
        b4.write(_fmt_num(row.nn_cosine, digits=3))
        b5.write(_fmt_num(row.split_gain, digits=3))
        b6.write(_fmt_num(row.mean_alignment, digits=2))


def _render_members(bundle: BrowseBundle, row: ClusterRow, occ_index: dict | None) -> None:
    st.subheader(f"Leaf {row.cluster_id}")
    _leaf_actions(row, key_prefix="cluster", neighbors=True)
    st.caption(_leaf_caption(row))
    if row.medoid_text:
        st.markdown("**Medoid**")
        st.write(row.medoid_text)
    if row.child_sizes and row.child_intras:
        a_n, b_n = row.child_sizes
        a_in, b_in = row.child_intras
        st.caption(
            f"2-means: A n={a_n} intra={_fmt_num(a_in)}  ·  "
            f"B n={b_n} intra={_fmt_num(b_in)}  ·  split_gain={_fmt_num(row.split_gain)}"
        )
    else:
        st.caption("2-means skipped (n < 3).")

    members = members_for_cluster(bundle, row.cluster_id)
    st.caption(f"{len(members)} member claims, sorted by cosine to medoid")
    q = st.text_input("Filter claims", key=f"q_{row.cluster_id}").strip().casefold()
    if q:
        members = [m for m in members if q in m.claim_text.casefold() or q in m.claim_key.casefold()]
        st.caption(f"{len(members)} after filter")
    if not members:
        st.write("No members.")
        return
    page_key = f"page_{row.cluster_id}"
    start, end = _pager(
        n_items=len(members),
        page_size=_PAGE_SIZE,
        state_key=page_key,
        btn_prefix=f"mem_{row.cluster_id}",
    )
    page = start // _PAGE_SIZE
    slice_rows = members[start:end]
    st.dataframe(
        [
            {
                "child": _child_tag(m.twomeans_child),
                "cos": m.cosine_to_medoid,
                "align": m.alignment,
                "occ": m.occurrence_count,
                "claim": _clip(m.claim_text, 160),
            }
            for m in slice_rows
        ],
        width="stretch",
        hide_index=True,
        height=min(420, 36 * len(slice_rows) + 38),
    )
    labels = [
        f"{_child_tag(m.twomeans_child)}  ·  cos={_fmt_num(m.cosine_to_medoid, digits=3)}  ·  "
        f"align={_fmt_num(m.alignment, digits=2)}  ·  ×{m.occurrence_count}  ·  {_clip(m.claim_text, 80)}"
        for m in slice_rows
    ]
    pick = st.selectbox(
        "Claim",
        range(len(slice_rows)),
        format_func=lambda i: labels[i],
        key=f"claim_{row.cluster_id}_{page}",
    )
    member = slice_rows[int(pick)]
    st.markdown(f"**Claim text** · 2-means child {_child_tag(member.twomeans_child)}")
    st.write(member.claim_text)
    st.caption(
        f"claim_key={member.claim_key}  ·  cos_to_medoid={_fmt_num(member.cosine_to_medoid)}  ·  "
        f"align={_fmt_num(member.alignment)}  ·  {member.occurrence_count} source post-chunk(s)"
    )
    _render_occurrences(member, occ_index)


def _render_neighbors(bundle: BrowseBundle, row: ClusterRow) -> None:
    st.subheader(f"Neighbors of leaf {row.cluster_id}")
    _leaf_actions(row, key_prefix="nn", members=True, neighbors=False)
    st.caption(_leaf_caption(row))
    if row.medoid_text:
        st.markdown("**Medoid**")
        st.write(row.medoid_text)
    ranked = neighbors_for_cluster(bundle, row.cluster_id, k=LEAF_NEIGHBOR_K)
    if not ranked:
        st.info("No other leaves.")
        return
    st.caption(f"Top {len(ranked)} other leaves by medoid cosine — click a name to open that leaf")
    h1, h2, h3, h4, h5 = st.columns([4.2, 1.0, 0.7, 1.0, 1.1])
    h1.caption("leaf")
    h2.caption("cosine")
    h3.caption("n")
    h4.caption("intra")
    h5.caption("split_gain")
    for other, cos in ranked:
        b1, b2, b3, b4, b5 = st.columns([4.2, 1.0, 0.7, 1.0, 1.1])
        name = f"{other.cluster_id}: {_clip(other.medoid_text, 90) or '(empty medoid)'}"
        if b1.button(name, key=f"nn_open_{row.cluster_id}_{other.cluster_id}", width="stretch"):
            _goto(view="cluster", cid=other.cluster_id)
        b2.write(f"{cos:.3f}")
        b3.write(str(other.size))
        b4.write(_fmt_num(other.mean_intra_cosine, digits=3))
        b5.write(_fmt_num(other.split_gain, digits=3))


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
        st.info("Pass `--from <cluster-dir>` (or `--labels`) to open an output.")
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
            args.selection,
            tuple(args.filter_specs or ()),
            str(args.claims) if args.claims else None,
        )
    except Exception as exc:  # noqa: BLE001
        st.error(str(exc))
        return

    out = bundle.output
    if out.meta_path:
        st.sidebar.caption(str(out.meta_path))
    st.sidebar.markdown(f"**Claims:** {int(bundle.labels.shape[0])}")
    st.sidebar.markdown(f"**Leaves:** {len(bundle.clusters)}")
    if bundle.applied_selection:
        st.sidebar.markdown(f"**Selection:** `{bundle.applied_selection}`")
    elif out.hint.raw:
        st.sidebar.markdown(f"**Selection (from JSON):** `{out.hint.raw}`")
    if bundle.applied_filters:
        st.sidebar.markdown("**Filters:** " + ", ".join(f"`{s}`" for s in bundle.applied_filters))
    if bundle.corpus:
        st.sidebar.markdown(f"**Corpus:** `{bundle.corpus}`")
    payload = out.payload or {}
    if payload.get("algorithm"):
        st.sidebar.caption(f"algorithm={payload.get('algorithm')}")
    if payload.get("n_clusters_frac") is not None:
        st.sidebar.caption(f"n_clusters_frac={payload.get('n_clusters_frac')}")
    if payload.get("n_clusters") is not None:
        st.sidebar.caption(f"n_clusters={payload.get('n_clusters')}")

    occ_index = None
    if bundle.claims_path and Path(bundle.claims_path).is_file():
        occ_index = _cached_occ(str(bundle.claims_path))
        attach_leaf_alignment(bundle, occ_index)
        n_scored = sum(1 for c in bundle.clusters if c.mean_alignment is not None)
        st.sidebar.caption(f"{len(occ_index)} chunks indexed")
        st.sidebar.caption(f"{n_scored}/{len(bundle.clusters)} leaves have extract alignment")
    else:
        st.sidebar.warning("No claims.json — post metadata unavailable")

    st.title("Cluster browser")
    if not bundle.clusters:
        st.warning("No clusters in labels.")
        return

    view, row = _nav_state(bundle)
    if view == "cluster" and row is not None:
        _render_members(bundle, row, occ_index)
    elif view == "neighbors" and row is not None:
        _render_neighbors(bundle, row)
    else:
        _render_home(bundle)


if __name__ == "__main__":
    main()
