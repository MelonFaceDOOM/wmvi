"""
Streamlit lab for embedding extracted claims under reusable profiles, then
searching, graphing, clustering, and scoring an editable triplet eval set.

From the **repository root** (so ``apps`` is importable), run::

  streamlit run apps/claim_extractor/embedding_lab/app.py

Setup, corporate-network SSL, and run transfer: see ``embedding_lab/README.md``.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from apps.claim_extractor.embedding_lab import claims_data, clustering, db, embed_runner, projection
from apps.claim_extractor.embedding_lab.eval_triplets import score_triplets
from apps.claim_extractor.embedding_lab.models import (
    DEFAULT_DOC_INSTRUCTION,
    DEFAULT_MODEL,
    DEFAULT_QUERY_INSTRUCTION,
    SEED_MODELS,
)
from apps.claim_extractor.learned.constants import REPO_ROOT

DEFAULT_SOURCE = str(REPO_ROOT / "data" / "posts_with_claims_full_v2.json")
_TOOLTIP_CHARS = 160


# --- small helpers (patterned on refinement_lab/app.py) ---


def _frag_rerun() -> None:
    st.rerun(scope="fragment")


def _app_rerun() -> None:
    """Full rerun so all tabs see DB/artifact changes (fragment rerun is Profiles-only)."""
    st.rerun(scope="app")


def _open_db():
    conn = db.connect(Path(st.session_state["emb_db_in"]))
    db.init_lab(conn)
    return conn


def _ensure_selectbox_key(key: str, options: list[str], *, preferred: str | None = None) -> None:
    if not options:
        return
    if st.session_state.get(key) not in options:
        st.session_state[key] = preferred if preferred in options else options[0]


def _truncate(text: str, n: int = _TOOLTIP_CHARS) -> str:
    text = (text or "").replace("\n", " ").strip()
    return text if len(text) <= n else text[: n - 1] + "\u2026"


def _source_fingerprint(path_str: str) -> str:
    p = claims_data._resolve_path(path_str, REPO_ROOT)
    if p is None:
        return "missing"
    stt = p.stat()
    return f"{p}:{stt.st_size}:{int(stt.st_mtime)}"


@st.cache_data(show_spinner="Loading claims…")
def _cached_load_claims(path_str: str, fingerprint: str):
    return claims_data.load_claims(path_str, repo_root=REPO_ROOT)


def _load_claims() -> tuple[claims_data.ClaimsBundle | None, str | None]:
    path_str = st.session_state["emb_source_in"]
    return _cached_load_claims(path_str, _source_fingerprint(path_str))


@st.cache_data(show_spinner=False)
def _cached_run_arrays(artifact_dir: str, vectors_fingerprint: str):
    return embed_runner.load_run_arrays(Path(artifact_dir))


def _load_run_arrays(artifact_dir: str):
    vecs_path = Path(artifact_dir) / embed_runner.VECTORS_FILE
    if not vecs_path.is_file():
        raise FileNotFoundError(f"Missing vectors at {vecs_path}")
    fp = f"{vecs_path.stat().st_size}:{int(vecs_path.stat().st_mtime)}"
    return _cached_run_arrays(artifact_dir, fp)


def _run_label(run: dict) -> str:
    unique = run.get("claim_count", 0)
    total = run.get("source_claim_count")
    count_txt = f"{unique:,} unique"
    if total and int(total) != int(unique):
        count_txt = f"{unique:,} unique / {int(total):,} source"
    return (
        f"{run['id']}: {run.get('profile_name', '?')} \u00b7 "
        f"{count_txt} \u00b7 {run.get('created_at', '')}"
    )


def _fmt_metric(value, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.2f}{suffix}"
    return f"{value}{suffix}"


def _render_group_sources(group: dict, posts: dict[str, dict], *, max_inline: int = 3) -> None:
    sources = group.get("sources") or []
    for src in sources[:max_inline]:
        tid = str(src.get("task_id", "?"))
        row_id = str(src.get("row_id", tid))
        st.caption(f"`{row_id}`")
        post = posts.get(tid)
        if post is not None:
            ptext = post.get("text_coreference_resolved") or post.get("text") or ""
            if ptext:
                st.caption(_truncate(str(ptext), 220))
    extra = len(sources) - max_inline
    if extra > 0:
        with st.expander(f"{extra} more source post(s)"):
            for src in sources[max_inline:]:
                tid = str(src.get("task_id", "?"))
                row_id = str(src.get("row_id", tid))
                st.caption(f"`{row_id}`")
                post = posts.get(tid)
                if post is not None:
                    ptext = post.get("text_coreference_resolved") or post.get("text") or ""
                    if ptext:
                        st.caption(_truncate(str(ptext), 220))


def _cluster_display_name(cluster_id: int, names: dict[int, dict]) -> str:
    meta = names.get(cluster_id)
    if meta and meta.get("name"):
        return str(meta["name"])
    if cluster_id == -1:
        return "noise"
    return f"Cluster {cluster_id}"


def _persist_cluster_names(
    conn,
    *,
    cluster_run_id: int,
    claim_texts: list[str],
    labels: np.ndarray,
) -> dict[int, dict]:
    sizes = clustering.cluster_sizes(labels)
    auto_names = clustering.name_clusters_tfidf(claim_texts, labels)
    db.upsert_cluster_names(conn, cluster_run_id=cluster_run_id, names=auto_names, sizes=sizes)
    return db.list_cluster_names(conn, cluster_run_id)


# --- Source Data tab ---


@st.fragment
def _render_source_tab() -> None:
    st.caption("Point the lab at a claims JSON. The embedding unit is the claim text alone.")
    st.text_input("SQLite path", key="emb_db_in")
    st.text_input("Source claims JSON", key="emb_source_in")

    bundle, err = _load_claims()
    if err:
        st.error(err)
        return
    assert bundle is not None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unique claims", f"{bundle.claim_count:,}")
    c2.metric("Source claims", f"{bundle.source_claim_count:,}")
    c3.metric("Posts", f"{bundle.post_count:,}")
    c4.metric("Collapsed", f"{bundle.source_claim_count - bundle.claim_count:,}")
    if bundle.source_claim_count:
        st.caption(
            f"Dedup ratio: **{bundle.claim_count / bundle.source_claim_count:.1%}** unique "
            f"({bundle.source_claim_count:,} \u2192 {bundle.claim_count:,})"
        )
    st.caption(f"Source hash: `{bundle.source_hash[:12]}`")

    st.markdown("**Sample claim groups**")
    sample = bundle.groups[:25]
    df = pd.DataFrame(
        [
            {
                "group_id": g.group_id,
                "claim": g.claim_text,
                "occurrences": g.count,
            }
            for g in sample
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)


# --- Embedding Profiles tab ---


@st.fragment
def _render_profiles_tab() -> None:
    conn = _open_db()
    try:
        _render_profiles_tab_content(conn)
    finally:
        conn.close()


def _render_profiles_tab_content(conn) -> None:
    st.caption("A profile = model + doc/query instructions + normalize. Running embeds the full claim set.")

    flash = st.session_state.pop("flash_embed_run", None)
    if flash:
        st.success(
            f"Embedded **{flash['claim_count']}** unique claims "
            f"({flash.get('source_claim_count', flash['claim_count'])} source) "
            f"in {flash['wall_seconds']}s on **{flash['device']}** "
            f"({_fmt_metric(flash.get('claims_per_sec'))} groups/s)."
        )

    new_name = st.text_input("New profile name", key="emb_new_profile", placeholder="e.g. bge-small-no-instruction")
    if st.button("Create profile", key="emb_prof_create"):
        if not new_name.strip():
            st.error("Name required.")
        else:
            pid = db.create_embed_profile(conn, name=new_name.strip())
            st.session_state["emb_edit_profile_id"] = pid
            _frag_rerun()

    profiles = db.list_embed_profiles(conn)
    if not profiles:
        st.info("Create a profile to begin.")
        return

    options = {f"{p.id}: {p.name}": p.id for p in profiles}
    keys = list(options.keys())
    preferred = next(
        (k for k, v in options.items() if v == st.session_state.get("emb_edit_profile_id", profiles[0].id)),
        keys[0],
    )
    _ensure_selectbox_key("emb_edit_profile_select", keys, preferred=preferred)
    st.selectbox("Edit profile", options=keys, key="emb_edit_profile_select")
    profile_id = options[st.session_state["emb_edit_profile_select"]]
    st.session_state["emb_edit_profile_id"] = profile_id
    profile = db.get_embed_profile(conn, profile_id)
    if profile is None:
        st.error("Profile not found.")
        return

    name = st.text_input("Name", value=profile.name, key=f"emb_name_{profile_id}")
    model_options = list(SEED_MODELS)
    if profile.model_id not in model_options:
        model_options = [profile.model_id, *model_options]
    _ensure_selectbox_key(f"emb_model_{profile_id}", model_options, preferred=profile.model_id)
    model_sel = st.selectbox("Model", options=model_options, key=f"emb_model_{profile_id}")
    custom_model = st.text_input("Or custom model id", value="", key=f"emb_model_custom_{profile_id}")
    model_id = custom_model.strip() or model_sel
    doc_instruction = st.text_input(
        "Doc instruction (corpus side; usually empty for BGE v1.5)",
        value=profile.doc_instruction,
        key=f"emb_doc_{profile_id}",
    )
    query_instruction = st.text_input(
        "Query instruction (search side only)",
        value=profile.query_instruction,
        key=f"emb_query_{profile_id}",
    )
    normalize = st.checkbox("Normalize embeddings", value=profile.normalize, key=f"emb_norm_{profile_id}")

    if st.button("Save profile", key=f"emb_save_{profile_id}"):
        db.update_embed_profile(
            conn,
            profile_id,
            name=name,
            model_id=model_id,
            doc_instruction=doc_instruction,
            query_instruction=query_instruction,
            normalize=normalize,
        )
        st.success("Saved.")
        _frag_rerun()

    st.divider()
    bundle, err = _load_claims()
    if err:
        st.error(err)
        return
    assert bundle is not None

    st.subheader("Compute device")
    cuda_hint = embed_runner.probe_cuda()
    st.caption(
        "PyTorch sees: "
        + (
            f"CUDA ({cuda_hint.get('cuda_device_name', '?')})"
            if cuda_hint.get("cuda_available")
            else (
                "CPU-only wheel (nvidia-smi GPU not usable by this Python — see README)"
                if cuda_hint.get("cpu_only_wheel")
                else "CPU only (CUDA not available)"
            )
        )
    )
    gpu_col, _ = st.columns([1, 3])
    with gpu_col:
        if st.button("Test GPU / device", key=f"emb_gpu_test_{profile_id}"):
            with st.spinner(f"Loading {model_id} and running encode test…"):
                st.session_state[f"emb_gpu_info_{profile_id}"] = embed_runner.probe_compute_device(
                    model_id=model_id,
                    run_encode_test=True,
                )
    gpu_info = st.session_state.get(f"emb_gpu_info_{profile_id}")
    if gpu_info:
        st.markdown(embed_runner.format_device_report(gpu_info))

    existing = db.get_embed_run_for(conn, profile_id, bundle.source_hash)
    if existing:
        st.info(f"This profile already has a run for the current source (run id={existing['id']}).")
    force = st.checkbox("Force re-run (overwrite existing)", value=False, key=f"emb_force_{profile_id}")

    if st.button("Run embedding on source", key=f"emb_run_{profile_id}", type="primary"):
        if existing and not force:
            st.warning("Run already exists; enable 'Force re-run' to overwrite.")
        else:
            _run_embedding(conn, profile, bundle)

    st.divider()
    st.subheader("Run history")
    runs = db.list_embed_runs(conn, profile_id=profile_id)
    if not runs:
        st.caption("(no runs yet)")
    for run in runs:
        with st.expander(_run_label(run), expanded=False):
            st.markdown(
                f"- device: **{run.get('device')}** \u00b7 wall: **{_fmt_metric(run.get('wall_seconds'), 's')}** "
                f"\u00b7 throughput: **{_fmt_metric(run.get('claims_per_sec'))}/s**\n"
                f"- peak RAM: **{_fmt_metric(run.get('peak_ram_mb'), ' MB')}** "
                f"(delta {_fmt_metric(run.get('ram_delta_mb'), ' MB')}) "
                f"\u00b7 peak GPU: **{_fmt_metric(run.get('peak_gpu_mb'), ' MB')}**\n"
                f"- vectors: **{run.get('claim_count')}** unique"
                + (
                    f" / **{run.get('source_claim_count')}** source"
                    if run.get("source_claim_count")
                    else ""
                )
                + f" x **{run.get('vector_dim')}** {run.get('dtype')} "
                f"\u00b7 artifact: **{_fmt_metric((run.get('artifact_bytes') or 0) / 1e6, ' MB')}**\n"
                f"- dir: `{run.get('artifact_dir')}`"
            )

    st.divider()
    st.subheader("Delete profile")
    n_runs = len(runs)
    st.caption(
        f"Permanently delete **{profile.name}** (id={profile_id})"
        + (
            f" and **{n_runs}** embedding run(s) with on-disk vectors."
            if n_runs
            else " (no embedding runs yet)."
        )
    )
    delete_confirm = st.checkbox("I understand this cannot be undone", key=f"emb_delete_confirm_{profile_id}")
    if st.button(
        "Delete profile",
        key=f"emb_delete_{profile_id}",
        disabled=not delete_confirm,
    ):
        if db.delete_embed_profile(conn, profile_id):
            st.session_state.pop("emb_edit_profile_id", None)
            st.session_state.pop(f"emb_gpu_info_{profile_id}", None)
            st.success(f"Deleted profile {profile_id}.")
            _app_rerun()
        else:
            st.error("Profile not found.")


def _run_embedding(conn, profile: db.EmbedProfile, bundle: claims_data.ClaimsBundle) -> None:
    artifact_dir = db.artifacts_root() / f"profile_{profile.id}" / f"run_{bundle.source_hash[:12]}"
    with st.status(f"Embedding {bundle.claim_count} claims with {profile.model_id}…", expanded=True) as status:
        progress = st.progress(0.0)
        device_logged = False

        def on_progress(done: int, total: int, msg: str) -> None:
            nonlocal device_logged
            progress.progress(min(1.0, done / max(total, 1)), text=msg if done else "Loading encoder…")
            if not device_logged and done == 0:
                status.write(msg)
                status.update(label=msg)
                device_logged = True
            elif done > 0:
                status.write(msg)

        try:
            metrics = embed_runner.run_embedding(
                profile=profile,
                groups=bundle.groups,
                source_hash=bundle.source_hash,
                source_path=bundle.source_path,
                source_claim_count=bundle.source_claim_count,
                artifact_dir=artifact_dir,
                on_progress=on_progress,
            )
            db.upsert_embed_run(
                conn,
                profile_id=profile.id,
                source_hash=bundle.source_hash,
                source_path=bundle.source_path,
                claim_count=int(metrics["claim_count"]),
                source_claim_count=int(metrics.get("source_claim_count") or bundle.source_claim_count),
                vector_dim=int(metrics["vector_dim"]),
                dtype="float32",
                artifact_dir=str(artifact_dir),
                metrics=metrics,
            )
            progress.empty()
            status.update(label="Embedding complete", state="complete")
            st.session_state["flash_embed_run"] = metrics
            _app_rerun()
        except Exception as exc:  # noqa: BLE001 - surface any failure to the UI
            progress.empty()
            status.update(label="Failed", state="error")
            st.error(f"Embedding failed: {exc}")


# --- Explore tab ---


@st.fragment
def _render_explore_tab() -> None:
    conn = _open_db()
    try:
        _render_explore_tab_content(conn)
    finally:
        conn.close()


def _render_explore_tab_content(conn) -> None:
    runs = db.list_embed_runs(conn)
    if not runs:
        st.info("No embedding runs yet. Create a profile and run embedding first.")
        return

    run_options = {_run_label(r): r["id"] for r in runs}
    keys = list(run_options.keys())
    _ensure_selectbox_key("emb_explore_run", keys)
    st.selectbox("Embedding run", options=keys, key="emb_explore_run")
    run_id = run_options[st.session_state["emb_explore_run"]]
    run = db.get_embed_run(conn, run_id)
    if run is None:
        st.error("Run not found.")
        return
    profile = db.get_embed_profile(conn, int(run["profile_id"]))

    try:
        vectors, index = _load_run_arrays(str(run["artifact_dir"]))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not load run artifacts: {exc}")
        return

    groups = index.get("groups") or []
    claim_texts = index.get("claim_texts") or [g.get("claim_text", "") for g in groups]

    st.subheader("Search")
    query = st.text_input("Search phrase", key="emb_search_q", placeholder="e.g. the covid vaccine causes heart problems")
    top_k = st.number_input("Top K", min_value=1, max_value=100, value=10, key="emb_search_k")
    if query.strip():
        if profile is None:
            st.error("Run's profile is missing; cannot embed the query.")
        else:
            q = embed_runner.embed_query(profile.model_id, query.strip(), query_instruction=profile.query_instruction)
            scores = vectors @ q
            k = int(min(top_k, len(scores)))
            top = np.argsort(-scores)[:k]
            bundle, _ = _load_claims()
            posts = bundle.posts_by_task_id if bundle else {}
            for rank, i in enumerate(top, start=1):
                i = int(i)
                group = groups[i] if i < len(groups) else {}
                count = int(group.get("count", 1))
                with st.container(border=True):
                    occ = f" \u00b7 x{count}" if count > 1 else ""
                    st.markdown(f"**{rank}. score={float(scores[i]):.3f}**{occ}")
                    st.markdown(claim_texts[i] if i < len(claim_texts) else "(claim text unavailable)")
                    if group:
                        _render_group_sources(group, posts)

    st.divider()
    st.subheader("Graph")
    cluster_runs = db.list_cluster_runs_for_embed(conn, run_id)
    color_options = ["(none)"] + [f"{cr['id']}: {cr['cluster_profile_name']}" for cr in cluster_runs]
    cg1, cg2, cg3 = st.columns(3)
    with cg1:
        method = st.selectbox("Projection", options=list(projection.PROJECTION_METHODS), key="emb_proj_method")
    with cg2:
        color_sel = st.selectbox("Color by cluster", options=color_options, key="emb_color_sel")
    with cg3:
        max_points = st.number_input("Max points", min_value=200, max_value=20000, value=3000, step=200, key="emb_maxpts")

    labels_full: np.ndarray | None = None
    cluster_run: dict | None = None
    cluster_names: dict[int, dict] = {}
    if color_sel != "(none)":
        cr_id = int(color_sel.split(":", 1)[0])
        cluster_run = next((c for c in cluster_runs if c["id"] == cr_id), None)
        if cluster_run is not None:
            try:
                labels_full = np.load(Path(cluster_run["labels_path"]))
            except Exception:  # noqa: BLE001
                labels_full = None
            cluster_names = db.list_cluster_names(conn, cr_id)

    if st.button("Render graph", key="emb_render_graph"):
        st.session_state["emb_graph_ready"] = True

    if st.session_state.get("emb_graph_ready"):
        with st.spinner("Projecting…"):
            proj = projection.project_2d(vectors, method=method, tsne_max_points=int(max_points))
            idx = proj.indices
            coords = proj.coords
            if len(idx) > int(max_points):
                rng = np.random.default_rng(0)
                sel = np.sort(rng.choice(len(idx), size=int(max_points), replace=False))
                idx = idx[sel]
                coords = coords[sel]

        rows = []
        for j, row_i in enumerate(idx.tolist()):
            cluster_id = int(labels_full[row_i]) if labels_full is not None and row_i < len(labels_full) else -999
            if cluster_id == -999:
                cluster_label = "all"
            else:
                cluster_label = _cluster_display_name(cluster_id, cluster_names)
            rows.append(
                {
                    "x": float(coords[j, 0]),
                    "y": float(coords[j, 1]),
                    "cluster": cluster_label,
                    "claim": _truncate(claim_texts[row_i] if row_i < len(claim_texts) else ""),
                }
            )
        df = pd.DataFrame(rows)
        if proj.subsampled:
            st.caption(f"t-SNE on a {len(idx)}-point subsample.")
        chart = (
            alt.Chart(df)
            .mark_circle(size=45, opacity=0.6)
            .encode(
                x=alt.X("x:Q", title=None),
                y=alt.Y("y:Q", title=None),
                color=alt.Color("cluster:N", legend=alt.Legend(title="cluster")),
                tooltip=["cluster", "claim"],
            )
            .interactive()
            .properties(height=520)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.caption("Choose options and click Render graph.")

    if labels_full is not None and cluster_run is not None:
        st.divider()
        _render_cluster_browser(
            conn,
            cluster_run=cluster_run,
            cluster_names=cluster_names,
            labels=labels_full,
            groups=groups,
            claim_texts=claim_texts,
        )


def _render_cluster_browser(
    conn,
    *,
    cluster_run: dict,
    cluster_names: dict[int, dict],
    labels: np.ndarray,
    groups: list[dict],
    claim_texts: list[str],
) -> None:
    st.subheader("Browse clusters")
    cluster_run_id = int(cluster_run["id"])

    if st.button("Generate / refresh cluster names", key=f"emb_gen_names_{cluster_run_id}"):
        with st.spinner("Naming clusters…"):
            cluster_names = _persist_cluster_names(
                conn,
                cluster_run_id=cluster_run_id,
                claim_texts=claim_texts,
                labels=labels,
            )
        _frag_rerun()

    sizes = clustering.cluster_sizes(labels)
    cluster_ids = sorted(sizes.keys(), key=lambda cid: (-sizes[cid], cid))

    def _option_label(cid: int) -> str:
        name = _cluster_display_name(cid, cluster_names)
        return f"{name} ({sizes[cid]})"

    options = {_option_label(cid): cid for cid in cluster_ids}
    if not options:
        st.caption("No clusters in this run.")
        return

    opt_keys = list(options.keys())
    _ensure_selectbox_key(f"emb_cluster_pick_{cluster_run_id}", opt_keys)
    st.selectbox("Cluster", options=opt_keys, key=f"emb_cluster_pick_{cluster_run_id}")
    selected_cid = options[st.session_state[f"emb_cluster_pick_{cluster_run_id}"]]

    current_name = _cluster_display_name(selected_cid, cluster_names)
    rename_col, save_col = st.columns([4, 1])
    with rename_col:
        new_name = st.text_input("Rename cluster", value=current_name, key=f"emb_rename_{cluster_run_id}_{selected_cid}")
    with save_col:
        st.write("")
        if st.button("Save name", key=f"emb_save_name_{cluster_run_id}_{selected_cid}"):
            db.update_cluster_name(
                conn,
                cluster_run_id=cluster_run_id,
                cluster_id=selected_cid,
                name=new_name,
                size=sizes.get(selected_cid, 0),
            )
            _frag_rerun()

    member_idx = np.where(labels == selected_cid)[0]
    rows = []
    for i in member_idx.tolist():
        i = int(i)
        group = groups[i] if i < len(groups) else {}
        rows.append(
            {
                "claim_text": claim_texts[i] if i < len(claim_texts) else "",
                "count": int(group.get("count", 1)),
                "n_sources": len(group.get("sources") or []),
            }
        )
    rows.sort(key=lambda r: (-int(r["count"]), str(r["claim_text"])))
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, height=420)


# --- Clustering Profiles tab ---


@st.fragment
def _render_clustering_tab() -> None:
    conn = _open_db()
    try:
        _render_clustering_tab_content(conn)
    finally:
        conn.close()


def _render_clustering_tab_content(conn) -> None:
    st.caption("Clustering runs on the full-dimensional vectors of a chosen embedding run.")

    new_name = st.text_input("New clustering profile name", key="clu_new_name", placeholder="e.g. kmeans-12")
    new_algo = st.selectbox("Algorithm", options=list(clustering.CLUSTER_ALGORITHMS), key="clu_new_algo")
    if st.button("Create clustering profile", key="clu_create"):
        if not new_name.strip():
            st.error("Name required.")
        else:
            pid = db.create_cluster_profile(
                conn, name=new_name.strip(), algorithm=new_algo, params=clustering.default_params(new_algo)
            )
            st.session_state["clu_edit_id"] = pid
            _frag_rerun()

    profiles = db.list_cluster_profiles(conn)
    if not profiles:
        st.info("Create a clustering profile to begin.")
        return

    options = {f"{p.id}: {p.name} ({p.algorithm})": p.id for p in profiles}
    keys = list(options.keys())
    preferred = next(
        (k for k, v in options.items() if v == st.session_state.get("clu_edit_id", profiles[0].id)), keys[0]
    )
    _ensure_selectbox_key("clu_edit_select", keys, preferred=preferred)
    st.selectbox("Edit clustering profile", options=keys, key="clu_edit_select")
    profile_id = options[st.session_state["clu_edit_select"]]
    st.session_state["clu_edit_id"] = profile_id
    profile = db.get_cluster_profile(conn, profile_id)
    if profile is None:
        st.error("Profile not found.")
        return

    name = st.text_input("Name", value=profile.name, key=f"clu_name_{profile_id}")
    _ensure_selectbox_key(f"clu_algo_{profile_id}", list(clustering.CLUSTER_ALGORITHMS), preferred=profile.algorithm)
    algorithm = st.selectbox(
        "Algorithm", options=list(clustering.CLUSTER_ALGORITHMS), key=f"clu_algo_{profile_id}"
    )
    params = _cluster_param_inputs(algorithm, profile.params, profile_id)

    if st.button("Save clustering profile", key=f"clu_save_{profile_id}"):
        db.update_cluster_profile(conn, profile_id, name=name, algorithm=algorithm, params=params)
        st.success("Saved.")
        _frag_rerun()

    st.divider()
    runs = db.list_embed_runs(conn)
    if not runs:
        st.info("No embedding runs to cluster yet.")
        return
    run_options = {_run_label(r): r["id"] for r in runs}
    rkeys = list(run_options.keys())
    _ensure_selectbox_key("clu_target_run", rkeys)
    st.selectbox("Embedding run to cluster", options=rkeys, key="clu_target_run")
    run_id = run_options[st.session_state["clu_target_run"]]

    if st.button("Run clustering", key=f"clu_run_{profile_id}", type="primary"):
        _run_clustering(conn, profile_id, algorithm, params, run_id)

    st.divider()
    st.subheader("Cluster runs for selected embedding run")
    for cr in db.list_cluster_runs_for_embed(conn, run_id):
        st.markdown(
            f"- **{cr['cluster_profile_name']}** ({cr['algorithm']}) \u00b7 "
            f"clusters={cr['n_clusters']} \u00b7 noise={cr.get('n_noise')} \u00b7 {cr['created_at']}"
        )


def _cluster_param_inputs(algorithm: str, current: dict, profile_id: int) -> dict:
    defaults = clustering.default_params(algorithm)
    merged = {**defaults, **(current or {})}
    if algorithm in ("kmeans", "agglomerative"):
        n = st.number_input(
            "n_clusters", min_value=2, max_value=200, value=int(merged.get("n_clusters", 12)),
            key=f"clu_k_{profile_id}_{algorithm}",
        )
        return {"n_clusters": int(n)}
    if algorithm == "dbscan":
        eps = st.number_input(
            "eps (cosine distance)", min_value=0.01, max_value=1.0, value=float(merged.get("eps", 0.35)),
            step=0.01, key=f"clu_eps_{profile_id}",
        )
        ms = st.number_input(
            "min_samples", min_value=1, max_value=100, value=int(merged.get("min_samples", 5)),
            key=f"clu_ms_{profile_id}",
        )
        return {"eps": float(eps), "min_samples": int(ms)}
    return {}


def _run_clustering(conn, cluster_profile_id: int, algorithm: str, params: dict, run_id: int) -> None:
    run = db.get_embed_run(conn, run_id)
    if run is None:
        st.error("Embedding run not found.")
        return
    try:
        vectors, index = _load_run_arrays(str(run["artifact_dir"]))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not load run artifacts: {exc}")
        return
    claim_texts = index.get("claim_texts") or []
    with st.spinner(f"Clustering {vectors.shape[0]} vectors with {algorithm}…"):
        try:
            result = clustering.run_clustering(vectors, algorithm=algorithm, params=params)
        except ValueError as exc:
            st.error(str(exc))
            return
        labels_path = Path(run["artifact_dir"]) / embed_runner.LABELS_FILE_TMPL.format(
            cluster_profile_id=cluster_profile_id
        )
        np.save(labels_path, result.labels)
        cluster_run_id = db.upsert_cluster_run(
            conn,
            embed_run_id=run_id,
            cluster_profile_id=cluster_profile_id,
            labels_path=str(labels_path),
            n_clusters=result.n_clusters,
            n_noise=result.n_noise,
        )
        _persist_cluster_names(
            conn,
            cluster_run_id=cluster_run_id,
            claim_texts=claim_texts,
            labels=result.labels,
        )
    st.success(f"Found {result.n_clusters} clusters ({result.n_noise} noise points).")
    _app_rerun()


# --- Eval Triplets tab ---


@st.fragment
def _render_triplets_tab() -> None:
    conn = _open_db()
    try:
        _render_triplets_tab_content(conn)
    finally:
        conn.close()


def _render_triplets_tab_content(conn) -> None:
    st.caption(
        "One global, editable triplet set (anchor, positive=should-be-close, "
        "negative=should-be-far). Scores are per embedding profile; re-run after editing."
    )

    triplets = db.list_triplets(conn)
    base_df = pd.DataFrame(
        [{"anchor": t.anchor, "positive": t.positive, "negative": t.negative} for t in triplets]
        or [{"anchor": "", "positive": "", "negative": ""}]
    )
    edited = st.data_editor(
        base_df,
        num_rows="dynamic",
        use_container_width=True,
        key="trip_editor",
        column_config={
            "anchor": st.column_config.TextColumn("anchor", width="large"),
            "positive": st.column_config.TextColumn("positive (close)", width="large"),
            "negative": st.column_config.TextColumn("negative (far)", width="large"),
        },
    )
    if st.button("Save triplet set", key="trip_save"):
        rows: list[tuple[str, str, str]] = []
        for _, r in edited.iterrows():
            a = str(r.get("anchor") or "").strip()
            p = str(r.get("positive") or "").strip()
            n = str(r.get("negative") or "").strip()
            if a and p and n:
                rows.append((a, p, n))
        db.replace_triplets(conn, rows)
        st.success(f"Saved {len(rows)} triplet(s).")
        _frag_rerun()

    st.divider()
    runs = db.list_embed_runs(conn)
    if not runs:
        st.info("No embedding runs yet; create a profile and embed first.")
        return
    run_options = {_run_label(r): r["id"] for r in runs}
    keys = list(run_options.keys())
    _ensure_selectbox_key("trip_run", keys)
    st.selectbox("Score against embedding run (uses its profile)", options=keys, key="trip_run")
    run_id = run_options[st.session_state["trip_run"]]
    run = db.get_embed_run(conn, run_id)
    profile = db.get_embed_profile(conn, int(run["profile_id"])) if run else None

    if st.button("Score triplets", key="trip_score", type="primary"):
        saved = db.list_triplets(conn)
        if not saved:
            st.warning("Save at least one triplet first.")
        elif profile is None:
            st.error("Run's profile missing.")
        else:
            with st.spinner("Embedding triplets and scoring…"):
                result = score_triplets(saved, profile=profile)
                db.upsert_triplet_result(
                    conn,
                    embed_run_id=run_id,
                    accuracy=result.accuracy,
                    mean_margin=result.mean_margin,
                    triplet_count=result.triplet_count,
                    per_triplet=result.per_triplet,
                )
            _frag_rerun()

    res = db.get_triplet_result(conn, run_id)
    if res:
        c1, c2, c3 = st.columns(3)
        c1.metric("Triplet accuracy", f"{(res.get('accuracy') or 0) * 100:.0f}%")
        c2.metric("Mean margin", f"{res.get('mean_margin') or 0:.3f}")
        c3.metric("Triplets", res.get("triplet_count") or 0)
        per = sorted(res.get("per_triplet", []), key=lambda d: d.get("margin", 0))
        st.markdown("**Smallest-margin triplets (hardest)**")
        st.dataframe(
            pd.DataFrame(per),
            use_container_width=True,
            hide_index=True,
        )


def main() -> None:
    st.set_page_config(page_title="Claim embedding lab", layout="wide")
    st.title("Claim embedding lab")

    st.session_state.setdefault("emb_db_in", str(db.default_db_path()))
    st.session_state.setdefault("emb_source_in", DEFAULT_SOURCE)

    tab_source, tab_profiles, tab_explore, tab_cluster, tab_triplets = st.tabs(
        ["Source Data", "Embedding Profiles", "Explore", "Clustering Profiles", "Eval Triplets"]
    )
    with tab_source:
        _render_source_tab()
    with tab_profiles:
        _render_profiles_tab()
    with tab_explore:
        _render_explore_tab()
    with tab_cluster:
        _render_clustering_tab()
    with tab_triplets:
        _render_triplets_tab()


if __name__ == "__main__":
    main()
