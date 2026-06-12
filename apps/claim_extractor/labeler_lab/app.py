"""
Streamlit lab: generic Ridge heads (name + ordered input variables), labeling, train, score, preview inference.

From the **repository root** (so ``apps`` is importable), run::

  streamlit run apps/claim_extractor/labeler_lab/app.py

The app prepends the repo root to ``sys.path`` if ``apps`` is not found (e.g. some Streamlit cwd setups).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd

# Repo root = parent of ``apps/`` (labeler_lab -> claim_extractor -> apps -> wmvi)
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import streamlit as st

from apps.claim_extractor.labeler_lab import claims_data, db, eval_metrics, field_inputs, splits, standard_heads, var_registry
from apps.claim_extractor.learned.constants import REPO_ROOT
from apps.claim_extractor.learned.predict import FieldPredictor, clear_encoder_cache
from apps.claim_extractor.learned.train import resolve_out_dir, run_train_from_pairs
from apps.claim_extractor.model_common import MANUAL_SCORE_FIELDS, parse_score_01
from apps.claim_extractor.scoring_inputs import context_text_for_post_row

_PREVIEW_LONG_CHARS = 1000
_PREVIEW_LONG_LINES = 12
_PREVIEW_MAX_HEIGHT_PX = 360


def _slug(name: str) -> str:
    x = re.sub(r"[^a-zA-Z0-9_-]+", "-", name.strip().lower()).strip("-")
    return (x[:80] if x else "head")


def _field_description(score_field_name: str | None) -> str | None:
    if not score_field_name:
        return None
    for key, desc in MANUAL_SCORE_FIELDS:
        if key == score_field_name:
            return desc
    return None


def _build_head_input(head: db.RidgeHead, post_row: dict, claim_dict: dict, *, tid: str, idx: int) -> str:
    return field_inputs.build_input_for_head(
        score_field_name=head.score_field_name,
        input_var_keys=head.input_var_keys,
        post_row=post_row,
        claim_dict=claim_dict,
        claim_index=idx,
        task_id=tid,
    )


def _use_claim_dedup(head: db.RidgeHead) -> bool:
    if head.score_field_name != "claim_vaccine_alignment_score":
        return False
    return bool(st.session_state.get("dedupe_claim_text", True))


def _labeled_keys(conn, head_id: int) -> set[tuple[str, int]]:
    return {
        (r["task_id"], r["claim_index"])
        for r in db.fetch_labels_sorted(conn, head_id, split=None)
    }


def _text_area_height(text: str, *, min_h: int = 68, max_h: int = 360, px_per_line: int = 20) -> int:
    """Pixel height for long preview text (scrollable); short text uses height='content'."""
    raw_lines = text.splitlines() or [""]
    visual_lines = sum(max(1, (len(line) + 89) // 90) for line in raw_lines)
    return min(max_h, max(min_h, 16 + visual_lines * px_per_line))


def _preview_is_long(text: str) -> bool:
    return len(text) > _PREVIEW_LONG_CHARS or (text.count("\n") + 1) > _PREVIEW_LONG_LINES


def _preview_text_area(title: str, text: str, *, key: str) -> None:
    """Read-only preview: shrink to content when short; cap height + scroll when long."""
    st.markdown(f"**{title}**")
    if not text.strip():
        st.caption("(empty)")
        return
    if _preview_is_long(text):
        st.text_area(
            title,
            value=text,
            height=_text_area_height(text, max_h=_PREVIEW_MAX_HEIGHT_PX),
            disabled=True,
            key=key,
            label_visibility="collapsed",
        )
    else:
        st.text_area(
            title,
            value=text,
            height="content",
            disabled=True,
            key=key,
            label_visibility="collapsed",
        )


def _render_standard_field_labeling_context(
    head: db.RidgeHead,
    post_row: dict,
    claim_dict: dict,
    *,
    tid: str,
    idx: int,
) -> None:
    claim_text = str(claim_dict.get("claim") or "")
    _preview_text_area("Claim", claim_text, key=f"lbl_claim_{tid}_{idx}")
    if head.score_field_name != "claim_vaccine_alignment_score":
        ctx = context_text_for_post_row(post_row)
        _preview_text_area("Post context", ctx, key=f"lbl_ctx_{tid}_{idx}")
        if not ctx.strip():
            st.warning("Post text is missing on this row — check that posts JSON includes `text` or `text_coreference_resolved`.")
    with st.expander("Model input (training format)", expanded=False):
        model_input = _build_head_input(head, post_row, claim_dict, tid=tid, idx=idx)
        if head.score_field_name == "claim_vaccine_alignment_score":
            st.caption("Training encodes the claim only (`[CLAIM]` block).")
        else:
            st.caption(
                "Training encodes **claim and post context together** as one string "
                "with `[CLAIM]` and `[TEXT]` markers (shown below)."
            )
        st.code(model_input, language=None)


@st.cache_data(show_spinner=True)
def _load_posts(path_str: str) -> tuple[list, str | None]:
    """
    Load posts list from claims JSON. Returns (posts, error_message).
    ``error_message`` is set on missing path, I/O errors, invalid JSON, or invalid top-level shape.
    """
    if not path_str.strip():
        return [], "Posts path is empty."
    raw = path_str.strip()
    candidates = [Path(raw).expanduser()]
    p0 = candidates[0]
    if not p0.is_file() and not p0.is_absolute():
        candidates.append(REPO_ROOT / raw.lstrip("/"))

    p: Path | None = None
    for c in candidates:
        if c.is_file():
            p = c
            break
    if p is None:
        tried = ", ".join(str(c) for c in candidates)
        return [], f"No file found. Tried: {tried}"

    try:
        text = p.read_text(encoding="utf-8")
    except OSError as e:
        return [], f"Could not read {p}: {e}"

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as e:
        return [], (
            f"Invalid JSON in {p}: {e}\n\n"
            "Common causes: truncated download, hand-edited file with trailing commas, or wrong file. "
            "Re-export from the pipeline or validate with `python -m json.tool < file`."
        )

    if not isinstance(payload, dict):
        return [], "JSON root must be an object with a `posts` array."
    posts = payload.get("posts")
    if not isinstance(posts, list):
        return [], "Expected top-level key `posts` to be a JSON array."

    cleaned = [x for x in posts if isinstance(x, dict)]
    return cleaned, None


def _open_db(path: Path):
    conn = db.connect(path)
    db.init_schema(conn)
    return conn


def _queue_platform(post_row: dict) -> str:
    platform = post_row.get("platform")
    return str(platform) if platform else "(unknown)"


def _label_queue_platforms(queue: list) -> list[str]:
    return sorted({_queue_platform(item[0]) for item in queue})


def _filter_label_queue_by_platforms(queue: list, enabled: set[str]) -> list:
    if not enabled:
        return []
    return [item for item in queue if _queue_platform(item[0]) in enabled]


def _render_label_platform_filter(head_id: int, platforms: list[str]) -> set[str]:
    """Popover with per-platform checkboxes; all enabled by default."""
    label = "Platform filter"
    if platforms:
        label = f"Platform filter ({len(platforms)} sources)"
    with st.popover(label):
        st.caption("Uncheck platforms to exclude them from the labeling queue.")
        if not platforms:
            st.info("No platforms in the current queue.")
            return set()
        enabled: set[str] = set()
        for platform in platforms:
            if st.checkbox(platform, value=True, key=f"label_plat_{head_id}_{platform}"):
                enabled.add(platform)
        return enabled


def _claim_excerpt(claim_dict: dict, *, max_len: int = 120) -> str:
    text = str(claim_dict.get("claim") or "")
    if len(text) > max_len:
        return text[:max_len] + "…"
    return text


def _render_problem_claims(conn) -> None:
    st.caption(
        "Global pool of claims flagged during labeling. Post and claim metadata are snapshotted at flag time."
    )
    rows = db.fetch_problem_claims_sorted(conn, descending=True)
    st.write(f"**{len(rows)}** problem claim(s)")
    if not rows:
        st.info("No problem claims yet. Use **Add to problem claims** on the Manual label tab.")
        return

    table_rows: list[dict] = []
    for row in rows:
        claim_dict = row["claim_dict"]
        post_row = row["post_row"]
        table_rows.append(
            {
                "note": row["note"],
                "task_id": row["task_id"],
                "claim_index": row["claim_index"],
                "claim_excerpt": _claim_excerpt(claim_dict),
                "platform": str(post_row.get("platform") or ""),
                "flagged_from_head": row["flagged_from_head"],
                "created_at": row.get("created_at") or "",
                "delete": False,
            }
        )
    orig_notes = {(r["task_id"], r["claim_index"]): r["note"] for r in table_rows}

    df = pd.DataFrame(table_rows)
    col_config: dict = {
        "note": st.column_config.TextColumn("note", help="What is wrong with this claim?"),
        "task_id": st.column_config.TextColumn("task_id", disabled=True),
        "claim_index": st.column_config.NumberColumn("claim_index", disabled=True, format="%d"),
        "claim_excerpt": st.column_config.TextColumn("claim_excerpt", disabled=True),
        "platform": st.column_config.TextColumn("platform", disabled=True),
        "flagged_from_head": st.column_config.TextColumn("flagged_from_head", disabled=True),
        "created_at": st.column_config.TextColumn("created_at", disabled=True),
        "delete": st.column_config.CheckboxColumn("delete", help="Remove when you save"),
    }
    disabled_cols = [
        "task_id",
        "claim_index",
        "claim_excerpt",
        "platform",
        "flagged_from_head",
        "created_at",
    ]
    edited = st.data_editor(
        df,
        column_config=col_config,
        disabled=disabled_cols,
        hide_index=True,
        width="stretch",
        key="problem_claims_editor",
    )

    if st.button("Save changes", key="problem_claims_save"):
        n_updated = 0
        n_deleted = 0
        for _, erow in edited.iterrows():
            tid = str(erow["task_id"])
            cidx = int(erow["claim_index"])
            if bool(erow.get("delete")):
                if db.delete_problem_claim(conn, tid, cidx):
                    n_deleted += 1
                continue
            note_val = str(erow.get("note") or "")
            if orig_notes.get((tid, cidx)) != note_val:
                db.update_problem_claim_note(conn, task_id=tid, claim_index=cidx, note=note_val)
                n_updated += 1
        if n_updated or n_deleted:
            st.success(f"Saved: {n_updated} updated, {n_deleted} deleted.")
            st.rerun()
        else:
            st.info("No changes to save.")

    st.divider()
    st.subheader("Inspect snapshot")
    picker_labels = [
        f"{r['task_id']}:{r['claim_index']} — {_claim_excerpt(r['claim_dict'], max_len=60)}"
        for r in rows
    ]
    sel_idx = st.selectbox(
        "Row",
        options=list(range(len(rows))),
        format_func=lambda i: picker_labels[i],
        key="problem_claim_inspect",
    )
    selected = rows[sel_idx]
    post_row = selected["post_row"]
    claim_dict = selected["claim_dict"]
    _preview_text_area(
        "Claim",
        str(claim_dict.get("claim") or ""),
        key=f"prob_claim_{selected['task_id']}_{selected['claim_index']}",
    )
    _preview_text_area(
        "Post context",
        context_text_for_post_row(post_row),
        key=f"prob_ctx_{selected['task_id']}_{selected['claim_index']}",
    )
    with st.expander("Full post snapshot (JSON)", expanded=False):
        st.json(post_row)
    with st.expander("Full claim snapshot (JSON)", expanded=False):
        st.json(claim_dict)


def _render_metric_block(title: str, metrics: dict, *, help_prefix: str = "") -> None:
    st.markdown(f"**{title}**")
    n = metrics.get("n", 0)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("n", n, help=eval_metrics.METRIC_HELP["n"])
    with c2:
        st.metric(
            "MAE",
            eval_metrics.format_metric(metrics.get("mae")),
            help=eval_metrics.METRIC_HELP["mae"],
        )
    with c3:
        st.metric(
            "RMSE",
            eval_metrics.format_metric(metrics.get("rmse")),
            help=eval_metrics.METRIC_HELP["rmse"],
        )
    with c4:
        st.metric(
            "Pearson r",
            eval_metrics.format_metric(metrics.get("pearson")),
            help=eval_metrics.METRIC_HELP["pearson"],
        )
    if help_prefix:
        st.caption(help_prefix)


def _render_eval_score_results(
    cmp: dict,
    *,
    eval_rows: list,
    score_key: str | None,
    n_eval: int,
) -> None:
    ridge = cmp["ridge_vs_manual"]
    llm = cmp["llm_vs_manual"]

    with st.expander("What do these metrics mean?", expanded=False):
        st.markdown(eval_metrics.METRIC_HELP["mae"])
        st.markdown(eval_metrics.METRIC_HELP["rmse"])
        st.markdown(eval_metrics.METRIC_HELP["pearson"])
        st.markdown(eval_metrics.BEATS_LLM_HELP)
        st.caption(
            "Gold standard is **your manual label** on the eval split. "
            "LLM scores come from the posts JSON and are never used to train Ridge."
        )

    col_a, col_b = st.columns(2)
    with col_a:
        _render_metric_block("Ridge vs manual gold", ridge)
    with col_b:
        llm_note = ""
        if cmp["n_llm_invalid"]:
            llm_note = f"{cmp['n_llm_invalid']} eval row(s) skipped (missing/invalid LLM score)."
        _render_metric_block("LLM vs manual gold (benchmark)", llm, help_prefix=llm_note)

    beats = cmp.get("beats_llm")
    if beats is True:
        st.success(f"Ridge beats LLM on this eval split. {eval_metrics.BEATS_LLM_HELP}")
    elif beats is False:
        ridge_mae = ridge.get("mae")
        llm_mae = llm.get("mae")
        ridge_r = ridge.get("pearson")
        llm_r = llm.get("pearson")
        reasons: list[str] = []
        if ridge_mae is not None and llm_mae is not None and float(ridge_mae) >= float(llm_mae):
            reasons.append(
                f"Ridge MAE ({eval_metrics.format_metric(ridge_mae)}) "
                f"≥ LLM MAE ({eval_metrics.format_metric(llm_mae)})"
            )
        if ridge_r is not None and llm_r is not None and float(ridge_r) < float(llm_r):
            reasons.append(
                f"Ridge Pearson ({eval_metrics.format_metric(ridge_r)}) "
                f"< LLM Pearson ({eval_metrics.format_metric(llm_r)})"
            )
        detail = " · ".join(reasons) if reasons else "See metrics above."
        st.warning(f"Ridge does not yet beat LLM on this eval split. {detail}")
    elif score_key and n_eval >= 10:
        st.info("Need ≥10 valid LLM scores on eval rows to compute beats-LLM flag.")

    st.divider()
    st.subheader("Visual comparison")

    err_rows = eval_metrics.metrics_comparison_rows(ridge, llm)
    if err_rows:
        st.markdown("**Error metrics (lower is better)**")
        err_df = pd.DataFrame(err_rows)
        st.bar_chart(err_df, x="metric", y="value", color="model", stack=False)
        st.caption("Side-by-side MAE and RMSE for Ridge and LLM against your manual eval labels.")

    pearson_rows = eval_metrics.pearson_comparison_rows(ridge, llm)
    if pearson_rows:
        st.markdown("**Pearson correlation (higher is better)**")
        pearson_df = pd.DataFrame(pearson_rows).set_index("model")
        st.bar_chart(pearson_df)

    per_row = cmp.get("per_row") or []
    if per_row:
        ridge_pts, llm_pts = eval_metrics.scatter_rows_per_row(per_row)
        st.markdown("**Predicted vs manual label**")
        st.caption("Ideal predictions follow the diagonal (predicted = manual).")
        sc1, sc2 = st.columns(2)
        with sc1:
            st.markdown("*Ridge*")
            if ridge_pts:
                st.scatter_chart(pd.DataFrame(ridge_pts), x="manual", y="predicted")
            else:
                st.caption("No points.")
        with sc2:
            st.markdown("*LLM*")
            if llm_pts:
                st.scatter_chart(pd.DataFrame(llm_pts), x="manual", y="predicted")
            else:
                st.caption("No valid LLM scores on eval rows.")

        hist_rows = eval_metrics.abs_error_histogram_rows(per_row)
        if hist_rows:
            st.markdown("**Absolute error distribution**")
            hist_df = pd.DataFrame(hist_rows)
            st.bar_chart(hist_df, x="error_bin", y="count", color="model", stack=False)
            st.caption("How often each model is off by a given amount (|prediction − manual|).")

        st.markdown("**Per-row comparison**")
        st.dataframe(
            [
                {
                    "task_id": eval_rows[r["index"]][0],
                    "claim_index": eval_rows[r["index"]][1],
                    "y_manual": r["y_manual"],
                    "y_ridge": r["y_ridge"],
                    "y_llm": r["y_llm"],
                    "ridge_abs_err": r["ridge_abs_err"],
                    "llm_abs_err": r["llm_abs_err"],
                }
                for r in per_row
            ],
            width="stretch",
        )


def main() -> None:
    st.set_page_config(page_title="Ridge labeler lab", layout="wide")
    st.title("Ridge labeler lab")

    with st.sidebar:
        st.header("Settings")
        default_db = str(db.default_db_path())
        db_path = st.text_input("SQLite path", value=st.session_state.get("db_path", default_db), key="db_path_in")
        st.session_state["db_path"] = db_path
        posts_default = str(REPO_ROOT / "data" / "posts_with_claims_full.json")
        posts_path = st.text_input("Default posts JSON", value=st.session_state.get("posts_path", posts_default))
        st.session_state["posts_path"] = posts_path
        eval_frac = st.slider("Eval split fraction (for new labels)", 0.05, 0.5, 0.2, 0.05)
        st.session_state["eval_frac"] = eval_frac
        split_seed = st.number_input("Random seed (train/eval split)", value=42, step=1)
        st.session_state["split_seed"] = int(split_seed)
        if st.button("Clear encoder cache"):
            clear_encoder_cache()
            st.success("Encoder cache cleared.")
        if st.button("Clear posts file cache"):
            _load_posts.clear()
            st.success("Posts cache cleared. Reload the page or switch tabs after fixing JSON.")

    conn = _open_db(Path(db_path))
    posts, posts_err = _load_posts(posts_path)

    root_labeling, root_problems = st.tabs(["Ridge labeling", "Problem claims"])

    with root_problems:
        _render_problem_claims(conn)

    with root_labeling:
        st.subheader("Ridge heads")
        heads = db.list_heads(conn)
        head_options = {f"{h.id}: {h.name}": h.id for h in heads}
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Create head**")
            new_name = st.text_input("Name", key="new_head_name", placeholder="e.g. author_agreement_v1")
            st.caption("Variable keys — one per line, in order:")
            keys_raw = st.text_area(
                "Input variables",
                value="claim\ntext_coreference_resolved",
                height=120,
                key="new_head_keys",
                label_visibility="collapsed",
            )
            if st.button("Create head"):
                keys = [ln.strip() for ln in keys_raw.splitlines() if ln.strip()]
                bad = [k for k in keys if k not in var_registry.VAR_EXTRACTORS]
                if not new_name.strip():
                    st.error("Name required.")
                elif bad:
                    st.error(f"Unknown keys: {bad}. Allowed: {var_registry.list_var_keys()}")
                elif not keys:
                    st.error("At least one variable key required.")
                else:
                    try:
                        hid = db.create_head(conn, new_name.strip(), keys)
                        st.success(f"Created head id={hid}")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

        with c2:
            st.markdown("**Standard score-field heads**")
            st.caption("One Ridge head per SCORE_FIELD_NAMES with canonical [CLAIM]/[TEXT] templates.")
            if st.button("Create standard heads"):
                created, skipped = standard_heads.create_standard_heads(conn)
                if created:
                    st.success(f"Created {len(created)} head(s): {[f for f, _ in created]}")
                if skipped:
                    st.info(f"Already existed: {skipped}")
                if not created and not skipped:
                    st.warning("Nothing to create.")
                if created:
                    st.rerun()
            st.markdown("**Variable bank** (display names)")
            for k in var_registry.list_var_keys():
                st.text(f"{k}: {var_registry.display_name(k)}")

        if not heads:
            st.info("Create a head to enable workspace tabs.")
        else:
            sel = st.selectbox("Open head", options=list(head_options.keys()))
            head_id = head_options[sel]
            head = db.get_head(conn, head_id)
            if head is None:
                st.error("Head not found.")
            else:
                _render_ridge_head_workspace(
                    conn,
                    head_id=head_id,
                    head=head,
                    posts_path=posts_path,
                    posts=posts,
                    posts_err=posts_err,
                )

    conn.close()


def _render_ridge_head_workspace(
    conn,
    *,
    head_id: int,
    head: db.RidgeHead,
    posts_path: str,
    posts: list,
    posts_err: str | None,
) -> None:
    if head.score_field_name:
        desc = _field_description(head.score_field_name)
        st.info(f"**Standard field head:** `{head.score_field_name}`" + (f" — {desc}" if desc else ""))

    if head.score_field_name == "claim_vaccine_alignment_score":
        with st.sidebar:
            st.checkbox(
                "Dedupe claim text in labeling queue",
                value=st.session_state.get("dedupe_claim_text", True),
                key="dedupe_claim_text",
                help="Show one row per unique claim string (canonical occurrence). Labels attach to that row only.",
            )

    dedupe_on = _use_claim_dedup(head)

    tab_data, tab_run, tab_label, tab_revise, tab_train, tab_score = st.tabs(
        ["Data source", "Run model", "Manual label", "Review Labels", "Train", "Score (eval)"]
    )

    with tab_data:
        st.write("Posts JSON is loaded from the path below (full file).")
        st.code(posts_path, language=None)
        if posts_err:
            st.error(posts_err)
            st.caption("Use **Clear posts file cache** in the sidebar after fixing the file.")
        elif not posts:
            st.warning("Loaded file has no post dicts in `posts[]`.")
        else:
            n_claims = sum(1 for _ in claims_data.iter_success_claims(posts))
            st.metric("Success-post claims", n_claims)
            if dedupe_on:
                full_dedup = claims_data.dedup_stats(posts)
                d1, d2 = st.columns(2)
                d1.metric("Unique claim texts", full_dedup["unique"])
                d2.metric("Duplicate rows", full_dedup["duplicate_rows"])
                st.caption(
                    "Dedup is on for this head. Labels attach to the **canonical** row per claim text; "
                    "duplicate occurrences are skipped in Manual label."
                )

        st.divider()
        st.subheader("Label summary (current head)")
        n_train = db.count_labels(conn, head_id, "train")
        n_eval = db.count_labels(conn, head_id, "eval")
        n_labeled = n_train + n_eval
        eval_target = 30

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Labeled", n_labeled)
        m2.metric("Train split", n_train)
        m3.metric("Eval split", n_eval)
        m4.metric("Eval target", f"{n_eval}/{eval_target}")

        unlabeled_count: int | None = None
        orphaned = 0
        if not posts_err and posts:
            idx_map = claims_data.index_claims_by_key(posts)
            labeled_keys = _labeled_keys(conn, head_id)
            if dedupe_on:
                all_groups, _ = claims_data.build_claim_dedup_groups(posts)
                labeled_norms = claims_data.labeled_norm_keys(all_groups, labeled_keys)
                unlabeled_count = sum(1 for g in all_groups if g.norm_key not in labeled_norms)
            else:
                unlabeled_count = sum(
                    1
                    for _post, _claim, tid, cidx in claims_data.iter_success_claims(posts)
                    if (tid, cidx) not in labeled_keys
                )
            orphaned = sum(1 for tid, cidx in labeled_keys if (tid, cidx) not in idx_map)

        c_a, c_b, c_c, c_d = st.columns(4)
        with c_a:
            unlabeled_label = "Unlabeled unique" if dedupe_on else "Unlabeled"
            if unlabeled_count is not None:
                st.metric(unlabeled_label, unlabeled_count)
            else:
                st.metric(unlabeled_label, "—")
        with c_b:
            st.metric("Model trained", "Yes" if head.artifact_dir else "No")
        with c_c:
            if orphaned:
                st.metric("Orphaned labels", orphaned, help="Labels whose task_id/claim_index are not in the loaded posts JSON")
            else:
                st.metric("Orphaned labels", 0)
        with c_d:
            st.metric("Problem claims", db.count_problem_claims(conn))

        if n_labeled and not posts_err and posts:
            ys = [r["y"] for r in db.fetch_labels_sorted(conn, head_id, split=None)]
            st.caption(
                f"Score range **{min(ys):.2f}–{max(ys):.2f}** · mean **{sum(ys) / len(ys):.2f}** "
                f"({len(ys)} label(s) for `{head.name}`)"
            )
        elif not n_labeled:
            st.caption("No labels yet — use **Manual label** to start.")

        if n_eval < eval_target:
            st.info(f"Need **{eval_target - n_eval}** more eval label(s) before the eval benchmark is reliable.")
        elif head.artifact_dir:
            st.success("Eval target met for this head. Train if labels changed, then check **Score (eval)**.")

    with tab_run:
        st.caption("Runs BGE + Ridge on all success claims in the loaded file. Does not read LLM score columns from JSON.")
        if not head.artifact_dir:
            st.warning("Train this head first (artifact_dir not set).")
        else:
            art = resolve_out_dir(Path(head.artifact_dir))
            if not art.is_dir():
                st.error(f"Artifact dir missing: {art}")
            else:
                if posts_err:
                    st.error(posts_err)
                elif not posts:
                    st.warning("No posts loaded.")
                else:
                    preview_items = list(claims_data.iter_success_claims(posts))
                    n_preview = len(preview_items)
                    st.metric("Claims to score", n_preview)
                    if n_preview == 0:
                        st.info("No success claims in the loaded posts file.")
                    elif st.button("Run preview", key="run_prev"):
                        rows_out: list[dict] = []
                        batch_size = 32
                        with st.status(
                            f"Running preview on {n_preview} claim(s)…",
                            expanded=True,
                        ) as status:
                            st.write("Loading Ridge artifact and BGE encoder…")
                            pred = FieldPredictor.load(art, batch_size=batch_size)
                            progress = st.progress(0.0, text="Preparing inputs…")
                            prepared: list[tuple[str, str, int]] = []
                            for i, (post_row, claim_dict, tid, idx) in enumerate(preview_items):
                                txt = _build_head_input(head, post_row, claim_dict, tid=tid, idx=idx)
                                prepared.append((txt, tid, idx))
                                if n_preview:
                                    progress.progress(
                                        min(1.0, (i + 1) / n_preview * 0.1),
                                        text=f"Building inputs… {i + 1} / {n_preview}",
                                    )
                            for batch_start in range(0, len(prepared), batch_size):
                                batch = prepared[batch_start : batch_start + batch_size]
                                texts = [item[0] for item in batch]
                                scores = pred.predict_scores(texts)
                                for (txt, tid, idx), yh in zip(batch, scores):
                                    rows_out.append(
                                        {
                                            "task_id": tid,
                                            "claim_index": idx,
                                            "y_hat": yh,
                                            "input_excerpt": txt[:200].replace("\n", " ")
                                            + ("…" if len(txt) > 200 else ""),
                                        }
                                    )
                                done = min(len(prepared), batch_start + len(batch))
                                progress.progress(
                                    0.1 + 0.9 * done / len(prepared),
                                    text=f"Scoring claims… {done} / {n_preview}",
                                )
                            progress.empty()
                            status.update(
                                label=f"Preview complete — scored {n_preview} claim(s)",
                                state="complete",
                            )
                        st.success(f"Scored **{len(rows_out)}** claim(s) with `{head.name}`.")
                        st.dataframe(rows_out, width="stretch")
                        st.session_state[f"last_run_rows_{head_id}"] = rows_out
                    else:
                        last_rows = st.session_state.get(f"last_run_rows_{head_id}")
                        if last_rows:
                            st.caption(
                                f"Last preview: **{len(last_rows)}** row(s). "
                                "Click **Run preview** to refresh."
                            )
                            st.dataframe(last_rows, width="stretch")

    with tab_label:
        st.caption(
            "One variable at a time, then a score in [0, 1]. "
            "Train/eval split is assigned automatically from the sidebar fraction (stable per claim). "
            "Use **Platform filter** to include/exclude sources; **Shuffle labeling queue** mixes order (per Ridge head)."
        )
        if posts_err:
            st.error(posts_err)
        elif not posts:
            st.warning("Load posts JSON first (see Data source tab).")
        else:
            labeled_keys = _labeled_keys(conn, head_id)
            if dedupe_on:
                all_groups, _ = claims_data.build_claim_dedup_groups(posts)
                labeled_norms = claims_data.labeled_norm_keys(all_groups, labeled_keys)
                queue = [
                    (g.canonical_post_row, g.canonical_claim_dict, g.canonical_task_id, g.canonical_claim_index)
                    for g in all_groups
                    if g.norm_key not in labeled_norms
                ]
                st.caption("Dedup on: one queue item per unique claim text (canonical row).")
            else:
                queue = [
                    (post_row, claim_dict, tid, idx)
                    for post_row, claim_dict, tid, idx in claims_data.iter_success_claims(posts)
                    if (tid, idx) not in labeled_keys
                ]
            platforms_in_queue = _label_queue_platforms(queue)
            enabled_platforms = _render_label_platform_filter(head_id, platforms_in_queue)
            queue = _filter_label_queue_by_platforms(queue, enabled_platforms)
            shuffle_queue = st.checkbox(
                "Shuffle labeling queue",
                value=True,
                key=f"shuffle_queue_{head_id}",
                help=(
                    "Mix unlabeled claim order for this head only. "
                    "Uses the sidebar split seed; order is stable for this Ridge head across restarts."
                ),
            )
            if shuffle_queue and len(queue) > 1:
                queue = claims_data.shuffle_label_queue(
                    queue,
                    seed=int(st.session_state.get("split_seed", 42)),
                    head_id=head_id,
                )
            n_excluded = len(platforms_in_queue) - len(enabled_platforms)
            if n_excluded:
                st.caption(
                    f"Showing **{len(queue)}** unlabeled claim(s) "
                    f"({n_excluded} platform(s) excluded)."
                )
            else:
                st.write(f"Unlabeled: **{len(queue)}**")
            if not queue:
                if platforms_in_queue and not enabled_platforms:
                    st.warning("All platforms are excluded. Enable at least one in **Platform filter**.")
                else:
                    st.info("No unlabeled claims match the current filters.")
            elif queue:
                post_row, claim_dict, tid, idx = queue[0]
                st.write(
                    f"`task_id`={tid} · `claim_index`={idx} · `platform`={_queue_platform(post_row)}"
                )
                if head.score_field_name:
                    desc = _field_description(head.score_field_name)
                    if desc:
                        st.markdown(f"**Score semantics:** {desc}")
                    _render_standard_field_labeling_context(
                        head, post_row, claim_dict, tid=tid, idx=idx
                    )
                else:
                    for vk in head.input_var_keys:
                        st.markdown(f"**{var_registry.display_name(vk)}** (`{vk}`)")
                        var_text = var_registry.extract_var(vk, post_row, claim_dict)
                        _preview_text_area(
                            var_registry.display_name(vk),
                            var_text,
                            key=f"lbl_{vk}_{tid}_{idx}",
                        )
                y_in = st.text_input("Score y in [0, 1]", value="0.5", key=f"yval_{tid}_{idx}")
                already_problem = db.is_problem_claim(conn, tid, idx)
                if already_problem:
                    st.caption("Already in problem claims.")
                c_save, c_flag = st.columns(2)
                with c_save:
                    if st.button("Save label", key=f"save_lbl_{tid}_{idx}"):
                        yv, bad = parse_score_01(y_in.strip() if y_in else None)
                        if yv is None or bad:
                            st.error("Enter a number between 0 and 1.")
                        else:
                            ev = float(st.session_state.get("eval_frac", 0.2))
                            sp = splits.assign_label_split(
                                tid, idx, eval_frac=ev, seed=int(st.session_state.get("split_seed", 42))
                            )
                            db.upsert_label(conn, head_id=head_id, task_id=tid, claim_index=idx, y=yv, split=sp)
                            st.success(f"Saved ({sp}).")
                            st.rerun()
                with c_flag:
                    if st.button(
                        "Add to problem claims",
                        key=f"flag_prob_{tid}_{idx}",
                        disabled=already_problem,
                    ):
                        db.upsert_problem_claim(
                            conn,
                            task_id=tid,
                            claim_index=idx,
                            post_row=post_row,
                            claim_dict=claim_dict,
                            head_id=head_id,
                            flagged_from_head=head.name,
                        )
                        st.success("Added to problem claims.")
                        st.rerun()

    with tab_revise:
        st.caption("Edit **label** and **split** directly in the table, then save. Rows marked **delete** are removed on save.")
        split_filter = st.selectbox(
            "Split filter",
            options=["all", "train", "eval"],
            key=f"revise_split_{head_id}",
        )
        sort_desc = st.radio(
            "Sort by label",
            options=["Low → high", "High → low"],
            horizontal=True,
            key=f"revise_sort_{head_id}",
        )
        split_arg = None if split_filter == "all" else split_filter
        labeled = db.fetch_labels_sorted(
            conn, head_id, split_arg, descending=(sort_desc == "High → low")
        )
        st.write(f"**{len(labeled)}** label(s)")
        if not labeled:
            st.info("No labels yet for this head (or filter). Use Manual label first.")
        else:
            idx_map = claims_data.index_claims_by_key(posts) if not posts_err and posts else {}
            _, key_to_group = (
                claims_data.build_claim_dedup_groups(posts) if not posts_err and posts else ([], {})
            )
            table_rows: list[dict] = []
            for row in labeled:
                tid = row["task_id"]
                cidx = row["claim_index"]
                claim_excerpt = ""
                hit = idx_map.get((tid, cidx))
                if hit is not None:
                    _post, claim_dict = hit
                    claim_excerpt = str(claim_dict.get("claim") or "")[:120]
                    if len(str(claim_dict.get("claim") or "")) > 120:
                        claim_excerpt += "…"
                row_out: dict = {
                    "label": row["y"],
                    "split": row["split"],
                    "task_id": tid,
                    "claim_index": cidx,
                    "claim_excerpt": claim_excerpt,
                    "created_at": row.get("created_at") or "",
                    "delete": False,
                }
                if dedupe_on and key_to_group:
                    row_out["occurrences"] = claims_data.occurrence_count_for_key(
                        (tid, cidx), key_to_group
                    )
                table_rows.append(row_out)
            orig_by_key = {
                (r["task_id"], r["claim_index"]): r for r in table_rows
            }
            df = pd.DataFrame(table_rows)
            col_config: dict = {
                "label": st.column_config.NumberColumn(
                    "label",
                    min_value=0.0,
                    max_value=1.0,
                    step=0.01,
                    format="%.2f",
                    help="Manual score in [0, 1]",
                ),
                "split": st.column_config.SelectboxColumn(
                    "split",
                    options=["train", "eval"],
                    required=True,
                ),
                "task_id": st.column_config.TextColumn("task_id", disabled=True),
                "claim_index": st.column_config.NumberColumn(
                    "claim_index", disabled=True, format="%d"
                ),
                "claim_excerpt": st.column_config.TextColumn("claim_excerpt", disabled=True),
                "created_at": st.column_config.TextColumn("created_at", disabled=True),
                "delete": st.column_config.CheckboxColumn(
                    "delete",
                    help="Remove this label when you save",
                ),
            }
            disabled_cols = ["task_id", "claim_index", "claim_excerpt", "created_at"]
            if "occurrences" in df.columns:
                col_config["occurrences"] = st.column_config.NumberColumn(
                    "occurrences",
                    disabled=True,
                    help="Times this claim text appears in the loaded posts file",
                )
                disabled_cols.append("occurrences")

            edited = st.data_editor(
                df,
                column_config=col_config,
                disabled=disabled_cols,
                hide_index=True,
                width="stretch",
                key=f"review_editor_{head_id}_{split_filter}_{sort_desc}",
            )

            if posts_err:
                st.warning(f"Posts JSON not loaded: {posts_err} — claim excerpts may be empty.")
            elif not posts:
                st.warning("Posts JSON empty — claim excerpts unavailable.")

            if st.button("Save changes", key=f"review_save_{head_id}"):
                errors: list[str] = []
                n_updated = 0
                n_deleted = 0
                for _, erow in edited.iterrows():
                    tid = str(erow["task_id"])
                    cidx = int(erow["claim_index"])
                    if bool(erow.get("delete")):
                        if db.delete_label(conn, head_id, tid, cidx):
                            n_deleted += 1
                        continue
                    yv, bad = parse_score_01(erow["label"])
                    if yv is None or bad:
                        errors.append(f"{tid}:{cidx} — label must be in [0, 1]")
                        continue
                    split_val = str(erow["split"])
                    if split_val not in ("train", "eval"):
                        errors.append(f"{tid}:{cidx} — invalid split")
                        continue
                    orig = orig_by_key.get((tid, cidx))
                    if orig is None:
                        continue
                    changed = (
                        abs(float(orig["label"]) - yv) > 1e-9
                        or orig["split"] != split_val
                    )
                    if changed:
                        db.upsert_label(
                            conn,
                            head_id=head_id,
                            task_id=tid,
                            claim_index=cidx,
                            y=yv,
                            split=split_val,
                        )
                        n_updated += 1
                if errors:
                    for msg in errors[:10]:
                        st.error(msg)
                    if len(errors) > 10:
                        st.error(f"…and {len(errors) - 10} more error(s).")
                if n_updated or n_deleted:
                    st.success(f"Saved: {n_updated} updated, {n_deleted} deleted.")
                    st.rerun()
                elif not errors:
                    st.info("No changes to save.")

    with tab_train:
        if posts_err:
            st.error(posts_err)
        val_ratio = st.slider("Internal val split during train", 0.0, 0.4, 0.15, 0.05)
        ridge_alpha = st.number_input("Ridge alpha", value=1.0, min_value=1e-6, format="%f")
        batch_size = st.number_input("Encode batch size", value=32, min_value=1)
        if st.button("Train on train-split labels"):
            labeled = db.fetch_labels_xy(conn, head_id, "train")
            if posts_err:
                st.error(posts_err)
            elif not posts:
                st.error("No posts loaded.")
            elif len(labeled) < 3:
                st.error(f"Need at least 3 train-split labels; have {len(labeled)}.")
            else:
                train_rows = db.fetch_labels_sorted(conn, head_id, "train")
                train_warnings: list[str] = []
                if dedupe_on:
                    texts, ys, train_warnings = claims_data.dedupe_alignment_training_xy(
                        posts,
                        train_rows,
                        input_var_keys=head.input_var_keys,
                        score_field_name=head.score_field_name,
                    )
                    if len(train_rows) > len(texts):
                        st.caption(
                            f"Train dedupe: {len(train_rows)} label row(s) → **{len(texts)}** unique claim text(s)."
                        )
                else:
                    texts, ys = claims_data.build_xy_for_labels(
                        posts,
                        [(r["task_id"], r["claim_index"], r["y"]) for r in train_rows],
                        input_var_keys=head.input_var_keys,
                        score_field_name=head.score_field_name,
                    )
                if len(texts) < 3:
                    st.error("Too few labels matched posts JSON (check task_id / claim_index alignment).")
                else:
                    slug = _slug(head.name)
                    out_dir = resolve_out_dir(REPO_ROOT / "data" / "models" / "ridge_lab" / slug)
                    try:
                        for w in train_warnings[:5]:
                            st.warning(w)
                        metrics = run_train_from_pairs(
                            texts=texts,
                            ys=ys,
                            out_dir=out_dir,
                            head_name=head.name,
                            input_var_keys=head.input_var_keys,
                            val_ratio=val_ratio,
                            seed=int(st.session_state.get("split_seed", 42)),
                            ridge_alpha=float(ridge_alpha),
                            batch_size=int(batch_size),
                        )
                        db.update_head_artifact(conn, head_id, str(out_dir))
                        st.json(metrics)
                        st.success(f"Wrote artifact to {out_dir}")
                    except Exception as e:
                        st.exception(e)

    with tab_score:
        st.caption(
            "Metrics on **eval** split only (honest). Compares Ridge vs manual gold and LLM vs manual gold. "
            "LLM scores are benchmark-only — never used for training."
        )
        eval_rows = db.fetch_labels_xy(conn, head_id, "eval")
        n_eval = len(eval_rows)
        st.write(f"Eval labels: **{n_eval}**")
        if posts_err:
            st.error(posts_err)
        if n_eval < 10:
            st.warning("Fewer than 10 eval labels — metrics may be noisy.")
        if not head.artifact_dir:
            st.warning("Train first.")
        elif posts_err or not posts or not eval_rows:
            st.info("Need eval labels and a valid posts JSON (see Data source tab).")
        else:
            art = resolve_out_dir(Path(head.artifact_dir))
            if st.button("Score on eval"):
                texts, ys = claims_data.build_xy_for_labels(
                    posts,
                    eval_rows,
                    input_var_keys=head.input_var_keys,
                    score_field_name=head.score_field_name,
                )
                if len(texts) != len(ys) or not texts:
                    st.error("Could not join eval labels to loaded posts (path mismatch?).")
                else:
                    pred = FieldPredictor.load(art, batch_size=32)
                    y_hat = pred.predict_scores(texts)

                    idx = claims_data.index_claims_by_key(posts)
                    y_llm: list[float | None] = []
                    score_key = head.score_field_name
                    for tid, cidx, _y in eval_rows:
                        llm_v: float | None = None
                        if score_key:
                            hit = idx.get((tid, cidx))
                            if hit is not None:
                                _post, claim_dict = hit
                                parsed, bad = parse_score_01(claim_dict.get(score_key))
                                if parsed is not None and not bad:
                                    llm_v = parsed
                        y_llm.append(llm_v)

                    cmp = eval_metrics.compare_to_llm_baseline(ys, y_hat, y_llm)
                    _render_eval_score_results(
                        cmp,
                        eval_rows=eval_rows,
                        score_key=score_key,
                        n_eval=n_eval,
                    )


if __name__ == "__main__":
    main()
