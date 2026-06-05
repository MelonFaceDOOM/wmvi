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
        split_seed = st.number_input("Random seed (label split)", value=42, step=1)
        st.session_state["split_seed"] = int(split_seed)
        max_posts = st.number_input("Max success posts (0 = all)", min_value=0, value=200, step=50)
        max_claims = st.number_input("Max claims total (0 = all)", min_value=0, value=500, step=100)
        st.session_state["max_posts"] = int(max_posts)
        st.session_state["max_claims"] = int(max_claims)
        if st.button("Clear encoder cache"):
            clear_encoder_cache()
            st.success("Encoder cache cleared.")
        if st.button("Clear posts file cache"):
            _load_posts.clear()
            st.success("Posts cache cleared. Reload the page or switch tabs after fixing JSON.")

    conn = _open_db(Path(db_path))

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
        conn.close()
        return

    sel = st.selectbox("Open head", options=list(head_options.keys()))
    head_id = head_options[sel]
    head = db.get_head(conn, head_id)
    if head is None:
        st.error("Head not found.")
        conn.close()
        return

    if head.score_field_name:
        desc = _field_description(head.score_field_name)
        st.info(f"**Standard field head:** `{head.score_field_name}`" + (f" — {desc}" if desc else ""))

    tab_data, tab_run, tab_label, tab_revise, tab_train, tab_score = st.tabs(
        ["Data source", "Run model", "Manual label", "Review Labels", "Train", "Score (eval)"]
    )

    posts, posts_err = _load_posts(posts_path)

    with tab_data:
        st.write("Posts JSON is loaded with caps from the sidebar (max posts / max claims).")
        st.code(posts_path, language=None)
        if posts_err:
            st.error(posts_err)
            st.caption("Use **Clear posts file cache** in the sidebar after fixing the file.")
        elif not posts:
            st.warning("Loaded file has no post dicts in `posts[]`.")
        else:
            n_claims = sum(1 for _ in claims_data.iter_success_claims(posts, max_posts=None, max_claims=None))
            st.metric("Success-post claims (full file)", n_claims)
            st.caption("Caps apply in other tabs when iterating.")

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

        lim_p = st.session_state.get("max_posts") or None
        lim_c = st.session_state.get("max_claims") or None
        if lim_p == 0:
            lim_p = None
        if lim_c == 0:
            lim_c = None

        unlabeled_in_cap: int | None = None
        orphaned = 0
        if not posts_err and posts:
            idx_map = claims_data.index_claims_by_key(posts)
            cap_claims = list(
                claims_data.iter_success_claims(posts, max_posts=lim_p, max_claims=lim_c)
            )
            labeled_keys = {
                (r["task_id"], r["claim_index"])
                for r in db.fetch_labels_sorted(conn, head_id, split=None)
            }
            unlabeled_in_cap = sum(
                1 for _post, _claim, tid, cidx in cap_claims if (tid, cidx) not in labeled_keys
            )
            orphaned = sum(1 for tid, cidx in labeled_keys if (tid, cidx) not in idx_map)

        c_a, c_b, c_c = st.columns(3)
        with c_a:
            if unlabeled_in_cap is not None:
                st.metric("Unlabeled (sidebar cap)", unlabeled_in_cap)
            else:
                st.metric("Unlabeled (sidebar cap)", "—")
        with c_b:
            st.metric("Model trained", "Yes" if head.artifact_dir else "No")
        with c_c:
            if orphaned:
                st.metric("Orphaned labels", orphaned, help="Labels whose task_id/claim_index are not in the loaded posts JSON")
            else:
                st.metric("Orphaned labels", 0)

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
        st.caption("Runs BGE + Ridge on capped claims. Does not read LLM score columns from JSON.")
        if not head.artifact_dir:
            st.warning("Train this head first (artifact_dir not set).")
        else:
            art = resolve_out_dir(Path(head.artifact_dir))
            if not art.is_dir():
                st.error(f"Artifact dir missing: {art}")
            else:
                lim_p = st.session_state.get("max_posts") or None
                lim_c = st.session_state.get("max_claims") or None
                if lim_p == 0:
                    lim_p = None
                if lim_c == 0:
                    lim_c = None
                if posts_err:
                    st.error(posts_err)
                elif not posts:
                    st.warning("No posts loaded.")
                elif st.button("Run preview", key="run_prev"):
                    rows_out = []
                    pred = FieldPredictor.load(art, batch_size=32)
                    for post_row, claim_dict, tid, idx in claims_data.iter_success_claims(
                        posts, max_posts=lim_p, max_claims=lim_c
                    ):
                        txt = _build_head_input(head, post_row, claim_dict, tid=tid, idx=idx)
                        yh = pred.predict_scores([txt])[0]
                        rows_out.append(
                            {
                                "task_id": tid,
                                "claim_index": idx,
                                "y_hat": yh,
                                "input_excerpt": txt[:200].replace("\n", " ") + ("…" if len(txt) > 200 else ""),
                            }
                        )
                    st.dataframe(rows_out, use_container_width=True)
                    st.session_state["last_run_rows"] = rows_out

    with tab_label:
        st.caption(
            "One variable at a time, then a score in [0, 1]. "
            "Train/eval split is assigned automatically from the sidebar fraction (stable per claim)."
        )
        if posts_err:
            st.error(posts_err)
        elif not posts:
            st.warning("Load posts JSON first (see Data source tab).")
        else:
            lim_p = st.session_state.get("max_posts") or None
            lim_c = st.session_state.get("max_claims") or None
            if lim_p == 0:
                lim_p = None
            if lim_c == 0:
                lim_c = None
            queue = [
                (post_row, claim_dict, tid, idx)
                for post_row, claim_dict, tid, idx in claims_data.iter_success_claims(
                    posts, max_posts=lim_p, max_claims=lim_c
                )
                if db.get_label(conn, head_id, tid, idx) is None
            ]
            st.write(f"Unlabeled in cap: **{len(queue)}**")
            if queue:
                post_row, claim_dict, tid, idx = queue[0]
                st.write(f"`task_id`={tid} · `claim_index`={idx}")
                if head.score_field_name:
                    desc = _field_description(head.score_field_name)
                    if desc:
                        st.markdown(f"**Score semantics:** {desc}")
                    model_input = _build_head_input(head, post_row, claim_dict, tid=tid, idx=idx)
                    st.markdown("**Model input preview**")
                    st.text_area(
                        "structured input",
                        value=model_input,
                        height=min(400, max(120, 12 * (1 + len(model_input) // 80))),
                        disabled=True,
                        key=f"lbl_input_{tid}_{idx}",
                        label_visibility="collapsed",
                    )
                else:
                    for vk in head.input_var_keys:
                        st.markdown(f"**{var_registry.display_name(vk)}** (`{vk}`)")
                        st.text_area(
                            "value",
                            value=var_registry.extract_var(vk, post_row, claim_dict),
                            height=min(400, max(120, 12 * (1 + len(var_registry.extract_var(vk, post_row, claim_dict)) // 80))),
                            disabled=True,
                            key=f"lbl_{vk}_{tid}_{idx}",
                            label_visibility="collapsed",
                        )
                y_in = st.text_input("Score y in [0, 1]", value="0.5", key=f"yval_{tid}_{idx}")
                if st.button("Save label"):
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
                table_rows.append(
                    {
                        "label": row["y"],
                        "split": row["split"],
                        "task_id": tid,
                        "claim_index": cidx,
                        "claim_excerpt": claim_excerpt,
                        "created_at": row.get("created_at") or "",
                        "delete": False,
                    }
                )
            orig_by_key = {
                (r["task_id"], r["claim_index"]): r for r in table_rows
            }
            df = pd.DataFrame(table_rows)

            edited = st.data_editor(
                df,
                column_config={
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
                },
                disabled=["task_id", "claim_index", "claim_excerpt", "created_at"],
                hide_index=True,
                use_container_width=True,
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
                texts, ys = claims_data.build_xy_for_labels(
                    posts,
                    labeled,
                    input_var_keys=head.input_var_keys,
                    score_field_name=head.score_field_name,
                )
                if len(texts) < 3:
                    st.error("Too few labels matched posts JSON (check task_id / claim_index alignment).")
                else:
                    slug = _slug(head.name)
                    out_dir = resolve_out_dir(REPO_ROOT / "data" / "models" / "ridge_lab" / slug)
                    try:
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
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.markdown("**Ridge vs manual gold**")
                        st.json(cmp["ridge_vs_manual"])
                    with col_b:
                        st.markdown("**LLM vs manual gold** (benchmark)")
                        st.json(cmp["llm_vs_manual"])
                        if cmp["n_llm_invalid"]:
                            st.caption(f"Skipped {cmp['n_llm_invalid']} row(s) with invalid/missing LLM scores.")

                    beats = cmp.get("beats_llm")
                    if beats is True:
                        st.success("Ridge beats LLM on this eval split (MAE lower; Pearson ≥ LLM).")
                    elif beats is False:
                        st.warning("Ridge does not yet beat LLM on this eval split.")
                    elif score_key and n_eval >= 10:
                        st.info("Need ≥10 valid LLM scores on eval rows to compute beats-LLM flag.")

                    if cmp.get("per_row"):
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
                                for r in cmp["per_row"]
                            ],
                            use_container_width=True,
                        )

    conn.close()


if __name__ == "__main__":
    main()
