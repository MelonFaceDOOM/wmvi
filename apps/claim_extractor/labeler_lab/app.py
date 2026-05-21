"""
Streamlit lab: generic Ridge heads (name + ordered input variables), labeling, train, score, preview inference.

From the **repository root** (so ``apps`` is importable), run::

  streamlit run apps/claim_extractor/labeler_lab/app.py

The app prepends the repo root to ``sys.path`` if ``apps`` is not found (e.g. some Streamlit cwd setups).
"""

from __future__ import annotations

import json
import random
import re
import sys
from pathlib import Path

# Repo root = parent of ``apps/`` (labeler_lab -> claim_extractor -> apps -> wmvi)
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import streamlit as st

from apps.claim_extractor.labeler_lab import claims_data, db, eval_metrics, text_builder, var_registry
from apps.claim_extractor.learned.constants import REPO_ROOT
from apps.claim_extractor.learned.predict import FieldPredictor, clear_encoder_cache
from apps.claim_extractor.learned.train import resolve_out_dir, run_train_from_pairs
from apps.claim_extractor.model_common import parse_score_01


def _slug(name: str) -> str:
    x = re.sub(r"[^a-zA-Z0-9_-]+", "-", name.strip().lower()).strip("-")
    return (x[:80] if x else "head")


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

    tab_data, tab_run, tab_label, tab_train, tab_score = st.tabs(
        ["Data source", "Run model", "Manual label", "Train", "Score (eval)"]
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
            n = sum(1 for _ in claims_data.iter_success_claims(posts, max_posts=None, max_claims=None))
            st.metric("Success-post claims (full file)", n)
            st.caption("Caps apply in other tabs when iterating.")

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
                        txt = text_builder.build_structured_input(head.input_var_keys, post_row, claim_dict)
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
        st.caption("One variable at a time, then a score in [0, 1]. Split (train/eval) assigned randomly.")
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
                        random.seed(int(st.session_state.get("split_seed", 42)))
                        ev = float(st.session_state.get("eval_frac", 0.2))
                        sp = "eval" if random.random() < ev else "train"
                        db.upsert_label(conn, head_id=head_id, task_id=tid, claim_index=idx, y=yv, split=sp)
                        st.success(f"Saved ({sp}).")
                        st.rerun()

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
                texts, ys = claims_data.build_xy_for_labels(posts, head.input_var_keys, labeled)
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
        st.caption("Metrics on **eval** split only (honest). If &lt; 10 eval rows, interpret cautiously.")
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
                texts, ys = claims_data.build_xy_for_labels(posts, head.input_var_keys, eval_rows)
                if len(texts) != len(ys) or not texts:
                    st.error("Could not join eval labels to loaded posts (path mismatch?).")
                else:
                    pred = FieldPredictor.load(art, batch_size=32)
                    y_hat = pred.predict_scores(texts)
                    m = eval_metrics.eval_predictions(ys, y_hat)
                    st.json(m)

    conn.close()


if __name__ == "__main__":
    main()
