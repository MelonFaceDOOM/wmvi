"""
Streamlit lab for iterating claim-extraction prompts on a curated problem-post set.

From the **repository root**::

  streamlit run apps/claim_extractor/refinement_lab/app.py
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import streamlit as st

from apps.claim_extractor.claim_normalize import normalize_claim_text
# refinement_lab uses personal OpenAI; swap imports to check_azure_connectivity / load_azure_config for Azure.
from apps.claim_extractor.extraction_core import (
    check_openai_connectivity as check_llm_connectivity,
    load_openai_config as load_llm_config,
)
from apps.claim_extractor.learned.constants import REPO_ROOT
from apps.claim_extractor.refinement_lab import db, extract_runner, optimizer, posts_data, prompt_vars
from apps.claim_extractor.refinement_lab.meta_defaults import META_PROMPT_SPECS, validate_meta_prompt
from apps.claim_extractor.refinement_lab.models import DEFAULT_MODEL, SEED_MODELS
from apps.claim_extractor.scoring_inputs import context_text_for_post_row

_PREVIEW_LONG_CHARS = 1000
_PREVIEW_LONG_LINES = 12
_PREVIEW_MAX_HEIGHT_PX = 420


def _text_area_height(text: str, *, min_h: int = 68, max_h: int = 420, px_per_line: int = 20) -> int:
    raw_lines = text.splitlines() or [""]
    visual_lines = sum(max(1, (len(line) + 89) // 90) for line in raw_lines)
    return min(max_h, max(min_h, 16 + visual_lines * px_per_line))


def _preview_is_long(text: str) -> bool:
    return len(text) > _PREVIEW_LONG_CHARS or (text.count("\n") + 1) > _PREVIEW_LONG_LINES


def _show_post_text(text: str, *, key: str) -> None:
    """Single read-only text box; long posts scroll inside a capped height."""
    if not text.strip():
        st.caption("(empty post text)")
        return
    height: int | str = (
        _text_area_height(text, max_h=_PREVIEW_MAX_HEIGHT_PX)
        if _preview_is_long(text)
        else "content"
    )
    st.text_area(
        "post",
        value=text,
        height=height,
        disabled=True,
        key=key,
        label_visibility="collapsed",
    )


def _preview_text_area(title: str, text: str, *, key: str) -> None:
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


def _render_claim_list(claims: list | None, *, key_prefix: str) -> None:
    texts = posts_data.claim_texts(claims if isinstance(claims, list) else None)
    if not texts:
        st.caption("(no claims)")
        return
    for i, t in enumerate(texts):
        st.markdown(f"{i + 1}. {t}")


def _claim_norm_set(claims: list | None) -> set[str]:
    return {normalize_claim_text(t) for t in posts_data.claim_texts(claims) if normalize_claim_text(t)}


def _render_claim_diff(reference: list | None, profile_claims: list | None) -> None:
    ref_set = _claim_norm_set(reference)
    prof_set = _claim_norm_set(profile_claims)
    only_ref = ref_set - prof_set
    only_prof = prof_set - ref_set
    if only_ref:
        st.caption("Only in Reference:")
        for t in sorted(only_ref):
            st.markdown(f"- {t}")
    if only_prof:
        st.caption("Only in this profile:")
        for t in sorted(only_prof):
            st.markdown(f"- {t}")
    if not only_ref and not only_prof and ref_set:
        st.caption("Claim set matches Reference (normalized text).")


@st.cache_data(show_spinner=True)
def _load_posts(path_str: str) -> tuple[list, str | None]:
    if not path_str.strip():
        return [], "Posts path is empty."
    raw = path_str.strip()
    candidates = [Path(raw).expanduser()]
    if not candidates[0].is_file() and not candidates[0].is_absolute():
        candidates.append(REPO_ROOT / raw.lstrip("/"))
    p: Path | None = None
    for c in candidates:
        if c.is_file():
            p = c
            break
    if p is None:
        return [], f"No file found. Tried: {', '.join(str(c) for c in candidates)}"
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return [], f"Could not load {p}: {e}"
    if not isinstance(payload, dict):
        return [], "JSON root must be an object with a `posts` array."
    posts = payload.get("posts")
    if not isinstance(posts, list):
        return [], "Expected top-level key `posts` to be a JSON array."
    return [x for x in posts if isinstance(x, dict)], None


def _open_db(path: Path):
    conn = db.connect(path)
    db.init_lab(conn)
    return conn


def _shuffle_queue(queue: list, *, seed: int) -> list:
    if len(queue) < 2:
        return list(queue)
    return sorted(
        queue,
        key=lambda item: int(
            hashlib.sha256(f"browse|{seed}|{item[1]}".encode()).hexdigest()[:16],
            16,
        ),
    )


def _render_status_filter(key_prefix: str) -> set[str]:
    st.markdown("**Status filter**")
    enabled: set[str] = set()
    c1, c2, _ = st.columns([1, 1, 10])
    with c1:
        if st.checkbox("success", value=True, key=f"{key_prefix}_st_success"):
            enabled.add("success")
    with c2:
        if st.checkbox("failed", value=True, key=f"{key_prefix}_st_failed"):
            enabled.add("failed")
    return enabled


def _render_platform_filter(platforms: list[str], key_prefix: str) -> set[str]:
    label = f"Platform filter ({len(platforms)} sources)" if platforms else "Platform filter"
    with st.popover(label):
        st.caption("Uncheck platforms to exclude from the queue.")
        if not platforms:
            st.info("No platforms in queue.")
            return set()
        enabled: set[str] = set()
        for platform in platforms:
            if st.checkbox(platform, value=True, key=f"{key_prefix}_plat_{platform}"):
                enabled.add(platform)
        return enabled


def _build_browse_queue(
    posts: list,
    *,
    conn,
    enabled_status: set[str],
    enabled_platforms: set[str],
) -> list[tuple[dict, str]]:
    problem_ids = {p["task_id"] for p in db.fetch_problem_posts_sorted(conn, descending=False)}
    skip_ids = db.fetch_reviewed_skip_ids(conn)
    queue: list[tuple[dict, str]] = []
    for row, tid in posts_data.iter_all_posts(posts):
        if tid in problem_ids or tid in skip_ids:
            continue
        status = posts_data.extraction_status(row)
        platform = posts_data.platform_name(row)
        if enabled_status and status not in enabled_status:
            continue
        if enabled_platforms and platform not in enabled_platforms:
            continue
        queue.append((row, tid))
    return queue


def _render_post_header(post_row: dict, task_id: str, *, extra: str = "") -> None:
    status = posts_data.extraction_status(post_row)
    platform = posts_data.platform_name(post_row)
    line = f"`task_id`={task_id} · `platform`={platform} · extraction **{status}**"
    if extra:
        line += f" · {extra}"
    st.markdown(line)


def _render_browse_tab(conn, posts: list, posts_err: str | None) -> None:
    st.caption("Review source posts and mark extraction problems or skip.")
    if posts_err:
        st.error(posts_err)
        return
    if not posts:
        st.warning("No posts loaded.")
        return

    full_queue = _build_browse_queue(
        posts,
        conn=conn,
        enabled_status={"success", "failed"},
        enabled_platforms=set(),
    )
    all_platforms = sorted({posts_data.platform_name(r) for r, _ in full_queue})
    enabled_status = _render_status_filter("browse")
    enabled_platforms = _render_platform_filter(all_platforms, "browse")
    queue = _build_browse_queue(
        posts,
        conn=conn,
        enabled_status=enabled_status,
        enabled_platforms=enabled_platforms,
    )

    shuffle = st.checkbox("Shuffle browse queue", value=True, key="browse_shuffle")
    seed = int(st.session_state.get("browse_seed", 42))
    if shuffle and len(queue) > 1:
        queue = _shuffle_queue(queue, seed=seed)

    if not enabled_status:
        st.warning("Enable at least one status filter.")
        return
    if all_platforms and not enabled_platforms:
        st.warning("Enable at least one platform in **Platform filter**.")
        return

    st.write(f"**{len(queue)}** post(s) in browse queue")
    if not queue:
        st.info("No posts match filters (or all reviewed).")
        return

    post_row, tid = queue[0]
    _render_post_header(post_row, tid)
    ctx = context_text_for_post_row(post_row)
    _preview_text_area("Post text", ctx, key=f"browse_txt_{tid}")

    if posts_data.extraction_status(post_row) == "success":
        st.markdown("**Current extraction (source JSON)**")
        _render_claim_list(posts_data.baseline_claims_from_post(post_row), key_prefix=f"browse_{tid}")

    comment = st.text_area("Comment (problem description)", key=f"browse_comment_{tid}", height=100)
    c_mark, c_skip, _ = st.columns([2, 1, 8])
    with c_mark:
        if st.button("Mark as problem", key=f"browse_mark_{tid}"):
            note = (comment or "").strip()
            if not note:
                st.error("Add a comment describing the extraction problem.")
            else:
                db.upsert_problem_post(
                    conn,
                    task_id=tid,
                    post_row=post_row,
                    baseline_claims=posts_data.baseline_claims_from_post(post_row),
                    baseline_status=posts_data.extraction_status(post_row),
                    comment=note,
                    source="browse",
                )
                db.sync_baseline_extractions(conn)
                st.success("Marked as problem.")
                st.rerun()
    with c_skip:
        if st.button("Skip", key=f"browse_skip_{tid}"):
            db.add_reviewed_skip(conn, tid)
            st.rerun()


def _render_profiles_tab(conn) -> None:
    profiles = db.list_profiles(conn)
    st.caption("Prompt profiles run **claims-only** extraction on the problem post set only.")

    with st.expander("Variable bank", expanded=False):
        for k in prompt_vars.list_var_keys():
            st.text(f"{{{k}}} — {prompt_vars.display_name(k)}")

    c_new, _ = st.columns([1, 3])
    with c_new:
        new_name = st.text_input("New profile name", key="new_profile_name", placeholder="e.g. epidemiologist_v2")
        if st.button("Create profile"):
            if not new_name.strip():
                st.error("Name required.")
            else:
                pid = db.create_profile_from_latest(conn, new_name.strip())
                st.success(f"Created profile id={pid}")
                st.session_state["edit_profile_id"] = pid
                st.rerun()

    if not profiles:
        st.info("Create a profile to edit prompts and run extraction.")
        return

    profile_options = {f"{p.id}: {p.name}": p.id for p in profiles}
    default_id = st.session_state.get("edit_profile_id", profiles[0].id)
    default_key = next((k for k, v in profile_options.items() if v == default_id), list(profile_options.keys())[0])
    sel = st.selectbox("Edit profile", options=list(profile_options.keys()), index=list(profile_options.keys()).index(default_key))
    profile_id = profile_options[sel]
    profile = db.get_profile(conn, profile_id)
    if profile is None:
        st.error("Profile not found.")
        return

    name = st.text_input("Name", value=profile.name, key=f"prof_name_{profile_id}")
    model_options = list(SEED_MODELS)
    if profile.model not in model_options:
        model_options = [profile.model, *model_options]
    model_sel = st.selectbox("Model", options=model_options, index=model_options.index(profile.model), key=f"prof_model_{profile_id}")
    custom_model = st.text_input("Or custom model deployment", value="", key=f"prof_model_custom_{profile_id}")
    model = custom_model.strip() or model_sel
    max_claims = st.number_input("Max claims", min_value=1, max_value=20, value=profile.max_claims, key=f"prof_mc_{profile_id}")
    system_prompt = st.text_area(
        "System prompt",
        value=profile.system_prompt,
        height=280,
        key=f"prof_sys_{profile_id}",
        help="Use {var_name} placeholders from the variable bank.",
    )
    user_prompt = st.text_area(
        "User prompt",
        value=profile.user_prompt,
        height=200,
        key=f"prof_user_{profile_id}",
    )

    if st.button("Save profile", key=f"prof_save_{profile_id}"):
        db.update_profile(
            conn,
            profile_id,
            name=name,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            model=model,
            max_claims=int(max_claims),
        )
        st.success("Saved.")
        st.rerun()

    notes = db.fetch_profile_notes(conn, profile_id)
    if notes:
        with st.expander("Profile notes (optimizer)", expanded=False):
            for n in notes:
                st.markdown(f"**{n['kind']}** · {n['created_at']}")
                st.text(n["content"][:4000])

    problem_posts = db.fetch_problem_posts_sorted(conn, descending=False)
    st.write(f"Problem posts to extract: **{len(problem_posts)}**")
    if not problem_posts:
        st.warning("Mark problem posts first on the Browse tab.")
        return

    test_col, run_col = st.columns([1, 1])
    with test_col:
        if st.button("Test OpenAI connection", key=f"prof_test_openai_{profile_id}"):
            with st.spinner("Testing OpenAI connection…"):
                ok, message = check_llm_connectivity(model)
            if ok:
                st.success(message)
            else:
                st.error(message)

    with run_col:
        run_clicked = st.button("Run on all problem posts", key=f"prof_run_{profile_id}")

    if run_clicked:
        if not system_prompt.strip() and not user_prompt.strip():
            st.warning("System and user prompts are both empty. Add at least one prompt before running.")
            return
        try:
            load_llm_config()
        except RuntimeError as exc:
            st.error(f"OpenAI not configured: {exc}")
            return

        profile = db.get_profile(conn, profile_id)
        if profile is None:
            st.error("Profile not found.")
            return
        with st.status(f"Running profile **{profile.name}** on {len(problem_posts)} post(s)…", expanded=True) as status:
            progress = st.progress(0.0)
            state = {"done": 0, "total": len(problem_posts)}

            def on_progress(done: int, total: int, msg: str) -> None:
                state["done"] = done
                state["total"] = max(total, 1)
                progress.progress(min(1.0, done / state["total"]), text=f"{msg} ({done}/{total})")

            try:
                ok, fail = extract_runner.run_profile_on_posts(
                    conn,
                    profile,
                    problem_posts,
                    on_progress=on_progress,
                )
                progress.empty()
                status.update(
                    label=f"Done — {ok} succeeded, {fail} failed",
                    state="complete" if fail == 0 else "error",
                )
                st.success(f"Extraction finished: **{ok}** ok, **{fail}** failed.")
                if fail:
                    st.info("Open the Compare tab to see per-post extraction errors.")
            except RuntimeError as exc:
                progress.empty()
                status.update(label="Failed", state="error")
                st.error(str(exc))
            except Exception as exc:
                progress.empty()
                status.update(label="Failed", state="error")
                st.error(f"Run failed: {exc}")


def _reference_claims_list(conn, task_id: str) -> list[dict]:
    ref = db.get_reference_claims(conn, task_id)
    return list(ref.claims) if ref else []


def _render_reference_tab(conn, *, tid: str, block_idx: int) -> None:
    with st.container(border=True):
        st.markdown("**:gold[Reference (gold)]** — editable ground truth (no prompts)")
        ref = db.get_reference_claims(conn, tid)
        if ref and ref.updated_at:
            st.caption(f"Source: **{ref.source}** · updated {ref.updated_at}")
        claims = _reference_claims_list(conn, tid)
        texts = posts_data.claim_texts(claims)
        for i, t in enumerate(texts):
            c1, c2, c3 = st.columns([6, 1, 1])
            with c1:
                edited = st.text_input(
                    f"claim_{i}",
                    value=t,
                    key=f"ref_edit_{block_idx}_{tid}_{i}",
                    label_visibility="collapsed",
                )
            with c2:
                if st.button("Save", key=f"ref_save_{block_idx}_{tid}_{i}"):
                    try:
                        db.edit_reference_claim(conn, tid, i, edited)
                        st.rerun()
                    except IndexError as e:
                        st.error(str(e))
            with c3:
                if st.button("Del", key=f"ref_del_{block_idx}_{tid}_{i}"):
                    try:
                        db.delete_reference_claim(conn, tid, i)
                        st.rerun()
                    except IndexError as e:
                        st.error(str(e))
        if not texts:
            st.caption("(no reference claims yet)")
        new_claim = st.text_input("New claim", key=f"ref_new_{block_idx}_{tid}", placeholder="Add a reference claim…")
        if st.button("Add claim", key=f"ref_add_{block_idx}_{tid}"):
            if not new_claim.strip():
                st.warning("Enter claim text.")
            else:
                db.add_reference_claim(conn, tid, new_claim.strip())
                st.rerun()


def _render_compare_extraction_tab(
    *,
    conn,
    pp: dict,
    post_row: dict,
    tid: str,
    prof: db.PromptProfile,
    hit: dict | None,
    reference_claims: list | None,
    block_idx: int,
) -> None:
    st.caption(f"Model: `{prof.model}` · max_claims={prof.max_claims}")
    ev = db.get_evaluation(conn, prof.id, tid)
    if ev and ev.get("f1") is not None:
        st.caption(
            f"vs Reference — P={ev.get('precision', 0):.2f} "
            f"R={ev.get('recall', 0):.2f} F1={ev.get('f1', 0):.2f}"
        )
    if hit is None:
        st.caption("(not run yet)")
        return
    if hit.get("run_at"):
        st.caption(f"Last run: {hit['run_at']}")
    if hit["status"] == "failed":
        st.error(hit.get("error") or "extraction failed")
        return
    out = hit.get("output_json")
    claims = out.get("claims") if isinstance(out, dict) else None
    _render_claim_list(claims, key_prefix=f"prof_{prof.id}_{tid}")
    _render_claim_diff(reference_claims, claims)
    with st.expander("Rendered prompts (preview)", expanded=False):
        try:
            sys_r, usr_r = prompt_vars.render_profile_prompts(
                system_prompt=prof.system_prompt,
                user_prompt=prof.user_prompt,
                post_row=post_row,
                max_claims=prof.max_claims,
            )
            st.markdown("**System**")
            st.code(sys_r, language=None)
            st.markdown("**User**")
            st.code(usr_r, language=None)
        except ValueError as e:
            st.error(str(e))


def _render_compare_post_block(
    conn,
    pp: dict,
    profiles: list[db.PromptProfile],
    *,
    block_idx: int,
) -> None:
    post_row = pp["post_row"]
    tid = pp["task_id"]
    _render_post_header(
        post_row,
        tid,
        extra=f"marked **{pp['source']}** · baseline **{pp['baseline_status']}**",
    )
    if pp.get("comment"):
        st.markdown(f"**Comment:** {pp['comment']}")
    if pp.get("created_at"):
        st.caption(f"Added {pp['created_at']}")

    ctx = context_text_for_post_row(post_row)
    _show_post_text(ctx, key=f"compare_post_{block_idx}_{tid}")

    reference_claims = _reference_claims_list(conn, tid)
    extractions = db.fetch_extractions_for_task(conn, tid)
    extraction_by_profile = {e["profile_id"]: e for e in extractions}

    baseline_prof = db.get_profile_by_name(conn, db.BASELINE_PROFILE_NAME)
    non_baseline = [p for p in sorted(profiles, key=lambda x: x.id) if p.name != db.BASELINE_PROFILE_NAME]

    tab_labels = ["Reference", "Baseline"] + [p.name for p in non_baseline]
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        _render_reference_tab(conn, tid=tid, block_idx=block_idx)

    with tabs[1]:
        if baseline_prof is None:
            st.caption("Baseline profile not found.")
        else:
            hit = extraction_by_profile.get(baseline_prof.id)
            _render_compare_extraction_tab(
                conn=conn,
                pp=pp,
                post_row=post_row,
                tid=tid,
                prof=baseline_prof,
                hit=hit,
                reference_claims=reference_claims,
                block_idx=block_idx,
            )

    for i, prof in enumerate(non_baseline):
        with tabs[i + 2]:
            _render_compare_extraction_tab(
                conn=conn,
                pp=pp,
                post_row=post_row,
                tid=tid,
                prof=prof,
                hit=extraction_by_profile.get(prof.id),
                reference_claims=reference_claims,
                block_idx=block_idx,
            )


def _render_compare_tab(conn) -> None:
    problem_posts = db.fetch_problem_posts_sorted(conn, descending=True)
    profiles = db.list_profiles(conn)
    if not problem_posts:
        st.info("No problem posts yet. Mark some on the Browse tab.")
        return

    platforms = sorted({posts_data.platform_name(pp["post_row"]) for pp in problem_posts})
    compare_platforms = _render_platform_filter(platforms, "compare")
    filtered = [
        pp for pp in problem_posts if posts_data.platform_name(pp["post_row"]) in compare_platforms
    ] if compare_platforms else []

    if platforms and not compare_platforms:
        st.warning("Enable at least one platform filter.")
        return
    if not filtered:
        st.info("No problem posts match platform filter.")
        return

    st.caption(f"**{len(filtered)}** problem post(s)")
    if not profiles:
        st.info("Create and run a prompt profile on the Profiles tab to compare extractions.")

    eval_profiles = [p for p in profiles if p.name != db.BASELINE_PROFILE_NAME]
    if eval_profiles:
        eval_sel = {f"{p.id}: {p.name}": p for p in eval_profiles}
        eval_key = st.selectbox("Evaluate profile vs Reference", options=list(eval_sel.keys()))
        eval_prof = eval_sel[eval_key]
        expensive = st.text_input("Judge model", value=eval_prof.model, key="compare_judge_model")
        if st.button("Run evaluation vs Reference", key="compare_run_eval"):
            try:
                load_llm_config()
                with st.spinner("Judging…"):
                    optimizer.judge_profile_against_reference(
                        conn,
                        eval_prof,
                        filtered,
                        judge_model=expensive.strip() or eval_prof.model,
                    )
                st.success("Evaluation complete.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

    for i, pp in enumerate(filtered):
        if i > 0:
            st.divider()
        _render_compare_post_block(conn, pp, profiles, block_idx=i)


def _render_optimize_tab(conn) -> None:
    st.caption("Configure objective, generate Reference gold claims, and run the autonomous optimizer.")

    problem_posts = db.fetch_problem_posts_sorted(conn, descending=False)
    profiles = db.list_profiles(conn)
    if not problem_posts:
        st.warning("Mark problem posts on the Browse tab first.")
        return

    st.subheader("Objective")
    obj = db.get_meta_prompt(conn, "objective") or ""
    new_obj = st.text_area("Optimization objective", value=obj, height=120, key="opt_objective")
    if st.button("Save objective", key="opt_save_obj"):
        db.upsert_meta_prompt(conn, "objective", new_obj)
        st.success("Saved.")
        st.rerun()

    with st.expander("Meta-prompt templates", expanded=False):
        st.caption("Required placeholders are validated on save.")
        for name in sorted(META_PROMPT_SPECS.keys()):
            tpl = db.get_meta_prompt(conn, name) or META_PROMPT_SPECS[name]["template"]
            req = ", ".join(f"`{{{v}}}`" for v in META_PROMPT_SPECS[name]["required"])
            st.markdown(f"**{name}** — requires {req}")
            edited = st.text_area(name, value=tpl, height=160, key=f"meta_{name}")
            if st.button(f"Save {name}", key=f"save_meta_{name}"):
                try:
                    db.upsert_meta_prompt(conn, name, edited)
                    st.success(f"Saved {name}.")
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))

    st.divider()
    st.subheader("Reference setup")
    st.warning(
        "Generating Reference **overwrites** all Reference claims for every problem post, "
        "including manual edits."
    )
    if not profiles:
        st.info("Create a profile first.")
    else:
        prof_map = {f"{p.id}: {p.name}": p for p in profiles}
        ref_prof_key = st.selectbox("Profile (prompts only)", options=list(prof_map.keys()), key="opt_ref_prof")
        ref_prof = prof_map[ref_prof_key]
        ref_model = st.text_input("Expensive model for Reference", value=ref_prof.model, key="opt_ref_model")
        if st.button("Generate Reference from profile", key="opt_gen_ref"):
            try:
                load_llm_config()
                with st.status("Generating Reference…", expanded=True) as status:
                    prog = st.progress(0.0)

                    def on_progress(done: int, total: int, msg: str) -> None:
                        prog.progress(min(1.0, done / max(total, 1)), text=f"{msg} ({done}/{total})")

                    ok, fail = extract_runner.run_profile_on_posts(
                        conn,
                        ref_prof,
                        problem_posts,
                        model=ref_model.strip() or ref_prof.model,
                        on_progress=on_progress,
                        write_reference=True,
                    )
                    status.update(label=f"Done — {ok} ok, {fail} failed", state="complete")
                st.success(f"Reference updated: **{ok}** posts.")
            except Exception as exc:
                st.error(str(exc))

    st.divider()
    st.subheader("Improvement loop")
    if not profiles:
        return
    inp_map = {f"{p.id}: {p.name}": p for p in profiles}
    inp_key = st.selectbox("Input profile", options=list(inp_map.keys()), key="opt_in_prof")
    input_prof = inp_map[inp_key]
    c1, c2, c3 = st.columns(3)
    with c1:
        cheap_model = st.text_input("Cheap model", value=input_prof.model, key="opt_cheap")
    with c2:
        expensive_model = st.text_input("Expensive model", value="gpt-5.4", key="opt_expensive")
    with c3:
        max_iters = st.number_input("Max iterations", min_value=1, max_value=10, value=3, key="opt_max_iters")
    patience = st.number_input("Patience (no-improve stops)", min_value=1, max_value=5, value=2, key="opt_patience")

    if st.button("Run optimization", key="opt_run", type="primary"):
        try:
            load_llm_config()
            cfg = optimizer.OptimizationConfig(
                expensive_model=expensive_model.strip() or input_prof.model,
                cheap_model=cheap_model.strip() or input_prof.model,
                max_iters=int(max_iters),
                patience=int(patience),
            )
            log_box = st.empty()
            logs: list[str] = []

            def on_progress(msg: str) -> None:
                logs.append(msg)
                log_box.code("\n".join(logs[-12:]))

            with st.status("Optimizing…", expanded=True):
                result = optimizer.run_optimization(
                    conn,
                    input_prof,
                    problem_posts,
                    cfg,
                    on_progress=on_progress,
                )
            st.success(f"Run complete (id={result.get('run_id')}).")
            if result.get("summary"):
                st.json(result["summary"])
        except Exception as exc:
            st.error(str(exc))

    st.divider()
    st.subheader("Run history")
    runs = db.list_optimization_runs(conn, limit=10)
    if not runs:
        st.caption("(no runs yet)")
    for run in runs:
        with st.expander(f"Run {run['id']} · {run['input_profile_name']} · {run['status']} · {run['created_at']}"):
            st.json(run.get("config") or {})
            if run.get("summary"):
                st.json(run["summary"])
            iters = db.fetch_iterations_for_run(conn, run["id"])
            for it in iters:
                st.markdown(
                    f"**Iter {it['iter_index']}** · profile `{it.get('profile_name')}` · "
                    f"accepted={it['accepted']}"
                )
                if it.get("metrics"):
                    m = it["metrics"]
                    st.caption(f"macro F1={m.get('macro_f1')} · micro F1={m.get('micro_f1')}")


def main() -> None:
    st.set_page_config(page_title="Claim extraction refinement", layout="wide")
    st.title("Claim extraction refinement lab")

    with st.sidebar:
        st.header("Settings")
        default_db = str(db.default_db_path())
        db_path = st.text_input("SQLite path", value=st.session_state.get("refine_db_path", default_db), key="refine_db_in")
        st.session_state["refine_db_path"] = db_path
        posts_default = str(REPO_ROOT / "data" / "posts_with_claims_full.json")
        posts_path = st.text_input(
            "Source posts JSON",
            value=st.session_state.get("refine_posts_path", posts_default),
            key="refine_posts_in",
        )
        st.session_state["refine_posts_path"] = posts_path
        st.number_input("Browse shuffle seed", value=42, step=1, key="browse_seed")
        if st.button("Clear posts cache"):
            _load_posts.clear()
            st.success("Posts cache cleared.")

        st.divider()
        st.markdown("**Prompt variables**")
        for k in prompt_vars.list_var_keys():
            st.caption(f"`{{{k}}}` — {prompt_vars.display_name(k)}")

    conn = _open_db(Path(db_path))
    posts, posts_err = _load_posts(posts_path)

    tab_browse, tab_profiles, tab_compare, tab_optimize = st.tabs(
        ["Browse", "Profiles", "Compare", "Optimize"]
    )
    with tab_browse:
        _render_browse_tab(conn, posts, posts_err)
    with tab_profiles:
        _render_profiles_tab(conn)
    with tab_compare:
        _render_compare_tab(conn)
    with tab_optimize:
        _render_optimize_tab(conn)

    conn.close()


if __name__ == "__main__":
    main()
