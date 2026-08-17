"""
Prompt Lab — iterate claim-extraction prompts on a curated problem-post set.

From the **repository root**::

  python -m apps.prompt_refinement
"""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from nlp.claim_extraction.clients import (
    check_azure_connectivity as check_llm_connectivity,
    load_azure_config as load_llm_config,
)
from apps.prompt_refinement import db, extract_runner, optimizer, posts_data, prompt_vars
from apps.prompt_refinement.meta_defaults import META_PROMPT_SPECS, validate_meta_prompt
from apps.prompt_refinement.models import DEFAULT_MODEL, SEED_MODELS

_PREVIEW_MAX_HEIGHT_PX = 420
_PREVIEW_MIN_HEIGHT_PX = 68


def _text_area_height(
    text: str,
    *,
    min_h: int = _PREVIEW_MIN_HEIGHT_PX,
    max_h: int = _PREVIEW_MAX_HEIGHT_PX,
    px_per_line: int = 20,
) -> int:
    raw_lines = text.splitlines() or [""]
    visual_lines = sum(max(1, (len(line) + 89) // 90) for line in raw_lines)
    return min(max_h, max(min_h, 16 + visual_lines * px_per_line))

def _show_post_text(text: str, *, key: str) -> None:
    """Read-only text box sized to content, capped with scroll for very long posts."""
    if not text.strip():
        st.caption("(empty post text)")
        return
    st.text_area(
        "post",
        value=text,
        height=_text_area_height(text),
        disabled=True,
        key=key,
        label_visibility="collapsed",
    )

def _preview_text_area(title: str, text: str, *, key: str) -> None:
    st.markdown(f"**{title}**")
    if not text.strip():
        st.caption("(empty)")
        return
    st.text_area(
        title,
        value=text,
        height=_text_area_height(text),
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

def _lab_db_path() -> Path:
    return Path(st.session_state["refine_db_in"])

def _frag_rerun() -> None:
    st.rerun(scope="fragment")

def _app_rerun() -> None:
    st.rerun(scope="app")

def _bump_lab_data() -> None:
    st.session_state["lab_data_revision"] = st.session_state.get("lab_data_revision", 0) + 1

def _show_run_failures(failures: list[dict[str, str]], *, title: str = "Errors") -> None:
    if not failures:
        return
    lines = [f"{item['task_id']}: {item['error']}" for item in failures]
    if len(lines) <= 8:
        for line in lines:
            st.error(line)
        return
    with st.expander(f"{title} ({len(lines)} posts)", expanded=True):
        st.code("\n".join(lines))

def _ensure_selectbox_key(key: str, options: list[str], *, preferred: str | None = None) -> None:
    if not options:
        return
    if st.session_state.get(key) not in options:
        st.session_state[key] = preferred if preferred in options else options[0]

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

def _render_source_data_tab_content(conn, posts: list, posts_err: str | None) -> None:
    st.caption("Review source posts and mark extraction problems or skip.")

    st.text_input("SQLite path", key="refine_db_in")
    st.text_input("Source posts JSON", key="refine_posts_in")

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

    shuffle_key = "source_data_shuffled_queue"
    if st.button("Shuffle", key="source_data_shuffle", disabled=len(queue) < 2):
        shuffled = list(queue)
        random.shuffle(shuffled)
        st.session_state[shuffle_key] = shuffled
        _frag_rerun()

    stored = st.session_state.get(shuffle_key)
    if stored:
        stored_ids = {tid for _, tid in stored}
        current_ids = {tid for _, tid in queue}
        if stored_ids == current_ids:
            queue = stored
        else:
            del st.session_state[shuffle_key]

    if not enabled_status:
        st.warning("Enable at least one status filter.")
        return
    if all_platforms and not enabled_platforms:
        st.warning("Enable at least one platform in **Platform filter**.")
        return

    st.write(f"**{len(queue)}** post(s) in queue")
    if not queue:
        st.info("No posts match filters (or all reviewed).")
        return

    post_row, tid = queue[0]
    _render_post_header(post_row, tid)
    ctx = posts_data.post_text(post_row)
    _preview_text_area("Post text", ctx, key=f"browse_txt_{tid}")

    if posts_data.extraction_status(post_row) == "success":
        st.markdown("**Current extraction (source JSON)**")
        _render_claim_list(posts_data.baseline_claims_from_post(post_row), key_prefix=f"browse_{tid}")

    comment = st.text_area("Comment (problem description)", key=f"browse_comment_{tid}", height=100)
    c_mark, c_skip, _ = st.columns([2, 1, 8])
    with c_mark:
        if st.button("Mark as problem", key=f"browse_mark_{tid}"):
            note = (comment or "").strip()
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
            _frag_rerun()
    with c_skip:
        if st.button("Skip", key=f"browse_skip_{tid}"):
            db.add_reviewed_skip(conn, tid)
            _frag_rerun()

@st.fragment
def _render_source_data_tab() -> None:
    conn = _open_db(_lab_db_path())
    try:
        posts, posts_err = _load_posts(st.session_state["refine_posts_in"])
        _render_source_data_tab_content(conn, posts, posts_err)
    finally:
        conn.close()

@st.fragment
def _render_profiles_tab() -> None:
    conn = _open_db(_lab_db_path())
    try:
        _render_profiles_tab_content(conn)
    finally:
        conn.close()

def _render_profiles_tab_content(conn) -> None:
    profiles = db.list_profiles(conn)
    st.caption("Prompt profiles run **claims-only** extraction on the problem post set only.")

    flash_run = st.session_state.pop("flash_prof_run", None)
    if flash_run:
        ok = int(flash_run["ok"])
        fail = int(flash_run["fail"])
        st.success(f"Extraction finished: **{ok}** ok, **{fail}** failed.")
        if fail:
            _show_run_failures(flash_run.get("failures") or [], title="Extraction errors")
            st.info("Open the Browse tab to see per-post extraction errors.")

    with st.expander("Variable bank", expanded=False):
        for k in prompt_vars.list_var_keys():
            st.text(f"{{{k}}} — {prompt_vars.display_name(k)}")

    c_new, _ = st.columns([1, 3])
    with c_new:
        new_name = st.text_input("New profile name", key="new_profile_name", placeholder="e.g. epidemiologist_v2")
        if st.button("Create profile", key="prof_create"):
            if not new_name.strip():
                st.error("Name required.")
            else:
                pid = db.create_profile_from_latest(conn, new_name.strip())
                st.success(f"Created profile id={pid}")
                st.session_state["edit_profile_id"] = pid
                created = db.get_profile(conn, pid)
                if created is not None:
                    st.session_state["edit_profile_select"] = f"{pid}: {created.name}"
                _frag_rerun()

    if not profiles:
        st.info("Create a profile to edit prompts and run extraction.")
        return

    profile_options = {f"{p.id}: {p.name}": p.id for p in profiles}
    option_keys = list(profile_options.keys())
    preferred_key = next(
        (k for k, v in profile_options.items() if v == st.session_state.get("edit_profile_id", profiles[0].id)),
        option_keys[0],
    )
    _ensure_selectbox_key("edit_profile_select", option_keys, preferred=preferred_key)

    st.selectbox("Edit profile", options=option_keys, key="edit_profile_select")
    profile_id = profile_options[st.session_state["edit_profile_select"]]
    st.session_state["edit_profile_id"] = profile_id
    profile = db.get_profile(conn, profile_id)
    if profile is None:
        st.error("Profile not found.")
        return

    d1, d2 = st.columns([2, 1])
    with d1:
        dup_name = st.text_input(
            "Duplicate as",
            key=f"dup_name_{profile_id}",
            placeholder="e.g. next_prompt",
        )
    with d2:
        st.write("")
        st.write("")
        if st.button("Duplicate profile", key=f"prof_dup_{profile_id}"):
            if not dup_name.strip():
                st.error("Name required.")
            else:
                new_id = db.duplicate_profile(conn, profile_id, new_name=dup_name.strip())
                st.success(f"Duplicated → id={new_id}")
                st.session_state["edit_profile_id"] = new_id
                _frag_rerun()

    name = st.text_input("Name", value=profile.name, key=f"prof_name_{profile_id}")
    model_options = list(SEED_MODELS)
    if profile.model not in model_options:
        model_options = [profile.model, *model_options]
    _ensure_selectbox_key(f"prof_model_{profile_id}", model_options)
    model_sel = st.selectbox("Model", options=model_options, key=f"prof_model_{profile_id}")
    custom_model = st.text_input("Or custom model deployment", value="", key=f"prof_model_custom_{profile_id}")
    model = custom_model.strip() or model_sel
    max_claims = st.number_input("Max claims", min_value=1, max_value=20, value=profile.max_claims, key=f"prof_mc_{profile_id}")

    prompts_root = REPO_ROOT / "nlp" / "claim_extraction" / "prompts"
    load_c1, load_c2, load_c3 = st.columns(3)
    with load_c1:
        if st.button("Load current from nlp prompts", key=f"load_cur_{profile_id}"):
            system_l, user_l = db.load_prompts_from_files(
                system_path=prompts_root / "extract_system.txt",
                user_path=prompts_root / "extract_user.txt",
            )
            db.update_profile(
                conn,
                profile_id,
                name=name,
                system_prompt=system_l,
                user_prompt=user_l,
                model=model,
                max_claims=int(max_claims),
            )
            st.success("Loaded current extract_*.txt")
            _frag_rerun()
    with load_c2:
        if st.button("Load next from candidates/", key=f"load_next_{profile_id}"):
            system_l, user_l = db.load_prompts_from_files(
                system_path=prompts_root / "candidates" / "next_system.txt",
                user_path=prompts_root / "candidates" / "next_user.txt",
            )
            db.update_profile(
                conn,
                profile_id,
                name=name,
                system_prompt=system_l,
                user_prompt=user_l,
                model=model,
                max_claims=int(max_claims),
            )
            st.success("Loaded candidates/next_*.txt")
            _frag_rerun()
    with load_c3:
        st.caption("Placeholders `{{…}}` → `{…}` on load.")

    profile = db.get_profile(conn, profile_id) or profile
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
        _frag_rerun()

    notes = db.fetch_profile_notes(conn, profile_id)
    if notes:
        with st.expander("Profile notes (optimizer)", expanded=False):
            for n in notes:
                st.markdown(f"**{n['kind']}** · {n['created_at']}")
                st.text(n["content"][:4000])

    problem_posts = db.fetch_problem_posts_sorted(conn, descending=False)
    st.write(f"Problem posts to extract: **{len(problem_posts)}**")
    if not problem_posts:
        st.warning("Mark problem posts first on the Source Data tab (or import-sample).")
        return

    existing_labels = db.list_run_labels_for_profile(conn, profile_id)
    suggested = db.next_run_label(conn, profile_id)
    st.caption(
        f"Existing run labels for this profile: "
        f"{', '.join(existing_labels) if existing_labels else '(none)'}"
    )
    run_label = st.text_input(
        "Run label",
        value=suggested,
        key=f"prof_run_label_{profile_id}",
        help="Stores this extract under a separate label (e.g. 1 and 2). "
        "Reusing a label overwrites that label only.",
    )

    test_col, run_col = st.columns([1, 1])
    with test_col:
        if st.button("Test Azure connection", key=f"prof_test_azure_{profile_id}"):
            with st.spinner("Testing Azure OpenAI connection…"):
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
        label = (run_label or "").strip() or suggested
        try:
            load_llm_config()
        except RuntimeError as exc:
            st.error(f"Azure OpenAI not configured: {exc}")
            return

        profile = db.get_profile(conn, profile_id)
        if profile is None:
            st.error("Profile not found.")
            return
        with st.status(
            f"Running profile **{profile.name}** label=**{label}** "
            f"on {len(problem_posts)} post(s)…",
            expanded=True,
        ) as status:
            progress = st.progress(0.0)
            state = {"done": 0, "total": len(problem_posts)}

            def on_progress(done: int, total: int, msg: str) -> None:
                state["done"] = done
                state["total"] = max(total, 1)
                progress.progress(min(1.0, done / state["total"]), text=f"{msg} ({done}/{total})")

            try:
                ok, fail, failures = extract_runner.run_profile_on_posts(
                    conn,
                    profile,
                    problem_posts,
                    on_progress=on_progress,
                    run_label=label,
                )
                progress.empty()
                status.update(
                    label=f"Done — {ok} succeeded, {fail} failed (label={label})",
                    state="complete" if fail == 0 else "error",
                )
                st.session_state["flash_prof_run"] = {
                    "ok": ok,
                    "fail": fail,
                    "failures": failures,
                    "run_label": label,
                }
                _bump_lab_data()
                _app_rerun()
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
    st.markdown("**Reference**")
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
                    _frag_rerun()
                except IndexError as e:
                    st.error(str(e))
        with c3:
            if st.button("Del", key=f"ref_del_{block_idx}_{tid}_{i}"):
                try:
                    db.delete_reference_claim(conn, tid, i)
                    _frag_rerun()
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
            _frag_rerun()

def _render_compare_extraction_tab(
    *,
    conn,
    tid: str,
    prof: db.PromptProfile,
    hit: dict | None,
    run_label: str | None = None,
) -> None:
    label_bit = f" · run `{run_label}`" if run_label else ""
    st.caption(f"Model: `{prof.model}` · max_claims={prof.max_claims}{label_bit}")
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
    _render_claim_list(claims, key_prefix=f"prof_{prof.id}_{tid}_{run_label or 'x'}")


def _render_compare_post_block(
    conn,
    pp: dict,
    *,
    selected_snapshots: list,
    block_idx: int,
) -> None:
    post_row = pp["post_row"]
    tid = pp["task_id"]

    with st.container(border=True):
        _render_post_header(
            post_row,
            tid,
            extra=f"marked **{pp['source']}** · baseline **{pp['baseline_status']}**",
        )
        if pp.get("comment"):
            st.markdown(f"**Comment:** {pp['comment']}")
        if pp.get("created_at"):
            st.caption(f"Added {pp['created_at']}")

        ctx = posts_data.post_text(post_row)
        _show_post_text(ctx, key=f"compare_post_{block_idx}_{tid}")

        if st.button("Remove from problem set", key=f"browse_remove_{block_idx}_{tid}"):
            db.delete_problem_post(conn, tid)
            _frag_rerun()

        st.divider()

        if not selected_snapshots:
            st.info("Select up to 4 extract snapshots above to compare side-by-side.")
            with st.expander("Reference claims", expanded=False):
                _render_reference_tab(conn, tid=tid, block_idx=block_idx)
            return

        cols = st.columns(len(selected_snapshots))
        for col, snap in zip(cols, selected_snapshots):
            with col:
                st.markdown(f"**{snap['key']}**")
                prof = db.get_profile(conn, int(snap["profile_id"]))
                if prof is None:
                    st.warning("Profile missing.")
                    continue
                hit = db.get_extraction(
                    conn,
                    profile_id=int(snap["profile_id"]),
                    task_id=tid,
                    run_label=str(snap["run_label"]),
                )
                _render_compare_extraction_tab(
                    conn=conn,
                    tid=tid,
                    prof=prof,
                    hit=hit,
                    run_label=str(snap["run_label"]),
                )

        with st.expander("Edit reference claims", expanded=False):
            _render_reference_tab(conn, tid=tid, block_idx=block_idx)


@st.fragment
def _render_compare_tab() -> None:
    conn = _open_db(_lab_db_path())
    try:
        _render_compare_tab_content(conn)
    finally:
        conn.close()


def _render_compare_tab_content(conn) -> None:
    st.markdown(
        """
        <style>
        div[data-testid="stVerticalBlockBorderWrapper"] {
            padding: 1rem 1.1rem 1.15rem;
            margin-bottom: 0.25rem;
            background-color: rgba(151, 166, 195, 0.09);
            border-radius: 0.65rem;
            box-shadow: 0 1px 2px rgba(49, 51, 63, 0.08);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    problem_posts = db.fetch_problem_posts_sorted(conn, descending=True)
    if not problem_posts:
        st.info("No problem posts yet. Import an eval sample or mark some on Source Data.")
        return

    snapshots = db.list_extraction_snapshots(conn)
    snap_keys = [s["key"] for s in snapshots]
    st.caption(
        f"**{len(problem_posts)}** problem post(s) · **{len(snapshots)}** extract snapshot(s)"
    )
    if not snap_keys:
        st.warning(
            "No extractions yet. Run a profile on the Profiles tab (use run labels 1 and 2)."
        )
    # Streamlit multiselect has no max_selections on older versions; clamp manually.
    selected_keys = st.multiselect(
        "Compare snapshots (max 4)",
        options=snap_keys,
        default=snap_keys[: min(4, len(snap_keys))],
        key="browse_snapshot_multiselect",
        help="e.g. current / 1, current / 2, next / 1, next / 2",
    )
    if len(selected_keys) > 4:
        st.warning("Showing first 4 selected snapshots.")
        selected_keys = selected_keys[:4]
    selected_snapshots = [s for s in snapshots if s["key"] in selected_keys][:4]

    for i, pp in enumerate(problem_posts):
        _render_compare_post_block(
            conn,
            pp,
            selected_snapshots=selected_snapshots,
            block_idx=i,
        )
        if i < len(problem_posts) - 1:
            st.write("")


def _render_optimize_tab_content(conn) -> None:
    st.caption("Configure objective, generate Reference claims, and run the autonomous optimizer.")

    problem_posts = db.fetch_problem_posts_sorted(conn, descending=False)
    profiles = db.list_profiles(conn)
    if not problem_posts:
        st.warning("Mark problem posts on the Source Data tab first.")
        return

    st.subheader("Objective")
    obj = db.get_meta_prompt(conn, "objective") or ""
    new_obj = st.text_area("Optimization objective", value=obj, height=120, key="opt_objective")
    if st.button("Save objective", key="opt_save_obj"):
        db.upsert_meta_prompt(conn, "objective", new_obj)
        st.success("Saved.")
        _frag_rerun()

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
                    _frag_rerun()
                except ValueError as e:
                    st.error(str(e))

    st.divider()
    st.subheader("Reference setup")
    flash_ref = st.session_state.pop("flash_opt_ref_gen", None)
    if flash_ref:
        ok = int(flash_ref["ok"])
        fail = int(flash_ref["fail"])
        if fail:
            st.warning(f"Reference updated for **{ok}** posts; **{fail}** failed.")
            _show_run_failures(flash_ref.get("failures") or [], title="Reference generation errors")
        else:
            st.success(f"Reference updated: **{ok}** posts.")
    st.warning(
        "Generating Reference **overwrites** all Reference claims for every problem post, "
        "including manual edits."
    )
    if not profiles:
        st.info("Create a profile first.")
    else:
        prof_map = {f"{p.id}: {p.name}": p for p in profiles}
        prof_keys = list(prof_map.keys())
        _ensure_selectbox_key("opt_ref_prof", prof_keys)
        ref_prof_key = st.selectbox("Profile (prompts only)", options=prof_keys, key="opt_ref_prof")
        ref_prof = prof_map[ref_prof_key]
        ref_model = st.text_input("Expensive model for Reference", value=ref_prof.model, key="opt_ref_model")
        if st.button("Generate Reference from profile", key="opt_gen_ref"):
            try:
                load_llm_config()
                with st.status("Generating Reference…", expanded=True) as status:
                    prog = st.progress(0.0)

                    def on_progress(done: int, total: int, msg: str) -> None:
                        prog.progress(min(1.0, done / max(total, 1)), text=f"{msg} ({done}/{total})")

                    ok, fail, failures = extract_runner.run_profile_on_posts(
                        conn,
                        ref_prof,
                        problem_posts,
                        model=ref_model.strip() or ref_prof.model,
                        on_progress=on_progress,
                        write_reference=True,
                    )
                    status.update(
                        label=f"Done — {ok} ok, {fail} failed",
                        state="complete" if fail == 0 else "error",
                    )
                st.session_state["flash_opt_ref_gen"] = {
                    "ok": ok,
                    "fail": fail,
                    "failures": failures,
                }
                _bump_lab_data()
                _app_rerun()
            except Exception as exc:
                st.error(str(exc))

    st.divider()
    st.subheader("Improvement loop")
    flash_opt = st.session_state.pop("flash_opt_run", None)
    if flash_opt:
        st.success(f"Run complete (id={flash_opt.get('run_id')}).")
        if flash_opt.get("summary"):
            st.json(flash_opt["summary"])
    if not profiles:
        return
    inp_map = {f"{p.id}: {p.name}": p for p in profiles}
    inp_keys = list(inp_map.keys())
    _ensure_selectbox_key("opt_in_prof", inp_keys)
    inp_key = st.selectbox("Input profile", options=inp_keys, key="opt_in_prof")
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
            st.session_state["flash_opt_run"] = {
                "run_id": result.get("run_id"),
                "summary": result.get("summary"),
            }
            _bump_lab_data()
            _app_rerun()
        except Exception as exc:
            st.error(str(exc))

    st.divider()
    st.subheader("Run history")
    _RUN_STALE_SECONDS = 180
    runs = db.list_optimization_runs(conn, limit=10)
    if not runs:
        st.caption("(no runs yet)")
    for run in runs:
        status = run["status"]
        idle = run.get("idle_seconds")
        label_status = status
        if status == "running":
            if idle is not None and idle > _RUN_STALE_SECONDS:
                label_status = "running? (stale)"
            else:
                label_status = "running"
        with st.expander(f"Run {run['id']} · {run['input_profile_name']} · {label_status} · {run['created_at']}"):
            if status == "running":
                idle_txt = f"{int(idle)}s ago" if idle is not None else "unknown"
                if idle is not None and idle > _RUN_STALE_SECONDS:
                    st.warning(
                        f"No activity for **{idle_txt}** (last heartbeat). This run is likely "
                        "stopped (e.g. tab/server closed). If it were live, the heartbeat would "
                        "update every few seconds. Reopen the run's source or mark it stopped below."
                    )
                    if st.button("Mark as stopped", key=f"opt_mark_stopped_{run['id']}"):
                        db.update_optimization_run(conn, run["id"], status="interrupted")
                        _frag_rerun()
                else:
                    st.info(f"Active — last heartbeat **{idle_txt}**. Reopen this tab to refresh.")
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

@st.fragment
def _render_optimize_tab() -> None:
    conn = _open_db(_lab_db_path())
    try:
        _render_optimize_tab_content(conn)
    finally:
        conn.close()

def main() -> None:
    st.set_page_config(page_title="Claim extraction refinement", layout="wide")
    st.title("Prompt Lab")

    default_db = str(db.default_db_path())
    default_posts = str(REPO_ROOT / "data" / "posts_with_claims_full.json")
    st.session_state.setdefault("refine_db_in", default_db)
    st.session_state.setdefault("refine_posts_in", default_posts)

    tab_source, tab_profiles, tab_browse, tab_optimize = st.tabs(
        ["Source Data", "Profiles", "Browse", "Optimize"]
    )
    with tab_source:
        _render_source_data_tab()
    with tab_profiles:
        _render_profiles_tab()
    with tab_browse:
        _render_compare_tab()
    with tab_optimize:
        _render_optimize_tab()

if __name__ == "__main__":
    main()
