"""Auto prompt optimization: reference generation, LLM judge, keep-best loop."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from apps.claim_extractor.api_requester import (
    ConcurrentApiRequester,
    RequestStatus,
    RequestTask,
    RetryPolicy,
    ThrottlePolicy,
    default_is_retryable_exception,
)
from apps.claim_extractor.extraction_core import openai_structured_completion
from apps.claim_extractor.refinement_lab import db, extract_runner, metrics, posts_data
from apps.claim_extractor.refinement_lab.db import PromptProfile
from apps.claim_extractor.refinement_lab.meta_defaults import (
    DIAGNOSE_POST_SCHEMA,
    EVALUATE_SCHEMA,
    META_PROMPT_SPECS,
    PROPOSE_PROMPT_SCHEMA,
    SUMMARIZE_PROBLEMS_SCHEMA,
    render_meta_template,
    validate_meta_prompt,
)
from apps.claim_extractor.scoring_inputs import context_text_for_post_row

DEFAULT_MAX_ITERS = 3
DEFAULT_PATIENCE = 2
DEFAULT_MIN_F1_GAIN = 0.01
DEFAULT_MAX_WORKERS = 4


@dataclass(frozen=True)
class OptimizationConfig:
    expensive_model: str
    cheap_model: str
    max_iters: int = DEFAULT_MAX_ITERS
    patience: int = DEFAULT_PATIENCE
    min_f1_gain: float = DEFAULT_MIN_F1_GAIN
    max_workers: int = DEFAULT_MAX_WORKERS


def _objective(conn) -> str:
    text = db.get_meta_prompt(conn, "objective")
    if text and text.strip():
        return text.strip()
    from apps.claim_extractor.refinement_lab.meta_defaults import DEFAULT_OBJECTIVE

    return DEFAULT_OBJECTIVE.strip()


def _meta_template(conn, name: str) -> str:
    text = db.get_meta_prompt(conn, name)
    if text and text.strip():
        return text.strip()
    spec = META_PROMPT_SPECS.get(name)
    if spec is None:
        raise ValueError(f"Unknown meta-prompt: {name}")
    return str(spec["template"]).strip()


class _StructuredJudgeClient:
    """RequestClient adapter for per-post diagnose calls."""

    def __init__(self, *, model: str, conn) -> None:
        self._model = model
        self._conn = conn

    def perform(self, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        objective = _objective(self._conn)
        template = _meta_template(self._conn, "diagnose_post")
        user = render_meta_template(
            template,
            {
                "objective": objective,
                "post_text": str(payload["post_text"]),
                "reference_claims": json.dumps(payload["reference_claims"], ensure_ascii=False, indent=2),
                "candidate_claims": json.dumps(payload["candidate_claims"], ensure_ascii=False, indent=2),
            },
        )
        out = openai_structured_completion(
            model=self._model,
            system="You are an expert evaluator for vaccine claim extraction quality.",
            user=user,
            schema=DIAGNOSE_POST_SCHEMA,
        )
        return out, {}


def generate_reference_from_profile(
    conn,
    profile: PromptProfile,
    problem_posts: list[dict[str, Any]],
    *,
    model: str,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> tuple[int, int]:
    return extract_runner.run_profile_on_posts(
        conn,
        profile,
        problem_posts,
        model=model,
        on_progress=on_progress,
        write_reference=True,
    )


def _candidate_claims_for_profile(
    conn, profile_id: int, task_id: str, hit: dict[str, Any] | None
) -> list[dict[str, Any]]:
    if hit is None or hit.get("status") != "success":
        return []
    out = hit.get("output_json")
    if not isinstance(out, dict):
        return []
    claims = out.get("claims")
    return claims if isinstance(claims, list) else []


def judge_profile_against_reference(
    conn,
    profile: PromptProfile,
    problem_posts: list[dict[str, Any]],
    *,
    judge_model: str,
    on_progress: Callable[[int, int, str], None] | None = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> dict[str, Any]:
    """Per-post LLM judge; persist evaluations. Returns aggregate metrics."""
    extractions = db.fetch_extractions_for_profile(conn, profile.id)
    client = _StructuredJudgeClient(model=judge_model, conn=conn)
    throttle = ThrottlePolicy(target_requests_per_minute=60, global_429_cooldown_s=15.0)
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=max_workers,
        retry_policy=RetryPolicy(max_retries=2),
        throttle_policy=throttle,
        is_retryable=default_is_retryable_exception,
    )

    tasks: list[RequestTask] = []
    for pp in problem_posts:
        tid = str(pp["task_id"])
        ref = db.get_reference_claims(conn, tid)
        ref_claims = ref.claims if ref else []
        hit = extractions.get(tid)
        cand = _candidate_claims_for_profile(conn, profile.id, tid, hit)
        tasks.append(
            RequestTask(
                task_id=tid,
                payload={
                    "post_text": context_text_for_post_row(pp["post_row"]),
                    "reference_claims": ref_claims,
                    "candidate_claims": cand,
                },
            )
        )

    per_post: list[dict[str, Any]] = []
    done = 0
    for result in requester.run(tasks):
        done += 1
        if on_progress:
            on_progress(done, len(tasks), result.task_id)
        if result.status != RequestStatus.SUCCESS or result.output is None:
            continue
        alignment = result.output
        prf = metrics.prf_from_alignment(alignment)
        db.upsert_evaluation(
            conn,
            profile_id=profile.id,
            task_id=result.task_id,
            alignment=alignment,
            precision=float(prf["precision"] or 0),
            recall=float(prf["recall"] or 0),
            f1=float(prf["f1"] or 0),
            judged_model=judge_model,
        )
        per_post.append(
            {
                "task_id": result.task_id,
                "alignment": alignment,
                "precision": prf["precision"],
                "recall": prf["recall"],
                "f1": prf["f1"],
            }
        )

    agg = metrics.aggregate_per_post(per_post)
    return {"per_post": per_post, "aggregate": agg}


def _summarize_problems(conn, *, judge_model: str, issue_notes: list[dict[str, Any]], profile: PromptProfile) -> dict[str, Any]:
    template = _meta_template(conn, "summarize_problems")
    user = render_meta_template(
        template,
        {
            "objective": _objective(conn),
            "issue_notes": json.dumps(issue_notes, ensure_ascii=False, indent=2),
            "current_system_prompt": profile.system_prompt,
        },
    )
    return openai_structured_completion(
        model=judge_model,
        system="You analyze patterns in claim extraction errors.",
        user=user,
        schema=SUMMARIZE_PROBLEMS_SCHEMA,
    )


def _propose_prompt(
    conn,
    *,
    judge_model: str,
    profile: PromptProfile,
    problems: list[dict[str, Any]],
) -> dict[str, Any]:
    template = _meta_template(conn, "propose_prompt")
    constraints = (
        "Keep {text_input} and {max_claims} placeholders. "
        "Make minimal edits. Do not remove the JSON output format section."
    )
    user = render_meta_template(
        template,
        {
            "objective": _objective(conn),
            "current_system_prompt": profile.system_prompt,
            "current_user_prompt": profile.user_prompt,
            "problems": json.dumps(problems, ensure_ascii=False, indent=2),
            "constraints": constraints,
        },
    )
    return openai_structured_completion(
        model=judge_model,
        system="You are an expert prompt engineer for structured claim extraction.",
        user=user,
        schema=PROPOSE_PROMPT_SCHEMA,
    )


def _evaluate_iteration(
    conn,
    *,
    judge_model: str,
    problems: list[dict[str, Any]],
    metrics_before: dict[str, Any],
    metrics_after: dict[str, Any],
    diff_examples: list[dict[str, Any]],
) -> dict[str, Any]:
    template = _meta_template(conn, "evaluate")
    user = render_meta_template(
        template,
        {
            "objective": _objective(conn),
            "targeted_problems": json.dumps(problems, ensure_ascii=False, indent=2),
            "metrics_before": json.dumps(metrics_before, ensure_ascii=False, indent=2),
            "metrics_after": json.dumps(metrics_after, ensure_ascii=False, indent=2),
            "diff_examples": json.dumps(diff_examples, ensure_ascii=False, indent=2),
        },
    )
    return openai_structured_completion(
        model=judge_model,
        system="You evaluate whether a prompt change improved claim extraction.",
        user=user,
        schema=EVALUATE_SCHEMA,
    )


def _collect_issue_notes(conn, profile_id: int, problem_posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    evals = db.fetch_evaluations_for_profile(conn, profile_id)
    notes: list[dict[str, Any]] = []
    for pp in problem_posts:
        tid = str(pp["task_id"])
        ev = evals.get(tid)
        if not ev or not ev.get("alignment"):
            continue
        al = ev["alignment"]
        notes.append(
            {
                "task_id": tid,
                "f1": ev.get("f1"),
                "issue_tags": al.get("issue_tags") or [],
                "missed": al.get("missed") or [],
                "extra": al.get("extra") or [],
            }
        )
    return notes


def _macro_f1(agg: dict[str, Any]) -> float:
    v = agg.get("macro_f1")
    return float(v) if v is not None else 0.0


def run_optimization(
    conn,
    input_profile: PromptProfile,
    problem_posts: list[dict[str, Any]],
    config: OptimizationConfig,
    *,
    on_progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Autonomous keep-best optimization loop."""
    if not problem_posts:
        return {"run_id": None, "profiles": [], "message": "No problem posts."}

    missing_ref = [
        pp["task_id"]
        for pp in problem_posts
        if not db.get_reference_claims(conn, str(pp["task_id"]))
    ]
    if missing_ref:
        raise RuntimeError(
            f"Reference claims missing for {len(missing_ref)} post(s). "
            "Generate Reference on the Optimize tab first."
        )

    run_id = db.create_optimization_run(
        conn,
        input_profile_id=input_profile.id,
        config={
            "expensive_model": config.expensive_model,
            "cheap_model": config.cheap_model,
            "max_iters": config.max_iters,
            "patience": config.patience,
            "min_f1_gain": config.min_f1_gain,
        },
    )

    def prog(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    created_profiles: list[int] = []
    best_profile = input_profile
    best_f1 = 0.0
    no_improve = 0

    try:
        prog("Running cheap extraction on input profile…")
        extract_runner.run_profile_on_posts(
            conn,
            input_profile,
            problem_posts,
            model=config.cheap_model,
            max_workers=config.max_workers,
        )
        prog("Judging input profile vs Reference…")
        judge_profile_against_reference(
            conn,
            input_profile,
            problem_posts,
            judge_model=config.expensive_model,
            max_workers=config.max_workers,
        )
        evals = db.fetch_evaluations_for_profile(conn, input_profile.id)
        per_post = [
            {
                "task_id": tid,
                "alignment": ev.get("alignment"),
                "precision": ev.get("precision"),
                "recall": ev.get("recall"),
                "f1": ev.get("f1"),
            }
            for tid, ev in evals.items()
        ]
        baseline_agg = metrics.aggregate_per_post(per_post)
        best_f1 = _macro_f1(baseline_agg)

        issue_notes = _collect_issue_notes(conn, input_profile.id, problem_posts)
        db.add_profile_note(
            conn,
            profile_id=input_profile.id,
            kind="problems",
            content=json.dumps(issue_notes, ensure_ascii=False, indent=2),
            run_id=run_id,
        )

        current_profile = input_profile
        metrics_before = baseline_agg

        for iteration in range(1, config.max_iters + 1):
            prog(f"Iteration {iteration}/{config.max_iters}: summarizing problems…")
            summary = _summarize_problems(
                conn,
                judge_model=config.expensive_model,
                issue_notes=issue_notes,
                profile=current_profile,
            )
            problems = summary.get("problems") or []
            if not problems:
                prog("No systemic problems found; stopping.")
                break

            prog(f"Iteration {iteration}: proposing prompt edits…")
            proposal = _propose_prompt(
                conn,
                judge_model=config.expensive_model,
                profile=current_profile,
                problems=problems if isinstance(problems, list) else [],
            )
            new_name = f"{input_profile.name}_opt{iteration}"
            new_id = db.create_profile(
                conn,
                name=new_name,
                system_prompt=str(proposal.get("system_prompt") or current_profile.system_prompt),
                user_prompt=str(proposal.get("user_prompt") or current_profile.user_prompt),
                model=config.cheap_model,
                max_claims=current_profile.max_claims,
            )
            created_profiles.append(new_id)
            new_profile = db.get_profile(conn, new_id)
            if new_profile is None:
                continue

            db.add_profile_note(
                conn,
                profile_id=new_id,
                kind="solutions",
                content=json.dumps(proposal.get("changes") or [], ensure_ascii=False, indent=2),
                run_id=run_id,
            )

            prog(f"Iteration {iteration}: running cheap extraction on new profile…")
            extract_runner.run_profile_on_posts(
                conn,
                new_profile,
                problem_posts,
                model=config.cheap_model,
                max_workers=config.max_workers,
            )
            prog(f"Iteration {iteration}: judging new profile…")
            judge_profile_against_reference(
                conn,
                new_profile,
                problem_posts,
                judge_model=config.expensive_model,
                max_workers=config.max_workers,
            )
            new_evals = db.fetch_evaluations_for_profile(conn, new_id)
            new_per_post = [
                {
                    "task_id": tid,
                    "alignment": ev.get("alignment"),
                    "precision": ev.get("precision"),
                    "recall": ev.get("recall"),
                    "f1": ev.get("f1"),
                }
                for tid, ev in new_evals.items()
            ]
            metrics_after = metrics.aggregate_per_post(new_per_post)

            diff_examples = []
            for pp in problem_posts[:3]:
                tid = str(pp["task_id"])
                ev = new_evals.get(tid)
                if ev and ev.get("alignment"):
                    diff_examples.append({"task_id": tid, "alignment": ev["alignment"]})

            eval_result = _evaluate_iteration(
                conn,
                judge_model=config.expensive_model,
                problems=problems if isinstance(problems, list) else [],
                metrics_before=metrics_before,
                metrics_after=metrics_after,
                diff_examples=diff_examples,
            )
            db.add_profile_note(
                conn,
                profile_id=new_id,
                kind="evaluation",
                content=json.dumps(eval_result, ensure_ascii=False, indent=2),
                run_id=run_id,
            )

            new_f1 = _macro_f1(metrics_after)
            accepted = new_f1 >= best_f1 + config.min_f1_gain
            db.add_optimization_iteration(
                conn,
                run_id=run_id,
                iter_index=iteration,
                profile_id=new_id,
                metrics=metrics_after,
                diagnosis=summary,
                proposed_changes=proposal,
                accepted=accepted,
                notes=str(eval_result.get("summary") or ""),
            )

            if accepted:
                best_f1 = new_f1
                best_profile = new_profile
                current_profile = new_profile
                issue_notes = _collect_issue_notes(conn, new_id, problem_posts)
                metrics_before = metrics_after
                no_improve = 0
                prog(f"Iteration {iteration} accepted (macro F1={new_f1:.3f}).")
            else:
                no_improve += 1
                prog(f"Iteration {iteration} rejected (macro F1={new_f1:.3f}, best={best_f1:.3f}).")
                if no_improve >= config.patience:
                    prog("Early stop: no improvement within patience.")
                    break

        summary = {
            "best_profile_id": best_profile.id,
            "best_profile_name": best_profile.name,
            "best_macro_f1": best_f1,
            "created_profile_ids": created_profiles,
        }
        db.update_optimization_run(conn, run_id, status="complete", summary=summary)
        return {"run_id": run_id, "profiles": created_profiles, "summary": summary}

    except Exception as exc:
        db.update_optimization_run(conn, run_id, status="failed", summary={"error": str(exc)})
        raise
