"""Run claims-only extraction for a prompt profile on problem posts."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from nlp.claim_extraction.api_requester import (
    ConcurrentApiRequester,
    RequestStatus,
    RequestTask,
    RetryPolicy,
    ThrottlePolicy,
    default_is_retryable_exception,
)
from nlp.claim_extraction.clients import build_azure_claims_client as build_claims_client
from apps.prompt_refinement import db, prompt_vars
from apps.prompt_refinement.db import PromptProfile

DEFAULT_TARGET_RPM = 90
DEFAULT_429_COOLDOWN_S = 20.0
DEFAULT_MAX_WORKERS = 4
DEFAULT_MAX_RETRIES = 3

def run_profile_on_posts(
    conn,
    profile: PromptProfile,
    problem_posts: list[dict[str, Any]],
    *,
    model: str | None = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    on_progress: Callable[[int, int, str], None] | None = None,
    write_reference: bool = False,
    run_label: str = "1",
) -> tuple[int, int, list[dict[str, str]]]:
    """
    Extract claims for each problem post using ``profile`` prompts.

    Results are stored under ``run_label`` (does not overwrite other labels).
    Returns (success_count, failure_count, failure_details).
    """
    if not problem_posts:
        return 0, 0, []

    profile_id = profile.id
    run_model = model or profile.model
    max_claims = profile.max_claims
    label = str(run_label or "1").strip() or "1"

    def system_builder(payload: dict[str, Any]) -> str:
        return str(payload["system_prompt"])

    def user_builder(payload: dict[str, Any]) -> str:
        return str(payload["user_prompt"])

    client = build_claims_client(
        model=run_model,
        claims_only=True,
        system_prompt_builder=system_builder,
        user_prompt_builder=user_builder,
    )
    throttle = ThrottlePolicy(
        target_requests_per_minute=max(1, int(os.getenv("CLAIMS_TARGET_RPM", str(DEFAULT_TARGET_RPM)))),
        global_429_cooldown_s=max(0.0, float(os.getenv("CLAIMS_429_COOLDOWN_S", str(DEFAULT_429_COOLDOWN_S)))),
    )
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=max_workers,
        retry_policy=RetryPolicy(max_retries=DEFAULT_MAX_RETRIES),
        throttle_policy=throttle,
        is_retryable=default_is_retryable_exception,
    )

    tasks: list[RequestTask] = []
    for i, pp in enumerate(problem_posts):
        post_row = pp["post_row"]
        task_id = str(pp["task_id"])
        system, user = prompt_vars.render_profile_prompts(
            system_prompt=profile.system_prompt,
            user_prompt=profile.user_prompt,
            post_row=post_row,
            max_claims=max_claims,
        )
        tasks.append(
            RequestTask(
                task_id=task_id,
                payload={
                    "system_prompt": system,
                    "user_prompt": user,
                    "max_claims": max_claims,
                },
            )
        )
        if on_progress:
            on_progress(i, len(problem_posts), f"Prepared {task_id}")

    success = 0
    failed = 0
    failures: list[dict[str, str]] = []
    done = 0
    for result in requester.run(tasks):
        done += 1
        if on_progress:
            on_progress(done, len(problem_posts), result.task_id)
        if result.status == RequestStatus.SUCCESS and result.output is not None:
            if write_reference:
                claims = result.output.get("claims") if isinstance(result.output, dict) else []
                if not isinstance(claims, list):
                    claims = []
                db.upsert_reference_claims(
                    conn,
                    task_id=result.task_id,
                    claims=claims,
                    source="generated",
                    generated_from_profile_id=profile_id,
                    generated_model=run_model,
                )
            else:
                db.upsert_profile_extraction(
                    conn,
                    profile_id=profile_id,
                    task_id=result.task_id,
                    status="success",
                    output_json=result.output,
                    error=None,
                    model=run_model,
                    run_label=label,
                )
            success += 1
        else:
            err = result.error or "unknown error"
            failures.append({"task_id": result.task_id, "error": err})
            if not write_reference:
                db.upsert_profile_extraction(
                    conn,
                    profile_id=profile_id,
                    task_id=result.task_id,
                    status="failed",
                    output_json=None,
                    error=err,
                    model=run_model,
                    run_label=label,
                )
            failed += 1
    return success, failed, failures
