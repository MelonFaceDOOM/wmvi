from __future__ import annotations

import random
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Iterable, Optional, Protocol

from openai import AzureOpenAI, OpenAI
from openai._exceptions import APIConnectionError, APIStatusError, APITimeoutError, RateLimitError


class RequestStatus(str, Enum):
    SUCCESS = "success"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_TERMINAL = "failed_terminal"


@dataclass(frozen=True)
class RequestTask:
    task_id: str
    payload: dict[str, Any]


@dataclass
class RequestResult:
    task_id: str
    status: RequestStatus
    output: Optional[dict[str, Any]]
    error: Optional[str]
    attempts: int
    response_meta: dict[str, Any]


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 3
    min_backoff_s: float = 0.75
    max_backoff_s: float = 30.0
    jitter_s: float = 0.5
    parse_retry_cap: int = 1


@dataclass(frozen=True)
class ThrottlePolicy:
    target_requests_per_minute: int = 90
    global_429_cooldown_s: float = 15.0
    min_request_spacing_s: float = 0.0


class RequestClient(Protocol):
    def perform(self, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Execute one API request.

        Returns:
          (output_dict, meta_dict)
        """


def _parse_chat_completion_response(
    resp: Any,
    *,
    output_parser: Callable[[str], dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not getattr(resp, "choices", None):
        raise RuntimeError("Model response has no choices.")
    content = getattr(resp.choices[0].message, "content", None)
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("Model response content is empty.")

    usage = getattr(resp, "usage", None)
    meta = {
        "model": getattr(resp, "model", None),
        "usage": {
            "prompt_tokens": getattr(usage, "prompt_tokens", None) if usage is not None else None,
            "completion_tokens": getattr(usage, "completion_tokens", None) if usage is not None else None,
            "total_tokens": getattr(usage, "total_tokens", None) if usage is not None else None,
        },
    }
    return output_parser(content), meta


class AzureClaimsClient:
    def __init__(
        self,
        *,
        api_key: str,
        azure_endpoint: str,
        api_version: str,
        model: str,
        system_prompt_builder: Callable[[dict[str, Any]], str],
        user_prompt_builder: Callable[[dict[str, Any]], str],
        response_schema: dict[str, Any],
        output_parser: Callable[[str], dict[str, Any]],
    ) -> None:
        self._client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=azure_endpoint,
            api_version=api_version,
        )
        self._model = model
        self._system_prompt_builder = system_prompt_builder
        self._user_prompt_builder = user_prompt_builder
        self._response_schema = response_schema
        self._output_parser = output_parser

    def perform(self, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        system_prompt = self._system_prompt_builder(payload)
        user_prompt = self._user_prompt_builder(payload)
        resp = self._client.chat.completions.create(
            model=self._model,
            temperature=0,
            response_format={"type": "json_schema", "json_schema": self._response_schema},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return _parse_chat_completion_response(resp, output_parser=self._output_parser)


class OpenAIClaimsClient:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        system_prompt_builder: Callable[[dict[str, Any]], str],
        user_prompt_builder: Callable[[dict[str, Any]], str],
        response_schema: dict[str, Any],
        output_parser: Callable[[str], dict[str, Any]],
        base_url: str | None = None,
    ) -> None:
        kwargs: dict[str, Any] = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
        self._client = OpenAI(**kwargs)
        self._model = model
        self._system_prompt_builder = system_prompt_builder
        self._user_prompt_builder = user_prompt_builder
        self._response_schema = response_schema
        self._output_parser = output_parser

    def perform(self, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        system_prompt = self._system_prompt_builder(payload)
        user_prompt = self._user_prompt_builder(payload)
        resp = self._client.chat.completions.create(
            model=self._model,
            temperature=0,
            response_format={"type": "json_schema", "json_schema": self._response_schema},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return _parse_chat_completion_response(resp, output_parser=self._output_parser)


RETRYABLE_ERROR_MARKERS = (
    "apiconnectionerror",
    "apitimeouterror",
    "ratelimiterror",
    "connection error",
    "timeout",
    "too many requests",
    "too_many_requests",
)

TERMINAL_ERROR_MARKERS = (
    "content_filter",
    "content policy",
    "content policy violation",
    "responsible ai",
    "invalid claim_vaccine_alignment_score",
    "invalid author_claim_agreement_score",
    "invalid attribution_anecdote_score",
    "invalid attribution_authority_score",
    "invalid attribution_common_knowledge_score",
    "badrequesterror",
)


def _format_exception_details(exc: BaseException) -> str:
    parts: list[str] = [f"{type(exc).__name__}: {exc}"]
    status = getattr(exc, "status_code", None)
    if status is not None:
        parts.append(f"status={status}")
    req = getattr(exc, "request", None)
    if req is not None:
        method = getattr(req, "method", None)
        url = getattr(req, "url", None)
        if method or url:
            parts.append(f"request={method or '?'} {url or '?'}")
    return " | ".join(parts)


def _default_is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (RateLimitError, APITimeoutError, APIConnectionError)):
        return True
    if isinstance(exc, APIStatusError):
        status = getattr(exc, "status_code", None)
        return status is not None and 500 <= int(status) <= 599
    # parsing/validation issues can be retried once by policy guard
    if isinstance(exc, (ValueError, TypeError)):
        return True
    return False


def default_is_retryable_exception(exc: BaseException) -> bool:
    return _default_is_retryable(exc)


def classify_error_text(error_text: str) -> RequestStatus:
    e = (error_text or "").lower()
    if any(m in e for m in RETRYABLE_ERROR_MARKERS):
        return RequestStatus.FAILED_RETRYABLE
    if any(m in e for m in TERMINAL_ERROR_MARKERS):
        return RequestStatus.FAILED_TERMINAL
    return RequestStatus.FAILED_TERMINAL


class _GlobalThrottle:
    def __init__(
        self,
        *,
        policy: ThrottlePolicy,
        now_fn: Callable[[], float],
        sleep_fn: Callable[[float], None],
    ) -> None:
        self._policy = policy
        self._now = now_fn
        self._sleep = sleep_fn
        self._lock = threading.Lock()
        self._cooldown_until = 0.0
        self._next_send_at = 0.0

    def before_send(self) -> None:
        rpm = max(1, int(self._policy.target_requests_per_minute))
        base_spacing = max(60.0 / rpm, float(self._policy.min_request_spacing_s))
        while True:
            with self._lock:
                now = self._now()
                ready_at = max(self._cooldown_until, self._next_send_at)
                wait_s = ready_at - now
                if wait_s <= 0:
                    self._next_send_at = max(self._next_send_at, now) + base_spacing
                    return
            self._sleep(wait_s)

    def on_rate_limit(self, retry_after_s: Optional[float]) -> float:
        cooldown = max(float(self._policy.global_429_cooldown_s), retry_after_s or 0.0)
        with self._lock:
            until = self._now() + cooldown
            if until > self._cooldown_until:
                self._cooldown_until = until
        return cooldown


class ConcurrentApiRequester:
    def __init__(
        self,
        *,
        client: RequestClient,
        max_workers: int = 2,
        retry_policy: Optional[RetryPolicy] = None,
        throttle_policy: Optional[ThrottlePolicy] = None,
        is_retryable: Callable[[BaseException], bool] = _default_is_retryable,
        now_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
        random_fn: Callable[[], float] = random.random,
        on_log: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._client = client
        self._max_workers = max(1, int(max_workers))
        self._retry_policy = retry_policy or RetryPolicy()
        self._throttle_policy = throttle_policy or ThrottlePolicy()
        self._is_retryable = is_retryable
        self._now = now_fn
        self._sleep = sleep_fn
        self._random = random_fn
        self._log = on_log or (lambda _msg: None)
        self._throttle = _GlobalThrottle(
            policy=self._throttle_policy,
            now_fn=self._now,
            sleep_fn=self._sleep,
        )

    def _compute_backoff(self, attempt_idx: int) -> float:
        rp = self._retry_policy
        exp = max(rp.min_backoff_s, rp.min_backoff_s * (2**attempt_idx))
        base = min(rp.max_backoff_s, exp)
        return base + self._random() * max(0.0, rp.jitter_s)

    def _extract_retry_after(self, exc: BaseException) -> Optional[float]:
        resp = getattr(exc, "response", None)
        headers = getattr(resp, "headers", None)
        if headers is None:
            return None
        raw = headers.get("Retry-After") or headers.get("retry-after")
        if not raw:
            return None
        try:
            return float(raw)
        except Exception:
            return None

    def _run_one(self, task: RequestTask) -> RequestResult:
        parse_failures = 0
        attempts = 0
        last_error: Optional[str] = None
        for attempt_idx in range(max(1, int(self._retry_policy.max_retries))):
            attempts = attempt_idx + 1
            try:
                self._throttle.before_send()
                output, meta = self._client.perform(task.payload)
                return RequestResult(
                    task_id=task.task_id,
                    status=RequestStatus.SUCCESS,
                    output=output,
                    error=None,
                    attempts=attempts,
                    response_meta=meta,
                )
            except BaseException as exc:
                detail = _format_exception_details(exc)
                last_error = detail
                retryable = self._is_retryable(exc)
                if isinstance(exc, (ValueError, TypeError)):
                    parse_failures += 1
                    if parse_failures > int(self._retry_policy.parse_retry_cap):
                        retryable = False
                if not retryable:
                    self._log(f"[terminal] task_id={task.task_id} attempt={attempts}: {detail}")
                    return RequestResult(
                        task_id=task.task_id,
                        status=RequestStatus.FAILED_TERMINAL,
                        output=None,
                        error=detail,
                        attempts=attempts,
                        response_meta={},
                    )

                retry_after = self._extract_retry_after(exc)
                if isinstance(exc, RateLimitError):
                    cooldown = self._throttle.on_rate_limit(retry_after)
                    self._log(
                        f"[rate_limit] task_id={task.task_id} attempt={attempts}: "
                        f"cooldown={cooldown:.2f}s error={detail}"
                    )
                    self._sleep(cooldown)
                else:
                    sleep_s = self._compute_backoff(attempt_idx)
                    self._log(
                        f"[retry] task_id={task.task_id} attempt={attempts}: "
                        f"sleep={sleep_s:.2f}s error={detail}"
                    )
                    self._sleep(sleep_s)

        return RequestResult(
            task_id=task.task_id,
            status=RequestStatus.FAILED_RETRYABLE,
            output=None,
            error=last_error or "RuntimeError: unknown failure",
            attempts=attempts,
            response_meta={},
        )

    def run(self, tasks: Iterable[RequestTask]) -> Iterator[RequestResult]:
        task_list = list(tasks)
        if not task_list:
            return
        futures: dict[Future[RequestResult], RequestTask] = {}
        executor = ThreadPoolExecutor(max_workers=self._max_workers)
        try:
            for task in task_list:
                futures[executor.submit(self._run_one, task)] = task
            for fut in as_completed(futures):
                task = futures[fut]
                try:
                    yield fut.result()
                except Exception as exc:
                    yield RequestResult(
                        task_id=task.task_id,
                        status=RequestStatus.FAILED_RETRYABLE,
                        output=None,
                        error=_format_exception_details(exc),
                        attempts=1,
                        response_meta={},
                    )
        finally:
            executor.shutdown(wait=True, cancel_futures=False)
