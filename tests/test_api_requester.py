from __future__ import annotations

from dataclasses import dataclass, field

from apps.claim_extractor.api_requester import (
    ConcurrentApiRequester,
    RequestStatus,
    RequestTask,
    RetryPolicy,
    ThrottlePolicy,
)
from apps.claim_extractor.dummy_api_client import (
    DummyApiClient,
    DummyBehavior,
    DummyRateLimits,
    DummyRetryableError,
    DummyTerminalError,
)


@dataclass
class FakeClock:
    now: float = 0.0
    sleeps: list[float] = field(default_factory=list)

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        s = max(0.0, float(seconds))
        self.sleeps.append(s)
        self.now += s


def _task(task_id: str, **payload) -> RequestTask:
    return RequestTask(task_id=task_id, payload=payload)


def test_under_limit_all_success() -> None:
    clock = FakeClock()
    client = DummyApiClient(
        limits=DummyRateLimits(requests_per_minute=1000, tokens_per_minute=1_000_000),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
    )
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=2,
        retry_policy=RetryPolicy(max_retries=2),
        throttle_policy=ThrottlePolicy(target_requests_per_minute=1000, global_429_cooldown_s=1.0),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
        random_fn=lambda: 0.0,
    )
    tasks = [_task(f"t{i}", estimated_tokens=100) for i in range(5)]
    results = list(requester.run(tasks))
    assert len(results) == 5
    assert all(r.status == RequestStatus.SUCCESS for r in results)


def test_req_limit_triggers_pausing() -> None:
    clock = FakeClock()
    client = DummyApiClient(
        limits=DummyRateLimits(requests_per_minute=1, tokens_per_minute=1_000_000),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
    )
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=1,
        retry_policy=RetryPolicy(max_retries=3),
        throttle_policy=ThrottlePolicy(
            target_requests_per_minute=10_000,
            global_429_cooldown_s=65.0,  # long enough to age out the 60s limiter window
        ),
        is_retryable=lambda exc: isinstance(exc, DummyRetryableError),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
        random_fn=lambda: 0.0,
    )
    tasks = [_task("a", estimated_tokens=10), _task("b", estimated_tokens=10)]
    results = list(requester.run(tasks))

    by_id = {r.task_id: r for r in results}
    assert by_id["a"].status == RequestStatus.SUCCESS
    assert by_id["b"].status == RequestStatus.FAILED_RETRYABLE
    assert any(s > 0.0 for s in clock.sleeps), "Expected retry sleep from req-limit handling"


def test_token_limit_triggers_pausing() -> None:
    clock = FakeClock()
    client = DummyApiClient(
        limits=DummyRateLimits(requests_per_minute=1000, tokens_per_minute=100),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
    )
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=1,
        retry_policy=RetryPolicy(max_retries=3),
        throttle_policy=ThrottlePolicy(
            target_requests_per_minute=10_000,
            global_429_cooldown_s=65.0,
        ),
        is_retryable=lambda exc: isinstance(exc, DummyRetryableError),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
        random_fn=lambda: 0.0,
    )
    tasks = [_task("a", estimated_tokens=80), _task("b", estimated_tokens=80)]
    results = list(requester.run(tasks))

    by_id = {r.task_id: r for r in results}
    assert by_id["a"].status == RequestStatus.SUCCESS
    assert by_id["b"].status == RequestStatus.FAILED_RETRYABLE
    assert any(s > 0.0 for s in clock.sleeps), "Expected retry sleep from token-limit handling"


def test_terminal_error_not_retried() -> None:
    clock = FakeClock()
    client = DummyApiClient(
        limits=DummyRateLimits(requests_per_minute=1000, tokens_per_minute=1_000_000),
        behavior=DummyBehavior(terminal_flag_key="force_terminal_error"),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
    )
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=1,
        retry_policy=RetryPolicy(max_retries=5),
        throttle_policy=ThrottlePolicy(target_requests_per_minute=1000, global_429_cooldown_s=1.0),
        is_retryable=lambda exc: not isinstance(exc, DummyTerminalError),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
        random_fn=lambda: 0.0,
    )
    results = list(requester.run([_task("x", force_terminal_error=True)]))
    assert len(results) == 1
    result = results[0]
    assert result.status == RequestStatus.FAILED_TERMINAL
    assert result.attempts == 1


def test_retryable_failure_exhaustion_returns_retryable_status() -> None:
    clock = FakeClock()
    client = DummyApiClient(
        limits=DummyRateLimits(requests_per_minute=1000, tokens_per_minute=1_000_000),
        behavior=DummyBehavior(retryable_flag_key="force_retryable_error"),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
    )
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=1,
        retry_policy=RetryPolicy(max_retries=2, min_backoff_s=1.0, max_backoff_s=2.0, jitter_s=0.0),
        throttle_policy=ThrottlePolicy(target_requests_per_minute=1000, global_429_cooldown_s=1.0),
        is_retryable=lambda exc: isinstance(exc, DummyRetryableError),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
        random_fn=lambda: 0.0,
    )
    results = list(requester.run([_task("x", force_retryable_error=True)]))
    assert len(results) == 1
    result = results[0]
    assert result.status == RequestStatus.FAILED_RETRYABLE
    assert result.attempts == 2
    assert any(s >= 1.0 for s in clock.sleeps)
