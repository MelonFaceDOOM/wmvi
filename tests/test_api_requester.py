from __future__ import annotations

from dataclasses import dataclass, field

from nlp.claim_extraction.api_requester import (
    AzureClaimsClient,
    ConcurrentApiRequester,
    OpenAIClaimsClient,
    RequestStatus,
    RequestTask,
    RetryPolicy,
    ThrottlePolicy,
)
from nlp.claim_extraction.dummy_api_client import (
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


class _FakeResponse:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = headers or {}


class FakeHttpError(Exception):
    def __init__(
        self,
        *,
        status_code: int = 429,
        headers: dict[str, str] | None = None,
        message: str = "rate limited",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = _FakeResponse(headers)


class OnceRateLimitedClient:
    def __init__(self, *, headers: dict[str, str] | None = None, status_code: int = 429) -> None:
        self._headers = headers
        self._status_code = status_code
        self.calls = 0

    def perform(self, payload: dict) -> tuple[dict, dict]:
        self.calls += 1
        if self.calls == 1:
            raise FakeHttpError(status_code=self._status_code, headers=self._headers)
        return {"ok": True}, {}


def _rate_limit_requester(
    client: OnceRateLimitedClient,
    clock: FakeClock,
    *,
    rpm: int = 10_000,
    cooldown_s: float = 20.0,
) -> ConcurrentApiRequester:
    return ConcurrentApiRequester(
        client=client,
        max_workers=1,
        retry_policy=RetryPolicy(max_retries=3, min_backoff_s=1.0, max_backoff_s=2.0, jitter_s=0.0),
        throttle_policy=ThrottlePolicy(
            target_requests_per_minute=rpm,
            global_429_cooldown_s=cooldown_s,
        ),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
        random_fn=lambda: 0.0,
    )


def test_retry_after_ms_wins_over_policy_cooldown() -> None:
    clock = FakeClock()
    client = OnceRateLimitedClient(headers={"retry-after-ms": "2000"})
    requester = _rate_limit_requester(client, clock, cooldown_s=20.0)
    results = list(requester.run([_task("a")]))
    assert results[0].status == RequestStatus.SUCCESS
    assert results[0].attempts == 2
    assert 2.0 in clock.sleeps
    assert 20.0 not in clock.sleeps


def test_retry_after_seconds_wins_over_policy_cooldown() -> None:
    clock = FakeClock()
    client = OnceRateLimitedClient(headers={"Retry-After": "1"})
    requester = _rate_limit_requester(client, clock, cooldown_s=20.0)
    results = list(requester.run([_task("a")]))
    assert results[0].status == RequestStatus.SUCCESS
    assert 1.0 in clock.sleeps
    assert 20.0 not in clock.sleeps


def test_429_without_retry_header_uses_policy_cooldown() -> None:
    clock = FakeClock()
    client = OnceRateLimitedClient(headers={})
    requester = _rate_limit_requester(client, clock, cooldown_s=20.0)
    results = list(requester.run([_task("a")]))
    assert results[0].status == RequestStatus.SUCCESS
    assert 20.0 in clock.sleeps


def test_http_429_status_triggers_global_pause_not_backoff() -> None:
    clock = FakeClock()
    client = OnceRateLimitedClient(headers={})
    requester = _rate_limit_requester(client, clock, cooldown_s=20.0)
    results = list(requester.run([_task("a")]))
    assert results[0].status == RequestStatus.SUCCESS
    assert 20.0 in clock.sleeps
    assert 1.0 not in clock.sleeps


def test_zero_rpm_does_not_pre_pace() -> None:
    clock = FakeClock()
    client = DummyApiClient(
        limits=DummyRateLimits(requests_per_minute=1000, tokens_per_minute=1_000_000),
        behavior=DummyBehavior(base_latency_s=0.01),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
    )
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=1,
        retry_policy=RetryPolicy(max_retries=1),
        throttle_policy=ThrottlePolicy(target_requests_per_minute=0, global_429_cooldown_s=1.0),
        now_fn=clock.monotonic,
        sleep_fn=clock.sleep,
        random_fn=lambda: 0.0,
    )
    results = list(requester.run([_task(f"t{i}", estimated_tokens=10) for i in range(3)]))
    assert len(results) == 3
    assert all(r.status == RequestStatus.SUCCESS for r in results)
    assert clock.now < 1.0
    assert all(s < 1.0 for s in clock.sleeps)


def test_openai_client_disables_sdk_retries(monkeypatch) -> None:
    captured: dict = {}

    def fake_openai(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("nlp.claim_extraction.api_requester.OpenAI", fake_openai)
    OpenAIClaimsClient(
        api_key="k",
        model="m",
        system_prompt_builder=lambda _p: "",
        user_prompt_builder=lambda _p: "",
        response_schema={},
        output_parser=lambda _s: {},
    )
    assert captured["max_retries"] == 0


def test_azure_client_disables_sdk_retries(monkeypatch) -> None:
    captured: dict = {}

    def fake_azure(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("nlp.claim_extraction.api_requester.AzureOpenAI", fake_azure)
    AzureClaimsClient(
        api_key="k",
        azure_endpoint="https://example.openai.azure.com",
        api_version="2024-08-01-preview",
        model="m",
        system_prompt_builder=lambda _p: "",
        user_prompt_builder=lambda _p: "",
        response_schema={},
        output_parser=lambda _s: {},
    )
    assert captured["max_retries"] == 0
