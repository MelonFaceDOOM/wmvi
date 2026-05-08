from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Optional


class DummyRetryableError(Exception):
    pass


class DummyTerminalError(Exception):
    pass


@dataclass(frozen=True)
class DummyRateLimits:
    requests_per_minute: int = 150
    tokens_per_minute: int = 150_000


@dataclass(frozen=True)
class DummyBehavior:
    # Approximate response latency for realism in concurrent tests.
    base_latency_s: float = 0.02
    # When payload has this key and truthy value, return terminal error.
    terminal_flag_key: str = "force_terminal_error"
    # When payload has this key and truthy value, return retryable error.
    retryable_flag_key: str = "force_retryable_error"


class DummyApiClient:
    """
    Deterministic-ish fake client with req/min + token/min limits.

    Use payload keys:
      - "estimated_tokens" (int) to control token accounting.
      - behavior flags from DummyBehavior to force errors.
    """

    def __init__(
        self,
        *,
        limits: Optional[DummyRateLimits] = None,
        behavior: Optional[DummyBehavior] = None,
        now_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self._limits = limits or DummyRateLimits()
        self._behavior = behavior or DummyBehavior()
        self._now = now_fn
        self._sleep = sleep_fn
        self._lock = threading.Lock()
        self._req_window: Deque[float] = deque()
        self._tok_window: Deque[tuple[float, int]] = deque()

    def _prune(self, now: float) -> None:
        floor = now - 60.0
        while self._req_window and self._req_window[0] < floor:
            self._req_window.popleft()
        while self._tok_window and self._tok_window[0][0] < floor:
            self._tok_window.popleft()

    def _current_tokens(self) -> int:
        return sum(tok for _, tok in self._tok_window)

    def perform(self, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        now = self._now()
        req_tokens = int(payload.get("estimated_tokens") or 100)
        if req_tokens < 0:
            req_tokens = 0

        with self._lock:
            self._prune(now)
            if len(self._req_window) >= max(1, int(self._limits.requests_per_minute)):
                raise DummyRetryableError("429 too_many_requests: dummy requests_per_minute exceeded")
            if self._current_tokens() + req_tokens > max(1, int(self._limits.tokens_per_minute)):
                raise DummyRetryableError("429 too_many_tokens: dummy tokens_per_minute exceeded")
            self._req_window.append(now)
            self._tok_window.append((now, req_tokens))

        if payload.get(self._behavior.terminal_flag_key):
            raise DummyTerminalError("dummy terminal failure requested")
        if payload.get(self._behavior.retryable_flag_key):
            raise DummyRetryableError("dummy retryable failure requested")

        self._sleep(max(0.0, float(self._behavior.base_latency_s)))
        output = {"claims": []}
        meta = {
            "provider": "dummy",
            "usage": {
                "prompt_tokens": req_tokens,
                "completion_tokens": 0,
                "total_tokens": req_tokens,
            },
        }
        return output, meta
