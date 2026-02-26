from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


# ----------------------------
# Generic fake scrape outcome
# ----------------------------

@dataclass
class FakeScrapeWindowOutcome:
    pages: int = 0
    found_v: int = 0
    stops: dict[str, int] = field(default_factory=dict)

    ins_v: int = 0
    skip_v: int = 0
    ins_c: int = 0
    skip_c: int = 0

    new_vids: list[dict] = field(default_factory=list)
    new_comments: list[dict] = field(default_factory=list)


# ----------------------------
# Fake budget tracker + quota client
# ----------------------------

class FakeBudgetTracker:
    """
    Minimal stand-in for BudgetTracker used by monitor/backfill.
    You can control affordability via remaining units.
    """
    def __init__(self, *, used: int = 0, remaining: int = 50_000) -> None:
        self._used = int(used)
        self._remaining = int(remaining)

    def used_units_today(self) -> int:
        return self._used

    def remaining_units_today(self) -> int:
        return self._remaining

    def can_afford(self, requested_units: int) -> bool:
        return int(requested_units) <= self._remaining


class FakeYTQuotaClient:
    """
    Minimal stand-in for YTQuotaClient used by backfill/monitor.

    You can:
      - control can_afford() result
      - expose a .tracker with used/remaining methods
    """
    def __init__(
        self,
        *,
        afford: bool = True,
        used: int = 0,
        remaining: int = 50_000,
    ) -> None:
        self._afford = bool(afford)
        self.tracker = FakeBudgetTracker(used=used, remaining=remaining)

    def can_afford(self, _method: str) -> bool:
        return self._afford


# ----------------------------
# Fake getcursor() context manager
# ----------------------------

class FakeCursor:
    """
    A tiny cursor that returns a predetermined fetchone() / fetchall().
    Optionally captures execute() calls for assertions.
    """
    def __init__(
        self,
        *,
        fetchone_value: Any = None,
        fetchall_value: Any = None,
        capture_exec: bool = False,
    ) -> None:
        self._fetchone_value = fetchone_value
        self._fetchall_value = fetchall_value
        self.capture_exec = bool(capture_exec)
        self.exec_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def execute(self, *args: Any, **kwargs: Any) -> None:
        if self.capture_exec:
            self.exec_calls.append((args, kwargs))

    def fetchone(self) -> Any:
        return self._fetchone_value

    def fetchall(self) -> Any:
        return self._fetchall_value


class FakeCursorContext:
    """
    Context manager that mimics db.db.getcursor().
    """
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> FakeCursor:
        return self._cursor

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def fake_getcursor(
    *,
    fetchone_value: Any = None,
    fetchall_value: Any = None,
    capture_exec: bool = False,
) -> tuple[Callable[..., FakeCursorContext], FakeCursor]:
    """
    Returns (getcursor_fn, cursor_obj) so tests can monkeypatch getcursor
    and also inspect what happened.

    Example:
        getcursor_fn, cur = fake_getcursor(fetchone_value=(None,))
        monkeypatch.setattr(mod, "getcursor", getcursor_fn)
        ...
        assert cur.exec_calls
    """
    cur = FakeCursor(
        fetchone_value=fetchone_value,
        fetchall_value=fetchall_value,
        capture_exec=capture_exec,
    )

    def _getcursor(*_a: Any, **_k: Any) -> FakeCursorContext:
        return FakeCursorContext(cur)

    return _getcursor, cur