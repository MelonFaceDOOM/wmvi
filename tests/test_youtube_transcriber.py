from __future__ import annotations

from datetime import datetime, timedelta
import tempfile
import os
from typing import Any

import pytest

pytestmark = pytest.mark.transcription

tr = pytest.importorskip("services.youtube.transcriber.transcriber")


# ----------------------------
# Helpers
# ----------------------------

class Now:
    def __init__(self, dt: datetime) -> None:
        self.dt = dt

    def __call__(self) -> datetime:
        return self.dt

    def set(self, dt: datetime) -> None:
        self.dt = dt

    def advance(self, seconds: float) -> None:
        self.dt = self.dt + timedelta(seconds=seconds)


class Mono:
    def __init__(self, value: float) -> None:
        self.value = float(value)

    def __call__(self) -> float:
        return self.value

    def set(self, value: float) -> None:
        self.value = float(value)

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


class Sleeper:
    def __init__(self, *, now: Now | None = None, mono: Mono | None = None) -> None:
        self.calls: list[float] = []
        self.now = now
        self.mono = mono

    def __call__(self, seconds: float) -> None:
        seconds = float(seconds)
        self.calls.append(seconds)
        if self.now is not None:
            self.now.advance(seconds)
        if self.mono is not None:
            self.mono.advance(seconds)


class DummyCursorFactory:
    def __init__(self, cur: Any | None = None) -> None:
        self.cur = cur if cur is not None else object()
        self.commit_calls: list[bool] = []

    def __call__(self, *, commit: bool = False):
        self.commit_calls.append(bool(commit))
        return self

    def __enter__(self):
        return self.cur

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture(autouse=True)
def cleanup_tempdirs():
    tr._cleanup_all_tempdirs()
    yield
    tr._cleanup_all_tempdirs()


# ----------------------------
# Scheduling helpers
# ----------------------------

def test_build_slot_times_spreads_within_buffer() -> None:
    def midpoint(a: float, b: float) -> float:
        return (a + b) / 2.0

    times = tr.build_slot_times(
        session_start_monotonic=100.0,
        session_seconds=100.0,
        slot_count=4,
        spread_fraction=0.90,
        uniform_fn=midpoint,
    )

    assert times == pytest.approx([111.25, 133.75, 156.25, 178.75])
    assert len(times) == 4
    assert times == sorted(times)
    assert times[0] >= 100.0
    assert times[-1] <= 190.0


def test_session_budget_reserves_until_quota_exhausted() -> None:
    mono = Mono(10.0)

    budget = tr.SessionBudget(
        max_videos=2,
        session_seconds=60.0,
        monotonic_fn=mono,
        uniform_fn=lambda a, b: a,
    )

    first = budget.reserve_slot()
    second = budget.reserve_slot()
    third = budget.reserve_slot()

    assert first is not None
    assert second is not None
    assert third is None


def test_session_budget_stops_after_deadline() -> None:
    mono = Mono(10.0)

    budget = tr.SessionBudget(
        max_videos=2,
        session_seconds=60.0,
        monotonic_fn=mono,
        uniform_fn=lambda a, b: a,
    )

    mono.set(1000.0)
    assert budget.reserve_slot() is None


def test_sleep_until_monotonic_sleeps_in_chunks() -> None:
    mono = Mono(0.0)
    sleeper = Sleeper(mono=mono)

    tr._sleep_until_monotonic(
        65.0,
        monotonic_fn=mono,
        sleep_fn=sleeper,
        max_chunk_s=30.0,
    )

    assert sleeper.calls == pytest.approx([30.0, 30.0, 5.0])
    assert mono() == pytest.approx(65.0)


def test_next_session_window_when_inside_session() -> None:
    now = datetime(2026, 2, 13, 9, 30, tzinfo=tr.LOCAL_TZ)

    window = tr.next_session_window(now)

    assert window.active is True
    assert window.start == datetime(2026, 2, 13, 9, 0, tzinfo=tr.LOCAL_TZ)
    assert window.end == datetime(2026, 2, 13, 12, 0, tzinfo=tr.LOCAL_TZ)


def test_next_session_window_rolls_to_next_day() -> None:
    now = datetime(2026, 2, 13, 22, 0, tzinfo=tr.LOCAL_TZ)

    window = tr.next_session_window(now)

    assert window.active is False
    assert window.start == datetime(2026, 2, 14, 9, 0, tzinfo=tr.LOCAL_TZ)
    assert window.end == datetime(2026, 2, 14, 12, 0, tzinfo=tr.LOCAL_TZ)


# ----------------------------
# One-item helpers
# ----------------------------

def test_claim_and_download_one_stops_when_no_slot_available() -> None:
    budget = tr.SessionBudget(
        max_videos=0,
        session_seconds=60.0,
        monotonic_fn=Mono(0.0),
        uniform_fn=lambda a, b: a,
    )

    step = tr.claim_and_download_one(budget)

    assert step.action == "stop"
    assert step.item is None


def test_claim_and_download_one_releases_slot_when_no_video_claimed() -> None:
    budget = tr.SessionBudget(
        max_videos=1,
        session_seconds=60.0,
        monotonic_fn=Mono(0.0),
        uniform_fn=lambda a, b: a,
    )
    cursor_factory = DummyCursorFactory()

    step = tr.claim_and_download_one(
        budget,
        cursor_factory=cursor_factory,
        claim_next_video_fn=lambda cur: None,
        sleep_until_fn=lambda target: None,
    )

    assert step.action == "stop"
    assert step.item is None
    assert budget.claimed == 0
    assert cursor_factory.commit_calls == [True]


def test_claim_and_download_one_retries_on_download_failure_and_cleans_up() -> None:
    budget = tr.SessionBudget(
        max_videos=1,
        session_seconds=60.0,
        monotonic_fn=Mono(0.0),
        uniform_fn=lambda a, b: a,
    )
    cursor_factory = DummyCursorFactory()
    cleanup_calls: list[Any] = []

    def cleanup(td) -> None:
        cleanup_calls.append(td)
        tr._cleanup_tempdir(td)

    def fail_download(url: str, path: str) -> None:
        raise tr.DownloadFailed("boom")

    step = tr.claim_and_download_one(
        budget,
        cursor_factory=cursor_factory,
        claim_next_video_fn=lambda cur: tr.ClaimedVideo(video_id="vid1", url="https://x"),
        download_audio_fn=fail_download,
        cleanup_tempdir_fn=cleanup,
        sleep_until_fn=lambda target: None,
    )

    assert step.action == "retry"
    assert step.item is None
    assert len(cleanup_calls) == 1
    assert budget.claimed == 1


def test_claim_and_download_one_enqueues_downloaded_audio() -> None:
    budget = tr.SessionBudget(
        max_videos=1,
        session_seconds=60.0,
        monotonic_fn=Mono(0.0),
        uniform_fn=lambda a, b: a,
    )
    cursor_factory = DummyCursorFactory()
    seen: dict[str, str] = {}

    def ok_download(url: str, path: str) -> None:
        seen["url"] = url
        seen["path"] = path

    step = tr.claim_and_download_one(
        budget,
        cursor_factory=cursor_factory,
        claim_next_video_fn=lambda cur: tr.ClaimedVideo(video_id="vid1", url="https://x"),
        download_audio_fn=ok_download,
        sleep_until_fn=lambda target: None,
    )

    assert step.action == "enqueue"
    assert step.item is not None
    assert step.item.video_id == "vid1"
    assert seen["url"] == "https://x"
    assert seen["path"] == step.item.audio_path
    assert step.item.audio_path == os.path.join(step.item.tempdir.name, "audio")


def test_transcribe_one_returns_transcript_result() -> None:
    item = tr.DownloadedAudio(
        video_id="vid1",
        audio_path="/tmp/audio",
        tempdir=tempfile.TemporaryDirectory(),
    )

    try:
        result = tr.transcribe_one(
            object(),
            item,
            transcribe_audio_fn=lambda model, path: (["seg1", "seg2"], "hello world"),
        )
    finally:
        tr._cleanup_tempdir(item.tempdir)

    assert result.video_id == "vid1"
    assert result.segments == ["seg1", "seg2"]
    assert result.transcript == "hello world"
    assert result.tempdir is item.tempdir


def test_save_one_persists_transcript_segments_and_registry() -> None:
    cursor_factory = DummyCursorFactory()
    calls: list[tuple] = []

    item = tr.TranscriptResult(
        video_id="vid1",
        segments=["seg1"],
        transcript="hello",
        tempdir=tempfile.TemporaryDirectory(),
    )

    try:
        tr.save_one(
            item,
            cursor_factory=cursor_factory,
            save_transcript_fn=lambda cur, video_id, transcript: calls.append(
                ("transcript", video_id, transcript)
            ),
            save_segments_fn=lambda cur, video_id, segments: calls.append(
                ("segments", video_id, segments)
            ),
            ensure_post_registered_fn=lambda cur, platform, key1: calls.append(
                ("registry", platform, key1)
            ),
        )
    finally:
        tr._cleanup_tempdir(item.tempdir)

    assert cursor_factory.commit_calls == [True]
    assert calls == [
        ("transcript", "vid1", "hello"),
        ("segments", "vid1", ["seg1"]),
        ("registry", "youtube_video", "vid1"),
    ]


# ----------------------------
# Scheduler step
# ----------------------------

def test_run_scheduler_cycle_sleeps_to_start_runs_session_and_waits_to_end() -> None:
    now = Now(datetime(2026, 2, 13, 8, 0, tzinfo=tr.LOCAL_TZ))
    sleeper = Sleeper(now=now)

    seen: dict[str, Any] = {}

    def budget_factory(remaining_s: float):
        seen["remaining_s"] = remaining_s
        return "BUDGET"

    def session_runner(budget) -> None:
        seen["budget"] = budget

    tr.run_scheduler_cycle(
        now_fn=now,
        sleep_fn=sleeper,
        session_runner=session_runner,
        budget_factory=budget_factory,
    )

    assert seen["budget"] == "BUDGET"
    assert seen["remaining_s"] == pytest.approx(3 * 3600)
    assert now() == datetime(2026, 2, 13, 12, 0, tzinfo=tr.LOCAL_TZ)
    assert sum(sleeper.calls) == pytest.approx(4 * 3600)