from __future__ import annotations

import logging
import os
import queue
import random
import signal
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dt_time
from typing import Any, Callable

from dotenv import load_dotenv

from db.db import init_pool, close_pool, getcursor
from db.post_registry_utils import ensure_post_registered
from transcription.transcription import (
    load_whisper_model,
    transcribe_audio_file,
)
from .download_yt_audio import download_yt_audio, DownloadFailed
from ..time import PACIFIC

load_dotenv()

log = logging.getLogger(__name__)

MAX_VID_LENGTH = 3 * 3600
AUDIO_QUEUE_SIZE = 3
SAVE_QUEUE_SIZE = 2

LOCAL_TZ = PACIFIC

SESSION_STARTS = [
    (9, 0),
    (18, 0),
]
SESSION_DURATION_HOURS = 3
VIDEOS_PER_SESSION = 20
SESSION_SPREAD_FRACTION = 0.90


# ---------------------------------------------------------------------
# Small data objects
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class SessionWindow:
    start: datetime
    end: datetime
    active: bool


@dataclass(frozen=True)
class ClaimedVideo:
    video_id: str
    url: str


@dataclass(frozen=True)
class DownloadedAudio:
    video_id: str
    audio_path: str
    tempdir: tempfile.TemporaryDirectory


@dataclass(frozen=True)
class TranscriptResult:
    video_id: str
    segments: Any
    transcript: str
    tempdir: tempfile.TemporaryDirectory


@dataclass(frozen=True)
class LoaderStepResult:
    action: str  # "enqueue" | "retry" | "stop"
    item: DownloadedAudio | None = None


# ---------------------------------------------------------------------
# Scheduling helpers
# ---------------------------------------------------------------------

def _default_now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def build_slot_times(
    *,
    session_start_monotonic: float,
    session_seconds: float,
    slot_count: int,
    spread_fraction: float = SESSION_SPREAD_FRACTION,
    uniform_fn: Callable[[float, float], float] = random.uniform,
) -> list[float]:
    if slot_count <= 0:
        return []

    session_seconds = max(1.0, float(session_seconds))
    spread_fraction = max(0.0, min(1.0, float(spread_fraction)))

    spread_seconds = session_seconds * spread_fraction
    bucket_width = spread_seconds / slot_count

    times: list[float] = []
    for i in range(slot_count):
        bucket_start = session_start_monotonic + (i * bucket_width)
        bucket_end = session_start_monotonic + ((i + 1) * bucket_width)
        times.append(uniform_fn(bucket_start, bucket_end))

    return times


class SessionBudget:
    """
    Artificial session budget / pacing helper.

    A slot is consumed when reserved.
    If no DB row is ultimately claimed, caller may release that slot.
    """

    def __init__(
        self,
        max_videos: int,
        session_seconds: float,
        *,
        spread_fraction: float = SESSION_SPREAD_FRACTION,
        monotonic_fn: Callable[[], float] = time.monotonic,
        uniform_fn: Callable[[float, float], float] = random.uniform,
    ):
        self.max_videos = int(max_videos)
        self.session_seconds = max(1.0, float(session_seconds))
        self.spread_fraction = float(spread_fraction)
        self.monotonic_fn = monotonic_fn
        self.uniform_fn = uniform_fn

        self.session_start_monotonic = self.monotonic_fn()
        self.deadline_monotonic = self.session_start_monotonic + self.session_seconds

        self.claimed = 0
        self.lock = threading.Lock()

        self.slot_times = build_slot_times(
            session_start_monotonic=self.session_start_monotonic,
            session_seconds=self.session_seconds,
            slot_count=self.max_videos,
            spread_fraction=self.spread_fraction,
            uniform_fn=self.uniform_fn,
        )

    def reserve_slot(self) -> float | None:
        with self.lock:
            now = self.monotonic_fn()
            if now >= self.deadline_monotonic:
                return None
            if self.claimed >= self.max_videos:
                return None

            slot_idx = self.claimed
            self.claimed += 1
            return self.slot_times[slot_idx]

    def release_slot(self) -> None:
        with self.lock:
            if self.claimed > 0:
                self.claimed -= 1


def _sleep_until_monotonic(
    target_mono: float,
    *,
    monotonic_fn: Callable[[], float] = time.monotonic,
    sleep_fn: Callable[[float], None] = time.sleep,
    max_chunk_s: float = 30.0,
) -> None:
    while True:
        remaining = target_mono - monotonic_fn()
        if remaining <= 0:
            return
        sleep_fn(min(remaining, max_chunk_s))


def _sleep_until_datetime(
    target_dt: datetime,
    *,
    now_fn: Callable[[], datetime] = _default_now_local,
    sleep_fn: Callable[[float], None] = time.sleep,
    max_chunk_s: float = 300.0,
) -> None:
    while True:
        remaining = (target_dt - now_fn()).total_seconds()
        if remaining <= 0:
            return
        sleep_fn(min(remaining, max_chunk_s))


def next_session_window(now: datetime | None = None) -> SessionWindow:
    now = now or datetime.now(LOCAL_TZ)
    candidates: list[tuple[datetime, datetime]] = []

    for day_offset in (0, 1):
        day = (now + timedelta(days=day_offset)).date()
        for hour, minute in SESSION_STARTS:
            start = datetime.combine(day, dt_time(hour, minute), tzinfo=LOCAL_TZ)
            end = start + timedelta(hours=SESSION_DURATION_HOURS)
            candidates.append((start, end))

    candidates.sort()

    for start, end in candidates:
        if start <= now < end:
            return SessionWindow(start=start, end=end, active=True)
        if now < start:
            return SessionWindow(start=start, end=end, active=False)

    raise RuntimeError("No session window found")


# ---------------------------------------------------------------------
# Global tempdir tracking
# ---------------------------------------------------------------------

_ACTIVE_TEMPDIRS: set[tempfile.TemporaryDirectory] = set()
_TEMPDIRS_LOCK = threading.Lock()


def _track_tempdir(td: tempfile.TemporaryDirectory) -> None:
    with _TEMPDIRS_LOCK:
        _ACTIVE_TEMPDIRS.add(td)


def _cleanup_tempdir(td: tempfile.TemporaryDirectory) -> None:
    try:
        td.cleanup()
    finally:
        with _TEMPDIRS_LOCK:
            _ACTIVE_TEMPDIRS.discard(td)


def _cleanup_all_tempdirs() -> None:
    with _TEMPDIRS_LOCK:
        tds = list(_ACTIVE_TEMPDIRS)
        _ACTIVE_TEMPDIRS.clear()

    for td in tds:
        try:
            td.cleanup()
        except Exception:
            pass


def _hard_exit(code: int) -> None:
    os._exit(code)


# ---------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------

def _handle_signal(signum, frame):
    logging.warning("Received signal %s, cleaning up temp files", signum)
    _cleanup_all_tempdirs()
    close_pool()
    logging.info("youtube transcriber: shutdown complete")
    _hard_exit(0)


# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------

def claim_next_video(cur) -> ClaimedVideo | None:
    cur.execute(
        """
        WITH next_video AS (
            SELECT video_id, url
            FROM youtube.video
            WHERE transcript IS NULL
              AND (
                    transcription_started_at IS NULL
                 OR transcription_started_at < now() - interval '6 hours'
              )
              AND duration_seconds IS NOT NULL
              AND duration_seconds <= %s
            ORDER BY created_at_ts
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE youtube.video y
        SET transcription_started_at = now()
        FROM next_video n
        WHERE y.video_id = n.video_id
        RETURNING y.video_id, y.url;
        """,
        (MAX_VID_LENGTH,),
    )
    row = cur.fetchone()
    if not row:
        return None

    video_id, url = row
    return ClaimedVideo(video_id=video_id, url=str(url))


def save_transcript(cur, video_id: str, transcript: str) -> None:
    cur.execute(
        """
        UPDATE youtube.video
           SET transcript = %s,
               transcript_updated_at = now()
         WHERE video_id = %s
        """,
        (transcript, video_id),
    )


def save_segments(cur, video_id: str, segments) -> None:
    cur.execute(
        """DELETE FROM youtube.transcript_segments WHERE video_id = %s""",
        (video_id,),
    )
    cur.executemany(
        """
        INSERT INTO youtube.transcript_segments (
            video_id,
            seg_idx,
            start_s,
            end_s,
            text
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        [
            (
                video_id,
                idx,
                seg.start,
                seg.end,
                seg.text,
            )
            for idx, seg in enumerate(segments)
        ],
    )


# ---------------------------------------------------------------------
# One-item helpers (primary test targets)
# ---------------------------------------------------------------------

def claim_and_download_one(
        budget: SessionBudget,
        *,
        cursor_factory=getcursor,
        claim_next_video_fn=claim_next_video,
        download_audio_fn=download_yt_audio,
        cleanup_tempdir_fn=_cleanup_tempdir,
        sleep_until_fn=_sleep_until_monotonic,
) -> LoaderStepResult:
    slot_time = budget.reserve_slot()
    if slot_time is None:
        return LoaderStepResult(action="stop")

    sleep_until_fn(slot_time)

    with cursor_factory(commit=True) as cur:
        claimed = claim_next_video_fn(cur)

    if claimed is None:
        logging.info("audio_loader: no claimable video found")
        budget.release_slot()
        return LoaderStepResult(action="stop")

    logging.info("audio_loader: claimed video %s", claimed.video_id)

    td = tempfile.TemporaryDirectory()
    _track_tempdir(td)
    audio_path = os.path.join(td.name, "audio")

    try:
        download_audio_fn(claimed.url, audio_path)
    except DownloadFailed:
        logging.warning("audio_loader: download failed for %s url=%s", claimed.video_id, claimed.url)
        cleanup_tempdir_fn(td)
        return LoaderStepResult(action="retry")

    return LoaderStepResult(
        action="enqueue",
        item=DownloadedAudio(
            video_id=claimed.video_id,
            audio_path=audio_path,
            tempdir=td,
        ),
    )


def transcribe_one(
    model: Any,
    item: DownloadedAudio,
    *,
    transcribe_audio_fn: Callable[[Any, str], tuple[Any, str]] = transcribe_audio_file,
) -> TranscriptResult:
    segments, transcript = transcribe_audio_fn(model, item.audio_path)
    return TranscriptResult(
        video_id=item.video_id,
        segments=segments,
        transcript=transcript,
        tempdir=item.tempdir,
    )


def save_one(
    item: TranscriptResult,
    *,
    cursor_factory: Callable[..., Any] = getcursor,
    save_transcript_fn: Callable[[Any, str, str], None] = save_transcript,
    save_segments_fn: Callable[[Any, str, Any], None] = save_segments,
    ensure_post_registered_fn: Callable[..., None] = ensure_post_registered,
) -> None:
    with cursor_factory(commit=True) as cur:
        save_transcript_fn(cur, item.video_id, item.transcript)
        save_segments_fn(cur, item.video_id, item.segments)
        ensure_post_registered_fn(cur, platform="youtube_video", key1=item.video_id)


# ---------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------

def audio_loader_worker(audio_q: queue.Queue, budget: SessionBudget) -> None:
    logging.info("audio_loader: started")

    while True:
        step = claim_and_download_one(budget)

        if step.action == "stop":
            logging.info("audio_loader: session quota/deadline reached, or no videos left")
            audio_q.put(None)
            return

        if step.action == "retry":
            continue

        assert step.item is not None
        logging.info("audio_loader: downloaded %s", step.item.video_id)
        audio_q.put(step.item)


def transcriber_worker(audio_q: queue.Queue, save_q: queue.Queue) -> None:
    logging.info("transcriber: loading whisper model")
    model = load_whisper_model()
    logging.info("transcriber: model loaded")

    while True:
        item = audio_q.get()
        try:
            if item is None:
                save_q.put(None)
                return
            logging.info("transcriber: processing %s", item.video_id)
            result = transcribe_one(model, item)
            save_q.put(result)
        finally:
            audio_q.task_done()


def saver_worker(save_q: queue.Queue) -> None:
    logging.info("saver: started")

    while True:
        item = save_q.get()
        try:
            if item is None:
                return

            save_one(item)
            logging.info("saver: video %s saved", item.video_id)
        finally:
            if isinstance(item, TranscriptResult):
                _cleanup_tempdir(item.tempdir)
            save_q.task_done()


# ---------------------------------------------------------------------
# Main / orchestration
# ---------------------------------------------------------------------

def _thread_entry(
    fn,
    *args,
    cleanup_all_tempdirs_fn: Callable[[], None] = _cleanup_all_tempdirs,
    exit_fn: Callable[[int], None] = _hard_exit,
):
    """
    Wrapper on each thread func to ensure that if it crashes:
        - temp files are cleaned up
        - program exits
    """
    try:
        fn(*args)
    except Exception:
        logging.exception("%s crashed", fn.__name__)
        cleanup_all_tempdirs_fn()
        exit_fn(1)


def run_one_session(budget: SessionBudget) -> None:
    audio_q = queue.Queue(maxsize=AUDIO_QUEUE_SIZE)
    save_q = queue.Queue(maxsize=SAVE_QUEUE_SIZE)

    threads = [
        threading.Thread(
            target=_thread_entry,
            args=(audio_loader_worker, audio_q, budget),
            daemon=True,
        ),
        threading.Thread(
            target=_thread_entry,
            args=(transcriber_worker, audio_q, save_q),
            daemon=True,
        ),
        threading.Thread(
            target=_thread_entry,
            args=(saver_worker, save_q),
            daemon=True,
        ),
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def run_scheduler_cycle(
    *,
    now_fn: Callable[[], datetime] = _default_now_local,
    sleep_fn: Callable[[float], None] = time.sleep,
    session_runner: Callable[[SessionBudget], None] = run_one_session,
    budget_factory: Callable[[float], SessionBudget] | None = None,
) -> None:
    window = next_session_window(now_fn())

    if not window.active:
        logging.info("Sleeping until next session at %s", window.start.isoformat())
        _sleep_until_datetime(window.start, now_fn=now_fn, sleep_fn=sleep_fn)

    now = now_fn()
    window = next_session_window(now)
    remaining_s = max(1.0, (window.end - now).total_seconds())

    if budget_factory is None:
        budget = SessionBudget(
            max_videos=VIDEOS_PER_SESSION,
            session_seconds=remaining_s,
            spread_fraction=SESSION_SPREAD_FRACTION,
        )
    else:
        budget = budget_factory(remaining_s)

    logging.info(
        "Starting session: quota=%s end=%s",
        VIDEOS_PER_SESSION,
        window.end.isoformat(),
    )
    session_runner(budget)

    now = now_fn()
    if now < window.end:
        logging.info("Session finished early; waiting until %s", window.end.isoformat())
        _sleep_until_datetime(window.end, now_fn=now_fn, sleep_fn=sleep_fn)


def main(prod=False) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    init_pool(prefix="prod" if prod else "dev")

    try:
        while True:
            run_scheduler_cycle()
    finally:
        _cleanup_all_tempdirs()
        close_pool()


if __name__ == "__main__":
    main()