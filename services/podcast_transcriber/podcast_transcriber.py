from __future__ import annotations

import logging
import os
import queue
import signal
import tempfile
import threading
from typing import Optional, Tuple

from dotenv import load_dotenv

from db.db import init_pool, close_pool, getcursor
from db.post_registry_utils import ensure_post_registered
from transcription.transcription import (
    load_whisper_model,
    transcribe_audio_file,
)
from . import download_episode, DownloadFailed

load_dotenv()

AUDIO_QUEUE_SIZE = 3
SAVE_QUEUE_SIZE = 2

# ----------------------------
# Global tempdir tracking
# ----------------------------

_ACTIVE_TEMPDIRS: set[tempfile.TemporaryDirectory] = set()
_TEMPDIRS_LOCK = threading.Lock()
_DONE = threading.Event()


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


# ----------------------------
# Signal handling
# ----------------------------

def _handle_signal(signum, frame):
    logging.warning("Received signal %s, cleaning up temp files", signum)
    _cleanup_all_tempdirs()
    close_pool()
    logging.info("podcast_transcriber: shutdown complete")
    os._exit(0)


# ----------------------------
# DB helpers
# ----------------------------

def claim_next_episode(cur) -> Optional[Tuple[str, str]]:
    cur.execute(
        """
        SELECT id, download_url
        FROM podcasts.episodes
        WHERE transcript IS NULL
          AND (
                transcription_started_at IS NULL
             OR transcription_started_at < now() - interval '6 hours'
          )
        ORDER BY created_at_ts
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        """
    )
    row = cur.fetchone()
    if not row:
        return None

    episode_id, url = row
    cur.execute(
        """
        UPDATE podcasts.episodes
           SET transcription_started_at = now()
         WHERE id = %s
        """,
        (episode_id,),
    )

    return episode_id, str(url)


def save_transcript(cur, episode_id: str, transcript: str) -> None:
    cur.execute(
        """
        UPDATE podcasts.episodes
           SET transcript = %s,
               transcript_updated_at = now()
         WHERE id = %s
        """,
        (transcript, episode_id),
    )


def save_segments(cur, episode_id: str, segments) -> None:
    cur.execute(
        """DELETE FROM podcasts.transcript_segments WHERE episode_id = %s""", (episode_id,))
    cur.executemany(
        """
        INSERT INTO podcasts.transcript_segments (
            episode_id,
            seg_idx,
            start_s,
            end_s,
            text
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        [
            (
                episode_id,
                idx,
                seg.start,
                seg.end,
                seg.text,
            )
            for idx, seg in enumerate(segments)
        ],
    )

# ----------------------------
# Workers
# ----------------------------


def audio_loader_worker(audio_q: queue.Queue, limit: Optional[int]) -> None:
    logging.info("audio_loader: started")
    claimed = 0

    while True:
        if limit is not None and claimed >= limit:
            audio_q.put(None)  # pass exit signal down the line
            logging.info("audio_loader: reached limit=%s", limit)
            return

        with getcursor(commit=True) as cur:
            item = claim_next_episode(cur)

        if item is None:
            audio_q.put(None)  # pass exit signal down the line
            logging.info("audio_loader: no episodes left")
            return

        episode_id, url = item
        claimed += 1

        td = tempfile.TemporaryDirectory()
        _track_tempdir(td)
        audio_path = os.path.join(td.name, "audio")
        logging.info("audio_loader: downloading %s", url)
        try:
            download_episode(url, audio_path)
            # completed by transcriber_worker()
            audio_q.put((episode_id, audio_path, td))
        except DownloadFailed as e:
            logging.warning("audio_loader: %s", e)
            _cleanup_tempdir(td)
            continue


def transcriber_worker(audio_q: queue.Queue, save_q: queue.Queue) -> None:
    logging.info("transcriber: loading whisper model")
    model = load_whisper_model()
    logging.info("transcriber: model loaded")

    while True:
        item = audio_q.get()
        if item is None:
            save_q.put(None)  # pass exit signal along
            return  # exit
        try:
            episode_id, audio_path, td = item
            segments, transcript = transcribe_audio_file(model, audio_path)
            save_q.put((episode_id, segments, transcript, td))
        finally:
            audio_q.task_done()


def saver_worker(save_q: queue.Queue) -> None:
    logging.info("saver: started")

    while True:
        item = save_q.get()
        if item is None:
            return  # exit
        try:
            episode_id, segments, transcript, td = item

            # Persist to DB (atomic within one transaction)
            with getcursor(commit=True) as cur:
                save_transcript(cur, episode_id, transcript)
                save_segments(cur, episode_id, segments)
                ensure_post_registered(
                    cur, platform="podcast_episode", key1=episode_id)
            logging.info("saver: episode %s saved", episode_id)
        finally:
            # Always clean up the tempdir once saver is done
            # Saver is the *final owner* of td
            if item is not None:
                _cleanup_tempdir(td)

            save_q.task_done()

# ----------------------------
# Main
# ----------------------------


def _thread_entry(fn, *args):
    """
    a wrapper on each thread func to ensure that if it crashes:
        - temp files are cleaned up
        - program exits
    """
    try:
        fn(*args)
    except Exception:
        logging.exception("%s crashed", fn.__name__)
        _cleanup_all_tempdirs()
        os._exit(1)


def main(prod=False, limit: Optional[int] = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if prod:
        init_pool(prefix="prod")
    else:
        init_pool(prefix="dev")

    audio_q = queue.Queue(maxsize=AUDIO_QUEUE_SIZE)
    save_q = queue.Queue(maxsize=SAVE_QUEUE_SIZE)

    threads = [
        threading.Thread(
            target=_thread_entry,
            args=(audio_loader_worker, audio_q, limit),
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

    try:
        for t in threads:
            t.join()
        logging.info("All episodes processed")
    finally:
        _cleanup_all_tempdirs()
        close_pool()


if __name__ == "__main__":
    main(limit=5)
