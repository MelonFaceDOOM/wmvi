from dotenv import load_dotenv

from db.db import init_pool, close_pool, getcursor
from transcription.transcription import (
    load_whisper_model,
    transcribe_audio_file,
)
from . import download_episode, DownloadFailed

load_dotenv()


def get_episodes(cur):
    cur.execute(
        """
        SELECT id, download_url
        FROM podcasts.episodes
        WHERE transcript IS NULL
          AND transcription_started_at IS NULL
        ORDER BY created_at_ts
        LIMIT 5
        """
    )
    row = cur.fetchall()
    if not row:
        return None
    return row


init_pool()
with getcursor() as cur:
    episodes = get_episodes(cur)

for id, url in episodes:
    outfile = "tempfiles/"+id+".mp3"
    download_episode(url, outfile)

close_pool()
