"""Hardcoded nitwitch public download + SFTP paths (not configurable via .env)."""

NITWITCH_DL_BASE_URL = "https://nitwitch.com/dl/transcription_exports/"
NITWITCH_SFTP_ROOT = "/mnt/md0/nitwitch_dl/transcription_exports"

# Subdirectory under transcription_exports (mirrored on HTTP and SFTP).
PODCAST_TRANSCRIPTS_SUBDIR = "podcast_transcripts"
CONTENT_SYNC_SUBDIR = "content_sync"
