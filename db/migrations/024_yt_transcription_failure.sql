BEGIN;

ALTER TABLE youtube.video
    ADD COLUMN IF NOT EXISTS transcription_failed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS transcription_failure_reason TEXT;

COMMIT;
