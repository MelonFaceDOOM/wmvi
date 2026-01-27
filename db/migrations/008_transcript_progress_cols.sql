BEGIN;

-- ----------------------------
-- podcasts.episodes
-- ----------------------------

ALTER TABLE podcasts.episodes
    ADD COLUMN IF NOT EXISTS transcription_started_at TIMESTAMPTZ;

-- ----------------------------
-- sm.youtube_video
-- ----------------------------

ALTER TABLE sm.youtube_video
    ADD COLUMN IF NOT EXISTS transcription_started_at TIMESTAMPTZ;

COMMIT;
