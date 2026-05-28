-- Tracks last successfully imported podcast transcript export (Azure prod).
-- Single row: id = 'global'.

CREATE TABLE IF NOT EXISTS sm.podcast_transcript_import_state (
    id                 TEXT PRIMARY KEY,
    last_imported_at   TIMESTAMPTZ
);

INSERT INTO sm.podcast_transcript_import_state (id, last_imported_at)
VALUES ('global', NULL)
ON CONFLICT (id) DO NOTHING;
