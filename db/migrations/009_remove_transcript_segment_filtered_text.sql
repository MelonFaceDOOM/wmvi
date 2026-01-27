BEGIN;

-- Drop dependent index first if it exists
DROP INDEX IF EXISTS seg_text_tsv_en_gin ;

-- Remove obsolete columns
ALTER TABLE podcasts.transcript_segments
    DROP COLUMN IF EXISTS tsv_en,
    DROP COLUMN IF EXISTS filtered_text;

-- Recreate tsv_en directly from text
ALTER TABLE podcasts.transcript_segments
    ADD COLUMN tsv_en tsvector
    GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(text, ''))
    ) STORED;

-- Recreate index
CREATE INDEX seg_text_tsv_en_gin
    ON podcasts.transcript_segments
    USING GIN (tsv_en);

COMMIT;
