ALTER TABLE youtube.search_status
    ADD COLUMN IF NOT EXISTS oldest_found_ts timestamptz,
    ADD COLUMN IF NOT EXISTS oldest_updated timestamptz DEFAULT now() NOT NULL;

-- initialize it to last_found_ts for existing rows
UPDATE youtube.search_status
SET oldest_found_ts = now() - interval '14 days'
WHERE oldest_found_ts IS NULL;