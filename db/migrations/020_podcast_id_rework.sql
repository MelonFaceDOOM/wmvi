-- OVERALL PURPOSE
-- replace existing slugified GUID id system with this new format:
--   "ep_<podcast_id>_<md5(token)>"
--   where token is built from the first of these that is present: guid > download_url > created_at_ts+title


ALTER TABLE podcasts.episodes
    DROP COLUMN IF EXISTS audio_path;

--An ETag (Entity Tag) in podcasting is
--an HTTP header used by servers to identify specific versions of audio files or RSS feeds
--    How it Works: The server generates a unique identifier (hash or version number) for a podcast file.
--    If the server returns a 304 Not Modified status, the app uses its cached version instead of re-downloading.
ALTER TABLE podcasts.shows
    ADD COLUMN IF NOT EXISTS etag text,
    ADD COLUMN IF NOT EXISTS last_modified text,
    ADD COLUMN IF NOT EXISTS last_fetch_ts timestamptz,
    ADD COLUMN IF NOT EXISTS last_http_status int,
    ADD COLUMN IF NOT EXISTS last_error text;

-- Normalize "missing" GUIDs to NULL (guid is currently NOT NULL)
ALTER TABLE podcasts.episodes
    ALTER COLUMN guid DROP NOT NULL;

UPDATE podcasts.episodes
SET guid = NULL
WHERE guid = '';

ALTER TABLE podcasts.episodes
    DROP CONSTRAINT IF EXISTS episodes_guid_not_empty_chk;

ALTER TABLE podcasts.episodes
    ADD CONSTRAINT episodes_guid_not_empty_chk
    CHECK (guid IS NULL OR guid <> '');


-- Replace UNIQUE(podcast_id, guid) with partial unique (only when guid is present)
ALTER TABLE podcasts.episodes
    DROP CONSTRAINT IF EXISTS episodes_podcast_guid_uniq;

CREATE UNIQUE INDEX IF NOT EXISTS episodes_podcast_guid_uniq
    ON podcasts.episodes (podcast_id, guid)
    WHERE guid IS NOT NULL AND guid <> '';

-- Add fallback uniqueness on download_url (also partial)
CREATE INDEX IF NOT EXISTS episodes_podcast_download_url_idx
    ON podcasts.episodes (podcast_id, download_url)
    WHERE download_url IS NOT NULL AND download_url <> '';

-- Update FK to allow ON UPDATE CASCADE (so we can rewrite episode IDs safely)
ALTER TABLE podcasts.transcript_segments
    DROP CONSTRAINT IF EXISTS transcript_segments_episode_id_fkey;

ALTER TABLE podcasts.transcript_segments
    ADD CONSTRAINT transcript_segments_episode_id_fkey
    FOREIGN KEY (episode_id)
    REFERENCES podcasts.episodes(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


-- Compute new deterministic IDs and update
-- Canonical episode token preference: guid > download_url > created_at_ts+title
-- New ID format: "ep_<podcast_id>_<md5(token)>"
-- NULLIF returns NULL if for nullif(a, b), a == b.
-- So by doing NULLIF(val, ''), we return NULL any time val is empty string.

-- FIRST: build old_id -> new_id mapping so we can update sm.post_registry after rewriting episodes.id
DROP TABLE IF EXISTS _podcast_episode_id_map;
CREATE TEMP TABLE _podcast_episode_id_map (
    old_id text PRIMARY KEY,
    new_id text NOT NULL UNIQUE
) ON COMMIT DROP;

INSERT INTO _podcast_episode_id_map (old_id, new_id)
SELECT
  e.id AS old_id,
  'ep_' || e.podcast_id::text || '_' ||
  md5(
    e.podcast_id::text || ':' ||
    COALESCE(
      NULLIF(e.guid, ''),
      NULLIF(e.download_url, ''),
      COALESCE(e.created_at_ts::text, '') || ':' || COALESCE(e.title, '')
    )
  ) AS new_id
FROM podcasts.episodes e;


-- FIRST: check to see if any dupe ids would be produced
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM _podcast_episode_id_map
        GROUP BY new_id
        HAVING COUNT(*) > 1
        LIMIT 1
    ) THEN
        RAISE EXCEPTION 'episode id recompute would create duplicates; aborting migration';
    END IF;
END $$;

-- Now perform the actual update
UPDATE podcasts.episodes e
SET id = m.new_id
FROM _podcast_episode_id_map m
WHERE e.id = m.old_id;

-- update pr with new ids
UPDATE sm.post_registry pr
SET key1 = m.new_id
FROM _podcast_episode_id_map m
WHERE pr.platform = 'podcast_episode'
  AND pr.key1 = m.old_id
  AND pr.key2 = '';

-- ensure pr update went smooth
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sm.post_registry pr
        WHERE pr.platform = 'podcast_episode'
          AND pr.key2 = ''
          AND NOT EXISTS (
              SELECT 1 FROM podcasts.episodes e WHERE e.id = pr.key1
          )
        LIMIT 1
    ) THEN
        RAISE EXCEPTION 'post_registry has podcast_episode rows that do not match any podcasts.episodes.id after migration';
    END IF;
END $$;

