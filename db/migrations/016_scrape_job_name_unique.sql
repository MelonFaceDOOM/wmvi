-- Prevent concurrent inserts/updates while deduping
LOCK TABLE scrape.job IN SHARE ROW EXCLUSIVE MODE;

-- Map duplicate names -> keep the lowest id, collect all ids for that name
CREATE TEMP TABLE job_dedupe_map AS
SELECT
  name,
  MIN(id) AS keep_id,
  ARRAY_AGG(id ORDER BY id) AS all_ids
FROM scrape.job
GROUP BY name
HAVING COUNT(*) > 1;

-- Update any referencing FK columns (single-column FKs only) to point to keep_id
DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT
      conrelid::regclass AS child_table,
      (SELECT attname
       FROM pg_attribute
       WHERE attrelid = conrelid AND attnum = conkey[1]) AS child_col
    FROM pg_constraint
    WHERE contype = 'f'
      AND confrelid = 'scrape.job'::regclass
      AND array_length(conkey, 1) = 1
      AND array_length(confkey, 1) = 1
  LOOP
    EXECUTE format(
      'UPDATE %s t
       SET %I = m.keep_id
       FROM job_dedupe_map m
       WHERE t.%I = ANY (m.all_ids)
         AND t.%I <> m.keep_id',
      r.child_table, r.child_col, r.child_col, r.child_col
    );
  END LOOP;
END $$;

-- Delete duplicates, keeping the lowest-id row for each name
DELETE FROM scrape.job j
USING job_dedupe_map m
WHERE j.name = m.name
  AND j.id <> m.keep_id;

-- Enforce uniqueness going forward
ALTER TABLE scrape.job
  ADD CONSTRAINT scrape_job_name_uniq UNIQUE (name);
