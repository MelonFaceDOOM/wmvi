-- Create search status table for reddit submissions
CREATE TABLE IF NOT EXISTS sm.reddit_submission_search_status (
    term_id integer NOT NULL,
    last_found_ts timestamp with time zone NOT NULL,
    last_found_id text NOT NULL,
    date_entered timestamp with time zone DEFAULT now() NOT NULL,
    last_updated timestamp with time zone DEFAULT now() NOT NULL
);

-- Primary key
ALTER TABLE sm.reddit_submission_search_status
    ADD CONSTRAINT reddit_submission_search_status_pkey PRIMARY KEY (term_id);

-- Index on last_found_ts 
CREATE INDEX IF NOT EXISTS reddit_submission_search_status_last_found_idx
    ON sm.reddit_submission_search_status USING btree (last_found_ts);

-- composite index to query by both
CREATE INDEX IF NOT EXISTS reddit_submission_search_status_last_found_ts_id_idx
    ON sm.reddit_submission_search_status USING btree (last_found_ts, last_found_id);

-- Foreign key to taxonomy.vaccine_term(id)
ALTER TABLE sm.reddit_submission_search_status
    ADD CONSTRAINT reddit_submission_search_status_term_fk
    FOREIGN KEY (term_id)
    REFERENCES taxonomy.vaccine_term(id)
    ON DELETE CASCADE;

