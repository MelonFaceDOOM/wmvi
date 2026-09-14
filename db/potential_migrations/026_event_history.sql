-- 026_event_history.sql
--
-- Foundation for vaccine-event extraction and post-to-event matching.
--
-- Design invariants:
--   * An event may have many evidence sources.
--   * The source that first caused an event to be created is preserved.
--   * A better source may later become preferred without replacing provenance.
--   * A web source can exist as metadata even when its body was not acquired.
--   * Post references are separate from the smaller set of sources used as
--     evidence for the event.
--   * Search terms discover candidate content; canonical subjects classify
--     events.
--
-- The existing news.article table is intentionally treated as unused and is
-- changed in place. Metadata-only articles are not registered as posts until
-- usable text is added.

BEGIN;

-- ===========================================================================
-- 1) Allow metadata-only news/web documents
-- ===========================================================================

ALTER TABLE news.article
    ALTER COLUMN text DROP NOT NULL;

ALTER TABLE news.article
    ADD COLUMN canonical_url TEXT,
    ADD COLUMN canonical_url_hash VARCHAR(32)
        GENERATED ALWAYS AS (md5(canonical_url)) STORED,
    ADD COLUMN snippet TEXT,
    ADD COLUMN discovery_provider TEXT,
    ADD COLUMN provider_external_id TEXT,
    ADD COLUMN acquisition_status TEXT NOT NULL DEFAULT 'discovered',
    ADD COLUMN last_fetch_attempt_at TIMESTAMPTZ,
    ADD COLUMN fetched_at TIMESTAMPTZ,
    ADD COLUMN fetch_http_status INTEGER,
    ADD COLUMN fetch_error TEXT,
    ADD COLUMN raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN content_hash VARCHAR(32)
        GENERATED ALWAYS AS (md5(text)) STORED;

UPDATE news.article
SET acquisition_status = CASE
    WHEN text IS NOT NULL AND btrim(text) <> '' THEN 'parsed'
    ELSE 'fetched'
END;

ALTER TABLE news.article
    ADD CONSTRAINT news_article_acquisition_status_chk CHECK (
        acquisition_status = ANY (ARRAY[
            'discovered'::text,
            'metadata_only'::text,
            'fetched'::text,
            'parsed'::text,
            'paywalled'::text,
            'robots_disallowed'::text,
            'deleted'::text,
            'fetch_failed'::text
        ])
    ),
    ADD CONSTRAINT news_article_fetch_http_status_chk CHECK (
        fetch_http_status IS NULL
        OR (fetch_http_status >= 100 AND fetch_http_status <= 599)
    ),
    ADD CONSTRAINT news_article_parsed_text_chk CHECK (
        acquisition_status <> 'parsed'
        OR (text IS NOT NULL AND btrim(text) <> '')
    );

CREATE UNIQUE INDEX news_article_canonical_url_hash_uniq
    ON news.article (canonical_url_hash)
    WHERE canonical_url_hash IS NOT NULL;

CREATE UNIQUE INDEX news_article_provider_external_uniq
    ON news.article (discovery_provider, provider_external_id)
    WHERE discovery_provider IS NOT NULL
      AND provider_external_id IS NOT NULL;

CREATE INDEX news_article_acquisition_status_idx
    ON news.article (acquisition_status, date_entered);

CREATE INDEX news_article_created_at_idx
    ON news.article (created_at_ts);

COMMENT ON COLUMN news.article.created_at_ts IS
    'Publisher-reported publication timestamp.';
COMMENT ON COLUMN news.article.acquisition_status IS
    'Acquisition lifecycle; article text is optional except when status=parsed.';
COMMENT ON COLUMN news.article.raw_metadata IS
    'Unmodified discovery/fetch metadata needed for provenance and reprocessing.';

-- A metadata-only article is a source reference, not searchable content. Add it
-- to post_registry only after text becomes available; this also gives newly
-- parsed articles a fresh post_id for incremental language/term workers.
DROP TRIGGER IF EXISTS article_reg_ins ON news.article;
DROP TRIGGER IF EXISTS article_reg_sync ON news.article;
DROP FUNCTION IF EXISTS sm.trg_article_reg_ins();

CREATE FUNCTION sm.trg_article_reg_sync() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.text IS NOT NULL AND btrim(NEW.text) <> '' THEN
    INSERT INTO sm.post_registry(platform, key1, key2)
    VALUES ('news_article', NEW.id::text, '')
    ON CONFLICT (platform, key1, key2) DO NOTHING;
  ELSE
    DELETE FROM sm.post_registry
    WHERE platform = 'news_article'
      AND key1 = NEW.id::text
      AND key2 = '';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER article_reg_sync
AFTER INSERT OR UPDATE OF text ON news.article
FOR EACH ROW EXECUTE FUNCTION sm.trg_article_reg_sync();

-- Bring any pre-existing blank article rows into agreement with the new rule.
DELETE FROM sm.post_registry pr
USING news.article a
WHERE pr.platform = 'news_article'
  AND pr.key1 = a.id::text
  AND pr.key2 = ''
  AND (a.text IS NULL OR btrim(a.text) = '');

-- ===========================================================================
-- 2) Canonical event subjects, separate from lexical search terms
-- ===========================================================================

CREATE TABLE taxonomy.subject (
    id              BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    date_entered    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    slug            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    kind            TEXT NOT NULL,
    description     TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,

    CONSTRAINT subject_slug_chk CHECK (
        slug = lower(slug)
        AND slug ~ '^[a-z0-9]+(?:-[a-z0-9]+)*$'
    ),
    CONSTRAINT subject_kind_chk CHECK (
        kind = ANY (ARRAY[
            'disease'::text,
            'vaccine'::text,
            'pathogen'::text,
            'policy_topic'::text,
            'organization'::text,
            'person'::text,
            'other'::text
        ])
    )
);

CREATE TABLE taxonomy.subject_term (
    subject_id      BIGINT NOT NULL
                        REFERENCES taxonomy.subject(id) ON DELETE CASCADE,
    term_id         INTEGER NOT NULL
                        REFERENCES taxonomy.vaccine_term(id) ON DELETE CASCADE,
    relationship    TEXT NOT NULL DEFAULT 'discovery',
    date_entered    TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (subject_id, term_id),
    CONSTRAINT subject_term_relationship_chk CHECK (
        relationship = ANY (ARRAY[
            'canonical'::text,
            'alias'::text,
            'discovery'::text
        ])
    )
);

CREATE INDEX subject_term_term_idx
    ON taxonomy.subject_term (term_id, subject_id);

COMMENT ON TABLE taxonomy.subject IS
    'Stable concepts used to classify events; independent of mutable search terms.';
COMMENT ON TABLE taxonomy.subject_term IS
    'Search terms that can nominate a canonical subject for deterministic/LLM review.';

-- Initial scope. More diseases, vaccines, and policy subjects can be added
-- without changing the event tables.
INSERT INTO taxonomy.subject (slug, name, kind, description)
VALUES (
    'measles',
    'Measles',
    'disease',
    'Events directly concerning measles infection, outbreaks, exposure, prevention, or response.'
);

INSERT INTO taxonomy.subject_term (subject_id, term_id, relationship)
SELECT
    s.id,
    t.id,
    CASE
        WHEN lower(t.name) = 'measles' THEN 'canonical'
        ELSE 'discovery'
    END
FROM taxonomy.subject s
JOIN taxonomy.vaccine_term t
  ON lower(t.name) = ANY (ARRAY[
      'measles'::text,
      'measles vaccine'::text,
      'mmr vaccine'::text,
      'mmr autism'::text
  ])
WHERE s.slug = 'measles'
ON CONFLICT (subject_id, term_id) DO NOTHING;

-- ===========================================================================
-- 3) Events
-- ===========================================================================

CREATE SCHEMA events;

CREATE TABLE events.event (
    id                      BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    date_entered            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    event_type              TEXT NOT NULL,
    title                   TEXT NOT NULL,
    summary                 TEXT,

    start_date              DATE,
    end_date                DATE,
    date_precision          TEXT NOT NULL DEFAULT 'unknown',
    location                JSONB NOT NULL DEFAULT '{}'::jsonb,
    attributes              JSONB NOT NULL DEFAULT '{}'::jsonb,

    status                  TEXT NOT NULL DEFAULT 'active',
    merged_into_event_id    BIGINT
                                REFERENCES events.event(id) ON DELETE RESTRICT,

    CONSTRAINT event_type_not_blank_chk CHECK (btrim(event_type) <> ''),
    CONSTRAINT event_title_not_blank_chk CHECK (btrim(title) <> ''),
    CONSTRAINT event_date_order_chk CHECK (
        end_date IS NULL OR start_date IS NULL OR end_date >= start_date
    ),
    CONSTRAINT event_date_precision_chk CHECK (
        date_precision = ANY (ARRAY[
            'exact'::text,
            'day'::text,
            'month'::text,
            'year'::text,
            'range'::text,
            'unknown'::text
        ])
    ),
    CONSTRAINT event_status_chk CHECK (
        status = ANY (ARRAY[
            'active'::text,
            'resolved'::text,
            'superseded'::text,
            'merged'::text,
            'disputed'::text
        ])
    ),
    CONSTRAINT event_merge_state_chk CHECK (
        (status = 'merged') = (merged_into_event_id IS NOT NULL)
    ),
    CONSTRAINT event_not_merged_into_self_chk CHECK (
        merged_into_event_id IS NULL OR merged_into_event_id <> id
    )
);

CREATE INDEX event_type_start_date_idx
    ON events.event (event_type, start_date);

CREATE INDEX event_start_date_idx
    ON events.event (start_date);

CREATE INDEX event_status_idx
    ON events.event (status)
    WHERE status <> 'merged';

CREATE INDEX event_location_gin
    ON events.event USING GIN (location);

COMMENT ON COLUMN events.event.attributes IS
    'Event-type-specific structured fields; stable common fields remain columns.';
COMMENT ON COLUMN events.event.merged_into_event_id IS
    'Canonical event retained when duplicate candidate events are merged.';

-- ===========================================================================
-- 4) Stable source identity and event evidence
-- ===========================================================================

CREATE TABLE events.source_item (
    id              BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    date_entered    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- post | web_document | api_record
    source_type     TEXT NOT NULL,

    -- Fast links to local content when it exists. Provider/external_id or URL
    -- remain after local content is removed.
    post_id         BIGINT
                        REFERENCES sm.post_registry(id) ON DELETE SET NULL,
    article_id      BIGINT
                        REFERENCES news.article(id) ON DELETE SET NULL,

    provider        TEXT,
    external_id     TEXT,
    url             TEXT,
    url_hash        VARCHAR(32) GENERATED ALWAYS AS (md5(url)) STORED,
    title           TEXT,
    published_at    TIMESTAMPTZ,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT source_item_type_chk CHECK (
        source_type = ANY (ARRAY[
            'post'::text,
            'web_document'::text,
            'api_record'::text
        ])
    ),
    CONSTRAINT source_item_locator_chk CHECK (
        (
            source_type = 'post'
            AND provider IS NOT NULL
            AND btrim(provider) <> ''
            AND external_id IS NOT NULL
            AND btrim(external_id) <> ''
        )
        OR (
            source_type = 'web_document'
            AND url IS NOT NULL
            AND btrim(url) <> ''
        )
        OR (
            source_type = 'api_record'
            AND provider IS NOT NULL
            AND btrim(provider) <> ''
            AND external_id IS NOT NULL
            AND btrim(external_id) <> ''
        )
    )
);

CREATE UNIQUE INDEX source_item_post_uniq
    ON events.source_item (post_id)
    WHERE post_id IS NOT NULL;

CREATE UNIQUE INDEX source_item_article_uniq
    ON events.source_item (article_id)
    WHERE article_id IS NOT NULL;

CREATE UNIQUE INDEX source_item_provider_external_uniq
    ON events.source_item (source_type, provider, external_id)
    WHERE provider IS NOT NULL AND external_id IS NOT NULL;

CREATE UNIQUE INDEX source_item_web_url_uniq
    ON events.source_item (url_hash)
    WHERE source_type = 'web_document' AND url_hash IS NOT NULL;

COMMENT ON TABLE events.source_item IS
    'Stable source identity. Local post/article links are optional accelerators, not the only provenance.';

CREATE TABLE events.event_source (
    event_id                BIGINT NOT NULL
                                REFERENCES events.event(id) ON DELETE CASCADE,
    source_item_id          BIGINT NOT NULL
                                REFERENCES events.source_item(id) ON DELETE RESTRICT,
    date_entered            TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Discovery provenance never needs to be replaced when preferred evidence
    -- changes.
    is_discovery_source     BOOLEAN NOT NULL DEFAULT FALSE,
    evidence_role           TEXT NOT NULL DEFAULT 'corroboration',
    is_preferred            BOOLEAN NOT NULL DEFAULT FALSE,
    preference_rank         SMALLINT,
    preference_reason       TEXT,

    confidence              NUMERIC(5,4),
    support_excerpt         TEXT,
    extraction_metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (event_id, source_item_id),
    CONSTRAINT event_source_role_chk CHECK (
        evidence_role = ANY (ARRAY[
            'primary'::text,
            'corroboration'::text,
            'contradiction'::text,
            'mention'::text
        ])
    ),
    CONSTRAINT event_source_rank_chk CHECK (
        preference_rank IS NULL OR preference_rank >= 0
    ),
    CONSTRAINT event_source_confidence_chk CHECK (
        confidence IS NULL OR (confidence >= 0 AND confidence <= 1)
    ),
    CONSTRAINT event_source_preferred_role_chk CHECK (
        NOT is_preferred OR evidence_role <> 'contradiction'
    )
);

CREATE UNIQUE INDEX event_source_one_preferred_idx
    ON events.event_source (event_id)
    WHERE is_preferred;

CREATE INDEX event_source_source_idx
    ON events.event_source (source_item_id, event_id);

CREATE INDEX event_source_discovery_idx
    ON events.event_source (event_id)
    WHERE is_discovery_source;

COMMENT ON COLUMN events.event_source.is_discovery_source IS
    'True when this source caused creation of the event; preserve when preferred evidence changes.';
COMMENT ON COLUMN events.event_source.is_preferred IS
    'Current best display/evidence source for this event; at most one per event.';
COMMENT ON COLUMN events.event_source.preference_rank IS
    'Event-specific source rank; lower is better. Ranking policy depends on event type.';

-- ===========================================================================
-- 5) Event classification and all post references
-- ===========================================================================

CREATE TABLE events.event_subject (
    event_id                BIGINT NOT NULL
                                REFERENCES events.event(id) ON DELETE CASCADE,
    subject_id              BIGINT NOT NULL
                                REFERENCES taxonomy.subject(id) ON DELETE RESTRICT,
    date_entered            TIMESTAMPTZ NOT NULL DEFAULT now(),

    relationship            TEXT NOT NULL DEFAULT 'related',
    confidence              NUMERIC(5,4),
    assigned_by             TEXT NOT NULL,
    assignment_version      TEXT NOT NULL DEFAULT '',
    source_item_id          BIGINT
                                REFERENCES events.source_item(id) ON DELETE SET NULL,
    assignment_metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (event_id, subject_id),
    CONSTRAINT event_subject_relationship_chk CHECK (
        relationship = ANY (ARRAY[
            'primary'::text,
            'related'::text,
            'implicated'::text
        ])
    ),
    CONSTRAINT event_subject_confidence_chk CHECK (
        confidence IS NULL OR (confidence >= 0 AND confidence <= 1)
    ),
    CONSTRAINT event_subject_assigned_by_chk CHECK (
        assigned_by = ANY (ARRAY[
            'human'::text,
            'llm'::text,
            'structured_source'::text,
            'rule'::text
        ])
    )
);

CREATE INDEX event_subject_subject_idx
    ON events.event_subject (subject_id, event_id);

COMMENT ON TABLE events.event_subject IS
    'Canonical event classification. Query this table, not keyword hits, for all measles events.';

-- Every post that references an event belongs here. Only posts promoted to
-- event evidence also receive source_item + event_source rows.
CREATE TABLE events.post_reference (
    event_id            BIGINT NOT NULL
                            REFERENCES events.event(id) ON DELETE CASCADE,
    post_id             BIGINT NOT NULL
                            REFERENCES sm.post_registry(id) ON DELETE CASCADE,
    analyzer_version    TEXT NOT NULL,
    analyzed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    reference_type      TEXT NOT NULL DEFAULT 'mentions',
    confidence          NUMERIC(5,4),
    support_excerpt     TEXT,
    analysis_metadata   JSONB NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (event_id, post_id, analyzer_version),
    CONSTRAINT post_reference_analyzer_version_chk CHECK (
        btrim(analyzer_version) <> ''
    ),
    CONSTRAINT post_reference_type_chk CHECK (
        reference_type = ANY (ARRAY[
            'reports'::text,
            'mentions'::text,
            'reacts'::text,
            'disputes'::text
        ])
    ),
    CONSTRAINT post_reference_confidence_chk CHECK (
        confidence IS NULL OR (confidence >= 0 AND confidence <= 1)
    )
);

CREATE INDEX post_reference_post_idx
    ON events.post_reference (post_id, event_id);

CREATE INDEX post_reference_event_current_idx
    ON events.post_reference (event_id, analyzed_at DESC);

COMMENT ON TABLE events.post_reference IS
    'LLM-detected post-to-event references. These are not automatically evidence sources.';

COMMIT;
