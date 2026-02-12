-- 003_claims_system.sql
--
-- Claims extraction + directory placement + narrative grouping.
-- Assumptions:
--  - posts live in sm.* tables and are referenced globally by sm.post_registry.id
--  - LLM extractor outputs atomic claims + facet/stance/template + ordered entities
--  - entities are currently limited to: vaccine_concept, disease, adverse_effect
--  - claims can be placed into multiple categories via claim groups (many-to-many)
--  - sm.post_summary replaces any need for sm.post_filtered

-- =========================
-- === SCHEMAS
-- =========================
CREATE SCHEMA IF NOT EXISTS directory;
CREATE SCHEMA IF NOT EXISTS claims;


-- =========================
-- === DIRECTORY TREE
-- =========================
-- Generic category tree (major/minor/etc).
CREATE TABLE IF NOT EXISTS directory.node (
    id          INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_id   INT REFERENCES directory.node(id) ON DELETE SET NULL,
    name        TEXT NOT NULL,
    slug        TEXT,
    sort_order  INT NOT NULL DEFAULT 0,
    depth       INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS directory_node_slug_uniq
    ON directory.node(slug)
    WHERE slug IS NOT NULL;

CREATE INDEX IF NOT EXISTS directory_node_parent_idx
    ON directory.node(parent_id);


-- =========================
-- === CANONICAL CLAIMS
-- =========================
-- One row per canonical claim text (typically normalized + with placeholders like [vaccine]).
CREATE TABLE IF NOT EXISTS claims.claim (
    id                   INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    normalized_text      TEXT NOT NULL UNIQUE,
    first_seen_post_id   BIGINT REFERENCES sm.post_registry(id) ON DELETE SET NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS claims_claim_first_seen_idx
    ON claims.claim(first_seen_post_id);


-- =========================
-- === EXTRACTION RUN STATUS
-- =========================
-- Tracks that a given extractor_version has scanned a given post.
-- Supports: claims_found, no_claims, error, skipped, etc.
CREATE TABLE IF NOT EXISTS claims.post_claim_status (
    post_id           BIGINT NOT NULL
                          REFERENCES sm.post_registry(id)
                          ON DELETE CASCADE,
    extractor_version TEXT NOT NULL,
    status            TEXT NOT NULL,
    last_run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_message     TEXT,
    PRIMARY KEY (post_id, extractor_version)
);

CREATE INDEX IF NOT EXISTS post_claim_status_status_idx
    ON claims.post_claim_status(status);

CREATE INDEX IF NOT EXISTS post_claim_status_last_run_idx
    ON claims.post_claim_status(last_run_at);


-- =========================
-- === CLAIM INSTANCES (PER POST)
-- =========================
-- One row per extracted claim instance in a post for a given extractor_version.
CREATE TABLE IF NOT EXISTS claims.post_claim (
    id                BIGSERIAL PRIMARY KEY,
    post_id           BIGINT NOT NULL
                         REFERENCES sm.post_registry(id)
                         ON DELETE CASCADE,
    claim_id          INT NOT NULL
                         REFERENCES claims.claim(id)
                         ON DELETE CASCADE,

    extractor_version TEXT NOT NULL,
    ordinal           INT NOT NULL DEFAULT 0, -- order of the claim within the post (0..N-1)

    -- LLM output fields for the instance
    claim_text        TEXT NOT NULL,          -- canonical claim text (likely same as claims.claim.normalized_text)
    facet             TEXT NOT NULL CHECK (facet IN ('safety','effectiveness','policy','mechanism','evidence','ethics','other')),
    stance            TEXT NOT NULL CHECK (stance IN ('anti_vaccine','pro_vaccine','neutral_or_unclear')),
    template          TEXT NOT NULL,           -- v1: free text

    -- optional provenance (useful for debugging and re-canonicalization)
    raw_text          TEXT,
    cleaned_text      TEXT,
    confidence        NUMERIC,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT post_claim_uniq UNIQUE (post_id, extractor_version, ordinal)
);

CREATE INDEX IF NOT EXISTS post_claim_post_idx
    ON claims.post_claim(post_id);

CREATE INDEX IF NOT EXISTS post_claim_claim_idx
    ON claims.post_claim(claim_id);

CREATE INDEX IF NOT EXISTS post_claim_extractor_idx
    ON claims.post_claim(extractor_version);

CREATE INDEX IF NOT EXISTS post_claim_facet_idx
    ON claims.post_claim(facet);

CREATE INDEX IF NOT EXISTS post_claim_template_idx
    ON claims.post_claim(template);

CREATE INDEX IF NOT EXISTS post_claim_stance_idx
    ON claims.post_claim(stance);


-- =========================
-- === ENTITIES (NORMALIZED)
-- =========================
-- Dedup layer for entity strings.
CREATE TABLE IF NOT EXISTS claims.entity (
    id          BIGSERIAL PRIMARY KEY,
    type        TEXT NOT NULL CHECK (type IN ('vaccine_concept','disease','adverse_effect')),
    value       TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT claims_entity_type_value_uniq UNIQUE (type, value)
);

CREATE INDEX IF NOT EXISTS claims_entity_type_idx
    ON claims.entity(type);

-- Ordered entities per extracted claim instance.
CREATE TABLE IF NOT EXISTS claims.post_claim_entity (
    post_claim_id BIGINT NOT NULL
        REFERENCES claims.post_claim(id) ON DELETE CASCADE,
    entity_id     BIGINT NOT NULL
        REFERENCES claims.entity(id) ON DELETE CASCADE,
    entity_order  INT NOT NULL DEFAULT 0, -- preserves order in claim_text

    PRIMARY KEY (post_claim_id, entity_order),
    CONSTRAINT post_claim_entity_no_dupe UNIQUE (post_claim_id, entity_id, entity_order)
);

CREATE INDEX IF NOT EXISTS post_claim_entity_entity_idx
    ON claims.post_claim_entity(entity_id, post_claim_id);

CREATE INDEX IF NOT EXISTS post_claim_entity_post_claim_idx
    ON claims.post_claim_entity(post_claim_id);


-- =========================
-- === NARRATIVE GROUPING
-- =========================
-- A narrative group lives within a directory node and holds 1..N canonical claims.
-- Claims can live in multiple groups (including across directory nodes).
CREATE TABLE IF NOT EXISTS claims.claim_group (
    id                 INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    directory_node_id  INT NOT NULL
                           REFERENCES directory.node(id)
                           ON DELETE CASCADE,
    label              TEXT NOT NULL,  -- human label for the narrative (can be edited)
    description        TEXT,
    representative_claim_id INT
                           REFERENCES claims.claim(id)
                           ON DELETE SET NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS claim_group_directory_idx
    ON claims.claim_group(directory_node_id);

CREATE INDEX IF NOT EXISTS claim_group_rep_claim_idx
    ON claims.claim_group(representative_claim_id);

-- Many-to-many: claim membership in narratives.
CREATE TABLE IF NOT EXISTS claims.claim_group_member (
    group_id   INT NOT NULL
                   REFERENCES claims.claim_group(id)
                   ON DELETE CASCADE,
    claim_id   INT NOT NULL
                   REFERENCES claims.claim(id)
                   ON DELETE CASCADE,
    added_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, claim_id)
);

CREATE INDEX IF NOT EXISTS claim_group_member_claim_idx
    ON claims.claim_group_member(claim_id);

CREATE INDEX IF NOT EXISTS claim_group_member_group_idx
    ON claims.claim_group_member(group_id);


