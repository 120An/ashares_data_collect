-- Phase 1 news Entity / EntityAlias shadow foundation.
-- Legacy instrument_info, instrument_changelog, sector_stock and
-- sector_changelog remain read-only upstream inputs and are untouched.
-- This migration is append-oriented: revisions are retained permanently.

CREATE TABLE IF NOT EXISTS news_entity_revision (
    entity_id                  VARCHAR(192) NOT NULL,
    entity_revision            INTEGER NOT NULL,
    schema_version             VARCHAR(64) NOT NULL DEFAULT 'entity_v1',
    entity_type                VARCHAR(40) NOT NULL,
    canonical_name             TEXT NOT NULL,
    normalized_name            TEXT NOT NULL,
    short_name                 TEXT,
    english_name               TEXT,
    aliases                    JSONB NOT NULL DEFAULT '[]'::jsonb,
    stock_code                 VARCHAR(20),
    exchange                   VARCHAR(16),
    external_ids               JSONB NOT NULL DEFAULT '{}'::jsonb,
    parent_entity_id           VARCHAR(192),
    country_region_codes       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    description                TEXT,
    status                     VARCHAR(24) NOT NULL,
    valid_from                 TIMESTAMPTZ,
    valid_to                   TIMESTAMPTZ,
    provenance_source_ids      TEXT[] NOT NULL,
    confidence                 NUMERIC(6,5) NOT NULL,
    entity_model_version       VARCHAR(192),
    merged_into_entity_id      VARCHAR(192),
    is_latest_revision         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                 TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (entity_id, entity_revision),
    CHECK (entity_revision >= 1),
    CHECK (confidence >= 0 AND confidence <= 1),
    CHECK (valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to),
    CHECK (LEFT(entity_id, 4) = 'ent_')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_news_entity_latest_revision
    ON news_entity_revision (entity_id)
    WHERE is_latest_revision;

CREATE INDEX IF NOT EXISTS ix_news_entity_stock_code_latest
    ON news_entity_revision (stock_code)
    WHERE is_latest_revision AND stock_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_news_entity_normalized_name_latest
    ON news_entity_revision (normalized_name)
    WHERE is_latest_revision;


CREATE TABLE IF NOT EXISTS news_entity_alias_revision (
    entity_alias_id            VARCHAR(192) NOT NULL,
    entity_id                  VARCHAR(192) NOT NULL,
    alias                      TEXT NOT NULL,
    normalized_alias           TEXT NOT NULL,
    alias_type                 VARCHAR(40) NOT NULL,
    language                   VARCHAR(32) NOT NULL,
    valid_from                 TIMESTAMPTZ,
    valid_to                   TIMESTAMPTZ,
    provenance_source_ids      TEXT[] NOT NULL,
    provenance_refs            JSONB NOT NULL DEFAULT '[]'::jsonb,
    confidence                 NUMERIC(6,5) NOT NULL,
    derived_by                 VARCHAR(32) NOT NULL,
    entity_model_version       VARCHAR(192),
    revision                   INTEGER NOT NULL,
    is_current                 BOOLEAN NOT NULL,
    is_latest_revision         BOOLEAN NOT NULL DEFAULT TRUE,
    manual_lock                BOOLEAN NOT NULL DEFAULT FALSE,
    created_at                 TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (entity_alias_id, revision),
    CHECK (revision >= 1),
    CHECK (confidence >= 0 AND confidence <= 1),
    CHECK (valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to),
    CHECK (LEFT(entity_alias_id, 7) = 'ealias_'),
    CHECK (LEFT(entity_id, 4) = 'ent_')
);

-- Entity is revisioned, while an alias points to the stable entity_id.  A
-- direct foreign key to one entity revision would therefore be incorrect;
-- application-layer validation must ensure the stable entity_id exists.
CREATE UNIQUE INDEX IF NOT EXISTS uq_news_entity_alias_latest_revision
    ON news_entity_alias_revision (entity_alias_id)
    WHERE is_latest_revision;

CREATE INDEX IF NOT EXISTS ix_news_entity_alias_lookup_current
    ON news_entity_alias_revision (normalized_alias, valid_from, valid_to)
    WHERE is_latest_revision AND is_current;

CREATE INDEX IF NOT EXISTS ix_news_entity_alias_lookup_history
    ON news_entity_alias_revision (normalized_alias, valid_from, valid_to)
    WHERE is_latest_revision;

CREATE INDEX IF NOT EXISTS ix_news_entity_alias_entity
    ON news_entity_alias_revision (entity_id, alias_type)
    WHERE is_latest_revision;
