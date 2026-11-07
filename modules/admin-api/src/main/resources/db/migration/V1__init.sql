CREATE TABLE environments (
    key        TEXT PRIMARY KEY,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Environment-independent part of a flag.
CREATE TABLE flags (
    key         TEXT PRIMARY KEY,
    name        TEXT        NOT NULL,
    kind        TEXT        NOT NULL,
    variations  JSONB       NOT NULL,
    salt        TEXT        NOT NULL,
    description TEXT,
    tags        JSONB       NOT NULL DEFAULT '[]',
    archived    BOOLEAN     NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Targeting of a flag in one environment. Stored as a document; `version` is used for optimistic locking
-- and as the monotonic version SDKs see.
CREATE TABLE flag_configs (
    flag_key    TEXT        NOT NULL REFERENCES flags (key),
    environment TEXT        NOT NULL REFERENCES environments (key),
    config      JSONB       NOT NULL,
    version     BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, flag_key)
);

CREATE TABLE segments (
    environment TEXT        NOT NULL REFERENCES environments (key),
    key         TEXT        NOT NULL,
    segment     JSONB       NOT NULL,
    version     BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (environment, key)
);

-- Who changed what and when.
CREATE TABLE audit_log (
    id          BIGSERIAL PRIMARY KEY,
    environment TEXT,
    entity_type TEXT        NOT NULL,
    entity_key  TEXT        NOT NULL,
    action      TEXT        NOT NULL,
    actor       TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX audit_log_entity_idx ON audit_log (entity_type, entity_key, created_at DESC);

-- Transactional outbox: written in the same transaction as the change itself, relayed to Kafka by a
-- separate process. Guarantees that every committed change is eventually published, and nothing else is.
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    key          TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
