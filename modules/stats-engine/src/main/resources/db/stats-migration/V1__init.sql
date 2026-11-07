-- First exposure of every context to every flag: assigns the context to a variation for analysis.
CREATE TABLE stats.exposures (
    environment  TEXT   NOT NULL,
    flag         TEXT   NOT NULL,
    context_key  TEXT   NOT NULL,
    variation    TEXT   NOT NULL,
    flag_version BIGINT NOT NULL,
    exposed_at   BIGINT NOT NULL, -- epoch millis
    PRIMARY KEY (environment, flag, context_key)
);

CREATE TABLE stats.metric_events (
    environment TEXT   NOT NULL,
    id          TEXT   NOT NULL, -- client-generated, makes redelivery idempotent
    metric      TEXT   NOT NULL,
    context_key TEXT   NOT NULL,
    value       DOUBLE PRECISION,
    occurred_at BIGINT NOT NULL, -- epoch millis
    PRIMARY KEY (environment, id)
);

-- Serves the per-user lookup in the analysis query.
CREATE INDEX metric_events_attribution_idx ON stats.metric_events (environment, metric, context_key, occurred_at);
