-- Context to carry along with the event, e.g. the W3C trace context of the request that made the change, so the
-- data plane can continue the same distributed trace.
ALTER TABLE outbox ADD COLUMN headers JSONB NOT NULL DEFAULT '{}';
