-- now() is the start of the transaction; clock_timestamp() is the moment the row is written, which is what
-- propagation latency should be measured from.
ALTER TABLE outbox ALTER COLUMN created_at SET DEFAULT clock_timestamp();
