-- Wake the outbox relay up as soon as something is committed, instead of waiting for the next poll.
-- NOTIFY is delivered on commit only, and identical notifications within a transaction are collapsed.
CREATE FUNCTION notify_outbox_inserted() RETURNS trigger AS
$$
BEGIN
    PERFORM pg_notify('outbox_inserted', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_inserted
    AFTER INSERT
    ON outbox
    FOR EACH STATEMENT
EXECUTE FUNCTION notify_outbox_inserted();
