-- Operational state, separate from the immutable banking journal.
CREATE TABLE analytics.dead_letters (
    id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    message_key BYTEA,
    payload BYTEA,
    captured_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'OPEN'
        CHECK (status IN ('OPEN','REPLAYED','RESOLVED')),
    replay_event_id UUID,
    replay_payload TEXT,
    replayed_at TIMESTAMP WITH TIME ZONE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    replay_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE(topic,partition_id,offset_id)
);
CREATE TABLE analytics.monitor_samples (
    sampled_at TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    pending BIGINT NOT NULL,
    oldest_pending_seconds DOUBLE PRECISION NOT NULL,
    unprocessed BIGINT NOT NULL,
    kafka_lag BIGINT NOT NULL,
    open_dead_letters BIGINT NOT NULL,
    failed_batches BIGINT NOT NULL,
    running_batches BIGINT NOT NULL
);
CREATE TABLE analytics.alerts (
    alert_key VARCHAR(80) PRIMARY KEY,
    active BOOLEAN NOT NULL,
    message VARCHAR(500) NOT NULL,
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    notified_at TIMESTAMP WITH TIME ZONE
);
CREATE INDEX dead_letters_status ON analytics.dead_letters(status,captured_at);
CREATE VIEW analytics.dead_letter_overview AS
    SELECT id,topic,partition_id,offset_id,captured_at,status,replayed_at,resolved_at,replay_count
    FROM analytics.dead_letters;
