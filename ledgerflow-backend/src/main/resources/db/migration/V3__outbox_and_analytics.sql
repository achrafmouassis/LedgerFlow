CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    journal_id UUID NOT NULL UNIQUE REFERENCES journals(id),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    operation_type VARCHAR(20) NOT NULL,
    debit_account_id UUID,
    credit_account_id UUID,
    amount NUMERIC(19,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE,
    next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error VARCHAR(200)
);
CREATE INDEX outbox_pending ON outbox_events(published_at,next_attempt_at,occurred_at);
-- Historical operations are retained and will be delivered when the pipeline is enabled.
INSERT INTO outbox_events(event_id,journal_id,schema_version,operation_type,debit_account_id,
    credit_account_id,amount,currency,occurred_at,created_at,next_attempt_at,attempts)
SELECT id,id,1,type,debit_account_id,credit_account_id,amount,currency,created_at,
    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0 FROM journals;

CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE analytics.processed_events (
    event_id UUID PRIMARY KEY,
    payload_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE TABLE analytics.transaction_facts (
    event_id UUID PRIMARY KEY REFERENCES analytics.processed_events(event_id),
    journal_id UUID NOT NULL UNIQUE,
    operation_type VARCHAR(20) NOT NULL,
    debit_account_id UUID,
    credit_account_id UUID,
    amount NUMERIC(19,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    event_date DATE NOT NULL
);
CREATE INDEX analytics_daily ON analytics.transaction_facts(event_date,currency,operation_type);
