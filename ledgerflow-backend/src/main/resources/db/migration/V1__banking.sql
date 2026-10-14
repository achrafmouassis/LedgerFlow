CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    owner VARCHAR(120) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    balance NUMERIC(19,2) NOT NULL CHECK (balance >= 0)
);
CREATE TABLE transfers (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(100) NOT NULL UNIQUE,
    source_account_id UUID NOT NULL REFERENCES accounts(id),
    destination_account_id UUID NOT NULL REFERENCES accounts(id),
    amount NUMERIC(19,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CHECK (source_account_id <> destination_account_id)
);
CREATE INDEX transfers_source_history ON transfers(source_account_id, created_at);
CREATE INDEX transfers_destination_history ON transfers(destination_account_id, created_at);
