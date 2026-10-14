-- A journal contains exactly two legs sharing amount/currency, so it cannot
-- have a missing or unbalanced leg. NULL account on a leg means bank cash.
CREATE TABLE journals (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(100) UNIQUE,
    type VARCHAR(20) NOT NULL CHECK (type IN ('OPENING','DEPOSIT','WITHDRAWAL','TRANSFER')),
    debit_account_id UUID REFERENCES accounts(id),
    credit_account_id UUID REFERENCES accounts(id),
    amount NUMERIC(19,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    transfer_id UUID UNIQUE REFERENCES transfers(id),
    CHECK (
        (type = 'OPENING' AND debit_account_id IS NULL AND credit_account_id IS NOT NULL
            AND transfer_id IS NULL AND idempotency_key IS NULL)
        OR (type = 'DEPOSIT' AND debit_account_id IS NULL AND credit_account_id IS NOT NULL
            AND transfer_id IS NULL AND idempotency_key IS NOT NULL)
        OR (type = 'WITHDRAWAL' AND debit_account_id IS NOT NULL AND credit_account_id IS NULL
            AND transfer_id IS NULL AND idempotency_key IS NOT NULL)
        OR (type = 'TRANSFER' AND debit_account_id IS NOT NULL AND credit_account_id IS NOT NULL
            AND debit_account_id <> credit_account_id AND transfer_id IS NOT NULL
            AND idempotency_key IS NOT NULL)
    )
);
CREATE INDEX journals_debit_history ON journals(debit_account_id, created_at);
CREATE INDEX journals_credit_history ON journals(credit_account_id, created_at);

-- Backfill the already persisted V1 state without modifying original balances.
-- Recover opening funds by undoing all incoming/outgoing transfers.
INSERT INTO journals (id, type, debit_account_id, credit_account_id, amount, currency, created_at)
SELECT id, 'OPENING', NULL, id, opening_balance, currency, CURRENT_TIMESTAMP
FROM (
    SELECT a.id, a.currency, a.balance
        + COALESCE((SELECT SUM(t.amount) FROM transfers t WHERE t.source_account_id = a.id), 0)
        - COALESCE((SELECT SUM(t.amount) FROM transfers t WHERE t.destination_account_id = a.id), 0)
        AS opening_balance
    FROM accounts a
) opening WHERE opening_balance > 0;
INSERT INTO journals (id, idempotency_key, type, debit_account_id, credit_account_id,
    amount, currency, created_at, transfer_id)
SELECT id, idempotency_key, 'TRANSFER', source_account_id, destination_account_id,
    amount, currency, created_at, id FROM transfers;
