# Banking API

This educational API simulates accounts and same-currency transfers. Opening
balances are simulation funds. Authentication and account ownership checks are
not implemented yet: run locally, never expose it publicly or use real funds.
The journal records balanced debit/credit pairs for opening funds, deposits,
withdrawals and transfers. Account balances are a projection of the customer
credit postings minus customer debit postings.

## Launch

To run both the API and PostgreSQL entirely in Docker (no local Java required):

```powershell
docker compose --profile app up --build -d
```

The optional `app` profile binds the backend to localhost:8080. This also avoids
local Windows Java NIO/Unix-domain socket failures such as
`Unable to establish loopback connection` when starting Tomcat. Use either this
option or the local Java launch below; do not run both on port 8080.

Requirements: Java 21 (JAVA_HOME configured), Docker with its engine running.
From the repository root in PowerShell:

```powershell
docker compose up -d --wait
cd ledgerflow-backend
.\mvnw.cmd spring-boot:run
```

Flyway creates the schema automatically. Hibernate validates it. API port: 8080.
Health: `http://localhost:8080/actuator/health`.

## Endpoints

| Method | Path | Result |
| --- | --- | --- |
| POST | /api/accounts | Create account; 201 and Location header |
| GET | /api/accounts/{id} | Owner, currency and current balance |
| POST | /api/transfers | Execute or replay transfer; 200 |
| POST | /api/accounts/{id}/deposits | Deposit simulated cash; 200 |
| POST | /api/accounts/{id}/withdrawals | Withdraw simulated cash; 200 |
| GET | /api/accounts/{id}/journal?page=0&size=20 | Balanced journal records involving the account |
| GET | /api/accounts/{id}/transfers?page=0&size=20 | Incoming and outgoing transfers, newest first |

Pages start at zero; size is 1-100. Currency is three uppercase letters. Amounts
accept at most 17 integer digits and 2 fractional digits. Opening balances must
be nonnegative and transfer amounts positive. Foreign exchange is not supported.

## Example (PowerShell, separate terminal)

```powershell
$alice = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/accounts -ContentType application/json -Body '{"owner":"Alice","currency":"MAD","openingBalance":1000.00}'
$bob = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/accounts -ContentType application/json -Body '{"owner":"Bob","currency":"MAD","openingBalance":0.00}'
$transferBody = @{ sourceAccountId=$alice.id; destinationAccountId=$bob.id; amount=150.00 } | ConvertTo-Json
$headers = @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/transfers -ContentType application/json -Headers $headers -Body $transferBody
# Replay: same transfer ID, no additional debit or credit.
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/transfers -ContentType application/json -Headers $headers -Body $transferBody
Invoke-RestMethod -Uri "http://localhost:8080/api/accounts/$($alice.id)"
Invoke-RestMethod -Uri "http://localhost:8080/api/accounts/$($bob.id)"
Invoke-RestMethod -Uri "http://localhost:8080/api/accounts/$($alice.id)/transfers"
```

Expected balances: Alice 850.00 MAD, Bob 150.00 MAD.

## Reliability

- Debit, credit, transfer history and journal insertion share one transaction.
- Deposits and withdrawals use the same account locks and global idempotency-key
  namespace as transfers. Reusing a key across operation types is a conflict.
- Customer funds are bank liabilities: a customer credit increases the balance;
  a debit decreases it. Deposits debit bank cash and credit customer funds;
  withdrawals reverse that pair. Transfers debit source and credit destination.
- Each journal row stores both legs with a shared positive amount and currency.
  Database checks require the appropriate counterpart for each operation type,
  preventing missing or unbalanced legs. API responses expose the two postings as
  `CUSTOMER:<uuid>` or the per-currency clearing account `CASH:<currency>`.
- The journal is immutable through the application and has no update/delete API.
  Database administrators can still edit it; this is not a tamper-proof audit log.
- A nonzero opening balance produces an opening journal. The V2 migration
  recovers old opening funds and backfills transfer journals without changing
  balances. Backfilled opening dates are migration dates, as V1 stored no creation date.
- Pessimistic account locks are acquired in UUID order to prevent lost updates
  and opposite-direction lock-order deadlocks. Balances cannot go negative.
- `Idempotency-Key` is required: 1-100 letters, digits or `._:-`. Same accounts
  and numerical amount return the original result. Another payload returns 409.
- A global database unique constraint protects keys even across unrelated
  accounts. Any conflicting transaction rolls back.
- Missing accounts return 404. Insufficient funds and destination balance
  overflow return 409. Self-transfers, different currencies and invalid input
  return 400.
- Transient database failures and lock timeouts are still possible; retry using
  the same key and payload. Keys are retained permanently in this increment.

## Tests

From `ledgerflow-backend`: `.\mvnw.cmd test`.
The test profile uses H2 in PostgreSQL compatibility mode and the same Flyway
migration. Tests cover API validation, history, atomicity, insufficient funds,
idempotency, balance overflow, simultaneous cash operations/transfers, journal
reconciliation, structural database constraints and migration from existing V1 data. H2 does not replace
verification against PostgreSQL.

GitHub Actions runs the complete suite on H2 and PostgreSQL 18 with Java 21.

Validation on 2026-10-14: all 27 tests passed on both databases. A real HTTP smoke
test in Java 21/Linux with PostgreSQL verified opening funds, deposit/replay,
withdrawal, transfer, expected balances and all four balanced journal records.
The multi-stage Docker image also builds successfully.

To run against a dedicated disposable PostgreSQL database instead, set
`TEST_DB_URL`, `TEST_DB_USERNAME`, and `TEST_DB_PASSWORD` before running the same
command. Tests persist synthetic data; never point them at production or valuable
data. Example URL: `jdbc:postgresql://localhost:55432/ledgerflow_test`.

## Try cash operations

Use a new key for each new operation, keeping it for retries:

```powershell
$depositHeaders = @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/accounts/$($bob.id)/deposits" -ContentType application/json -Headers $depositHeaders -Body '{"amount":50.00}'
$withdrawalHeaders = @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/accounts/$($bob.id)/withdrawals" -ContentType application/json -Headers $withdrawalHeaders -Body '{"amount":20.00}'
Invoke-RestMethod -Uri "http://localhost:8080/api/accounts/$($bob.id)/journal"
```

After the transfer example above, Bob now has 180.00 MAD. Both new operations
return their original operation ID and date when retried with the same key/body.

Next increments: Kafka/Outbox and data pipelines, followed by security.
