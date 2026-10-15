package com.ledgerflow.pipeline;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxCapture {
    private final JdbcTemplate jdbc;
    public OutboxCapture(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    /** Must join the banking transaction; no independent event can be committed. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void record(OperationEvent event) {
        OperationEvent e = event.validated();
        jdbc.update("""
            INSERT INTO outbox_events(event_id,journal_id,schema_version,operation_type,
                debit_account_id,credit_account_id,amount,currency,occurred_at,
                created_at,next_attempt_at,attempts)
            VALUES (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0)
            """, e.eventId(), e.journalId(), e.schemaVersion(), e.operationType(),
            e.debitAccountId(), e.creditAccountId(), e.amount(), e.currency(),
            java.sql.Timestamp.from(e.occurredAt()));
    }
}
