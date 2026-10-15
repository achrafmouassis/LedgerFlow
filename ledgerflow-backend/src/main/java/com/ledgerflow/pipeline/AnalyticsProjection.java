package com.ledgerflow.pipeline;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.time.ZoneOffset;
import java.sql.Timestamp;

@Service
public class AnalyticsProjection {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    public AnalyticsProjection(JdbcTemplate jdbc, ObjectMapper mapper) { this.jdbc = jdbc; this.mapper = mapper; }

    /** Inbox and fact share a transaction. Replay cannot double-count an operation. */
    @Transactional
    public void apply(OperationEvent input) {
        OperationEvent e = input.validated();
        String hash = hash(e);
        int inserted = jdbc.update("""
            INSERT INTO analytics.processed_events(event_id,payload_hash,processed_at)
            VALUES (?,?,CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING
            """,e.eventId(),hash);
        if (inserted == 0) {
            String original = jdbc.queryForObject("SELECT payload_hash FROM analytics.processed_events WHERE event_id=?",String.class,e.eventId());
            if (!hash.equals(original)) throw new InvalidEventException("Event ID reused with a different payload");
            return;
        }
        jdbc.update("""
            INSERT INTO analytics.transaction_facts(event_id,journal_id,operation_type,
                debit_account_id,credit_account_id,amount,currency,occurred_at,event_date)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,e.eventId(),e.journalId(),e.operationType(),e.debitAccountId(),e.creditAccountId(),
            e.amount(),e.currency(),Timestamp.from(e.occurredAt()),
            e.occurredAt().atZone(ZoneOffset.UTC).toLocalDate());
    }
    private String hash(OperationEvent event) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(mapper.writeValueAsBytes(event)));
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable",failure);
        }
    }
}
