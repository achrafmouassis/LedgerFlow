package com.ledgerflow.pipeline;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class OutboxRelay {
    public enum Result { EMPTY, PUBLISHED, RETRY_SCHEDULED }
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private final JdbcTemplate jdbc;
    public OutboxRelay(JdbcTemplate jdbc) { this.jdbc = jdbc; }
    private record Pending(OperationEvent event, int attempts) {}

    /** One locked row per transaction. Multiple relays can work without sharing a row. */
    @Transactional
    public Result publishNext(EventTransport transport) {
        List<Pending> rows = jdbc.query("""
            SELECT * FROM outbox_events
            WHERE published_at IS NULL AND next_attempt_at <= CURRENT_TIMESTAMP
            ORDER BY occurred_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED
            """, (rs,n) -> new Pending(new OperationEvent(rs.getInt("schema_version"),
                rs.getObject("event_id",java.util.UUID.class),rs.getObject("journal_id",java.util.UUID.class),
                rs.getString("operation_type"),rs.getObject("debit_account_id",java.util.UUID.class),
                rs.getObject("credit_account_id",java.util.UUID.class),rs.getBigDecimal("amount"),
                rs.getString("currency"),rs.getTimestamp("occurred_at").toInstant()),rs.getInt("attempts")));
        if (rows.isEmpty()) return Result.EMPTY;
        Pending row = rows.getFirst();
        try {
            transport.send(row.event());
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            long delay = Math.min(300, 1L << Math.min(row.attempts(),8));
            jdbc.update("""
                UPDATE outbox_events SET attempts=attempts+1,last_error=?,next_attempt_at=?
                WHERE event_id=?
                """, failure.getClass().getSimpleName(),Timestamp.from(Instant.now().plusSeconds(delay)),row.event().eventId());
            log.warn("Outbox delivery failed for {}; retry in {}s ({})",
                row.event().eventId(),delay,failure.getClass().getSimpleName());
            return Result.RETRY_SCHEDULED;
        }
        // Broker acknowledged. A crash before this DB commit may cause redelivery.
        jdbc.update("""
            UPDATE outbox_events SET published_at=CURRENT_TIMESTAMP,attempts=attempts+1,last_error=NULL
            WHERE event_id=?
            """,row.event().eventId());
        return Result.PUBLISHED;
    }
}
