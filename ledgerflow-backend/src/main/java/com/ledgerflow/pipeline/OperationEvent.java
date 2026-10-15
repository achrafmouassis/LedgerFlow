package com.ledgerflow.pipeline;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Versioned contract: one immutable event for each balanced journal record. */
public record OperationEvent(int schemaVersion, UUID eventId, UUID journalId,
        String operationType, UUID debitAccountId, UUID creditAccountId,
        BigDecimal amount, String currency, Instant occurredAt) {
    public OperationEvent validated() {
        if (schemaVersion != 1 || eventId == null || !eventId.equals(journalId)
                || occurredAt == null || currency == null || !currency.matches("[A-Z]{3}")
                || amount == null || amount.signum() <= 0 || amount.scale() > 2
                || amount.precision() - amount.scale() > 17 || operationType == null)
            throw new InvalidEventException("Invalid event envelope or monetary amount");
        boolean validLegs = switch (operationType) {
            case "OPENING", "DEPOSIT" -> debitAccountId == null && creditAccountId != null;
            case "WITHDRAWAL" -> debitAccountId != null && creditAccountId == null;
            case "TRANSFER" -> debitAccountId != null && creditAccountId != null
                && !debitAccountId.equals(creditAccountId);
            default -> false;
        };
        if (!validLegs) throw new InvalidEventException("Invalid operation type or ledger legs");
        return new OperationEvent(schemaVersion, eventId, journalId, operationType,
            debitAccountId, creditAccountId, amount.setScale(2), currency, occurredAt);
    }
}
