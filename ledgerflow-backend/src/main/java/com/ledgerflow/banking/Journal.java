package com.ledgerflow.banking;

import jakarta.persistence.*;
import org.hibernate.annotations.Immutable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/** A balanced two-leg posting; both legs always share amount and currency. */
@Entity
@Immutable
@Table(name = "journals")
public class Journal {
    public enum Type { OPENING, DEPOSIT, WITHDRAWAL, TRANSFER }
    @Id UUID id;
    @Column(unique = true, length = 100) String idempotencyKey;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20) Type type;
    UUID debitAccountId;
    UUID creditAccountId;
    @Column(nullable = false, precision = 19, scale = 2) BigDecimal amount;
    @Column(nullable = false, length = 3) String currency;
    @Column(nullable = false) Instant createdAt;
    @Column(unique = true) UUID transferId;
    protected Journal() {}
    Journal(UUID id, String key, Type type, UUID debitAccountId, UUID creditAccountId,
            BigDecimal amount, String currency, Instant createdAt, UUID transferId) {
        this.id = id; this.idempotencyKey = key; this.type = type;
        this.debitAccountId = debitAccountId; this.creditAccountId = creditAccountId;
        this.amount = amount; this.currency = currency;
        this.createdAt = createdAt.truncatedTo(ChronoUnit.MICROS); this.transferId = transferId;
    }
    static Journal opening(Account account) {
        return new Journal(UUID.randomUUID(), null, Type.OPENING, null, account.id,
            account.balance, account.currency, Instant.now(), null);
    }
    static Journal transfer(Transfer transfer) {
        return new Journal(transfer.id, transfer.idempotencyKey, Type.TRANSFER,
            transfer.sourceAccountId, transfer.destinationAccountId, transfer.amount,
            transfer.currency, transfer.createdAt, transfer.id);
    }
    static Journal cash(Account account, Type type, String key, BigDecimal amount) {
        return new Journal(UUID.randomUUID(), key, type,
            type == Type.WITHDRAWAL ? account.id : null,
            type == Type.DEPOSIT ? account.id : null,
            amount, account.currency, Instant.now(), null);
    }
}
