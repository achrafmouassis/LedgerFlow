package com.ledgerflow.banking;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transfers")
public class Transfer {
    @Id UUID id;
    @Column(nullable = false, unique = true, length = 100) String idempotencyKey;
    @Column(nullable = false) UUID sourceAccountId;
    @Column(nullable = false) UUID destinationAccountId;
    @Column(nullable = false, precision = 19, scale = 2) BigDecimal amount;
    @Column(nullable = false, length = 3) String currency;
    @Column(nullable = false) Instant createdAt;
    protected Transfer() {}
    Transfer(String key, Account source, Account destination, BigDecimal amount) {
        id = UUID.randomUUID(); idempotencyKey = key;
        sourceAccountId = source.id; destinationAccountId = destination.id;
        this.amount = amount; currency = source.currency; createdAt = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
    }
}
