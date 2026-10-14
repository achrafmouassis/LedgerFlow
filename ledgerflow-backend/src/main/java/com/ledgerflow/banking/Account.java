package com.ledgerflow.banking;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "accounts")
public class Account {
    @Id UUID id;
    @Column(nullable = false, length = 120) String owner;
    @Column(nullable = false, length = 3) String currency;
    @Column(nullable = false, precision = 19, scale = 2) BigDecimal balance;
    protected Account() {}
    Account(String owner, String currency, BigDecimal balance) {
        this.id = UUID.randomUUID(); this.owner = owner;
        this.currency = currency; this.balance = balance;
    }
}
