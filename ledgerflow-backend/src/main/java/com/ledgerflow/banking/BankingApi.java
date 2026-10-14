package com.ledgerflow.banking;

import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.List;

public final class BankingApi {
    private BankingApi() {}
    public record CreateAccount(
        @NotBlank @Size(max = 120) String owner,
        @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
        @NotNull @DecimalMin("0.00") @Digits(integer = 17, fraction = 2) BigDecimal openingBalance) {}
    public record AccountView(UUID id, String owner, String currency, BigDecimal balance) {
        static AccountView of(Account a) { return new AccountView(a.id, a.owner, a.currency, a.balance); }
    }
    public record CreateTransfer(
        @NotNull UUID sourceAccountId, @NotNull UUID destinationAccountId,
        @NotNull @DecimalMin(value = "0.00", inclusive = false)
        @Digits(integer = 17, fraction = 2) BigDecimal amount) {}
    public record TransferView(UUID id, UUID sourceAccountId, UUID destinationAccountId,
                               BigDecimal amount, String currency, Instant createdAt) {
        static TransferView of(Transfer t) {
            return new TransferView(t.id, t.sourceAccountId, t.destinationAccountId,
                                    t.amount, t.currency, t.createdAt);
        }
    }
    public record CashRequest(
        @NotNull @DecimalMin(value = "0.00", inclusive = false)
        @Digits(integer = 17, fraction = 2) BigDecimal amount) {}
    public record CashView(UUID id, UUID accountId, Journal.Type type,
                           BigDecimal amount, String currency, Instant createdAt) {
        static CashView of(Journal j) {
            return new CashView(j.id, j.type == Journal.Type.DEPOSIT ? j.creditAccountId : j.debitAccountId,
                j.type, j.amount, j.currency, j.createdAt);
        }
    }
    public record Posting(String ledgerAccount, String side, BigDecimal amount, String currency) {}
    public record JournalView(UUID id, Journal.Type type, Instant createdAt, List<Posting> entries) {
        static JournalView of(Journal j) {
            return new JournalView(j.id, j.type, j.createdAt, List.of(
                new Posting(ledger(j.debitAccountId, j.currency), "DEBIT", j.amount, j.currency),
                new Posting(ledger(j.creditAccountId, j.currency), "CREDIT", j.amount, j.currency)));
        }
        private static String ledger(UUID accountId, String currency) {
            return accountId == null ? "CASH:" + currency : "CUSTOMER:" + accountId;
        }
    }

}
