package com.ledgerflow.banking;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import static com.ledgerflow.banking.BankingApi.*;

@Service
@Transactional
public class BankingService {
    private static final BigDecimal MAX_BALANCE = new BigDecimal("99999999999999999.99");
    private final AccountRepository accounts;
    private final TransferRepository transfers;
    private final JournalRepository journals;
    public BankingService(AccountRepository accounts, TransferRepository transfers, JournalRepository journals) {
        this.accounts = accounts; this.transfers = transfers; this.journals = journals;
    }
    public AccountView create(CreateAccount request) {
        Account account = accounts.save(new Account(request.owner().strip(), request.currency(),
            money(request.openingBalance(), true)));
        if (account.balance.signum() > 0) journals.save(Journal.opening(account));
        return AccountView.of(account);
    }
    @Transactional(readOnly = true)
    public AccountView account(UUID id) { return AccountView.of(require(id)); }
    @Transactional(readOnly = true)
    public List<TransferView> history(UUID id, int page, int size) {
        require(id);
        return transfers.history(id, PageRequest.of(page, size)).stream().map(TransferView::of).toList();
    }
    public TransferView transfer(String key, CreateTransfer request) {
        validateKey(key);
        BigDecimal amount = money(request.amount(), false);
        if (request.sourceAccountId().equals(request.destinationAccountId()))
            throw error(HttpStatus.BAD_REQUEST, "SAME_ACCOUNT", "Source and destination must be different");
        // Always lock in the same order to avoid deadlocks for opposite-direction transfers.
        List<UUID> ids = new ArrayList<>(List.of(request.sourceAccountId(), request.destinationAccountId()));
        ids.sort(Comparator.naturalOrder());
        Map<UUID, Account> locked = new HashMap<>();
        for (UUID id : ids) locked.put(id, accounts.findLocked(id).orElseThrow(() -> missing()));
        // Check after locking: identical concurrent requests now see the committed first transfer.
        Optional<Journal> previousJournal = journals.findByIdempotencyKey(key);
        if (previousJournal.isPresent() && previousJournal.get().type != Journal.Type.TRANSFER)
            throw idempotencyConflict();
        Optional<Transfer> existing = transfers.findByIdempotencyKey(key);
        if (existing.isPresent()) {
            Transfer t = existing.get();
            if (!t.sourceAccountId.equals(request.sourceAccountId()) ||
                !t.destinationAccountId.equals(request.destinationAccountId()) ||
                t.amount.compareTo(request.amount()) != 0)
                throw error(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT", "This key was used for a different transfer");
            return TransferView.of(t);
        }
        Account source = locked.get(request.sourceAccountId());
        Account destination = locked.get(request.destinationAccountId());
        if (!source.currency.equals(destination.currency))
            throw error(HttpStatus.BAD_REQUEST, "CURRENCY_MISMATCH", "Accounts must have the same currency");
        if (source.balance.compareTo(amount) < 0)
            throw error(HttpStatus.CONFLICT, "INSUFFICIENT_FUNDS", "Insufficient funds");
        if (destination.balance.add(amount).compareTo(MAX_BALANCE) > 0)
            throw error(HttpStatus.CONFLICT, "BALANCE_LIMIT", "Destination balance limit exceeded");
        source.balance = source.balance.subtract(amount);
        destination.balance = destination.balance.add(amount);
        // The unique key is a database-level guard, including requests using unrelated accounts.
        Transfer transfer = transfers.saveAndFlush(new Transfer(key, source, destination, amount));
        journals.saveAndFlush(Journal.transfer(transfer));
        return TransferView.of(transfer);
    }
    public CashView deposit(UUID id, String key, CashRequest request) {
        return cash(id, key, request, Journal.Type.DEPOSIT);
    }
    public CashView withdraw(UUID id, String key, CashRequest request) {
        return cash(id, key, request, Journal.Type.WITHDRAWAL);
    }
    @Transactional(readOnly = true)
    public List<JournalView> journal(UUID id, int page, int size) {
        require(id);
        return journals.history(id, PageRequest.of(page, size)).stream().map(JournalView::of).toList();
    }
    private CashView cash(UUID id, String key, CashRequest request, Journal.Type type) {
        validateKey(key);
        BigDecimal amount = money(request.amount(), false);
        Account account = accounts.findLocked(id).orElseThrow(this::missing);
        Optional<Journal> existing = journals.findByIdempotencyKey(key);
        if (existing.isPresent()) {
            Journal j = existing.get();
            UUID owner = type == Journal.Type.DEPOSIT ? j.creditAccountId : j.debitAccountId;
            if (j.type != type || !id.equals(owner) || j.amount.compareTo(request.amount()) != 0)
                throw idempotencyConflict();
            return CashView.of(j);
        }
        if (type == Journal.Type.WITHDRAWAL && account.balance.compareTo(amount) < 0)
            throw error(HttpStatus.CONFLICT, "INSUFFICIENT_FUNDS", "Insufficient funds");
        BigDecimal next = type == Journal.Type.DEPOSIT ? account.balance.add(amount) : account.balance.subtract(amount);
        if (next.compareTo(MAX_BALANCE) > 0)
            throw error(HttpStatus.CONFLICT, "BALANCE_LIMIT", "Account balance limit exceeded");
        account.balance = next;
        return CashView.of(journals.saveAndFlush(Journal.cash(account, type, key, amount)));
    }
    private BigDecimal money(BigDecimal value, boolean allowZero) {
        if (value == null || value.signum() < 0 || (!allowZero && value.signum() == 0)
                || value.scale() > 2 || value.precision() - value.scale() > 17)
            throw error(HttpStatus.BAD_REQUEST, "INVALID_AMOUNT",
                "Amount must be within 17 integer digits and 2 decimal places and have a valid sign");
        return value.setScale(2, RoundingMode.UNNECESSARY);
    }
    private void validateKey(String key) {
        if (key == null || !key.matches("[A-Za-z0-9._:-]{1,100}"))
            throw error(HttpStatus.BAD_REQUEST, "INVALID_IDEMPOTENCY_KEY", "Provide an Idempotency-Key of 1-100 letters, digits or ._:-");
    }
    private BankingException idempotencyConflict() {
        return error(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT", "This key was used for a different operation");
    }
    private Account require(UUID id) { return accounts.findById(id).orElseThrow(() -> missing()); }
    private BankingException missing() { return error(HttpStatus.NOT_FOUND, "ACCOUNT_NOT_FOUND", "Account not found"); }
    private BankingException error(HttpStatus status, String code, String message) {
        return new BankingException(status, code, message);
    }
}
