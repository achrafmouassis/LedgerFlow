package com.ledgerflow.banking;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.*;
import java.net.URI;
import java.util.*;
import static com.ledgerflow.banking.BankingApi.*;

@RestController
@RequestMapping("/api")
public class BankingController {
    private final BankingService service;
    private final com.ledgerflow.security.BankingAccess access;
    public BankingController(BankingService service,com.ledgerflow.security.BankingAccess access) { this.service = service; this.access=access; }
    @PostMapping("/accounts")
    public ResponseEntity<AccountView> create(@Valid @RequestBody CreateAccount request) {
        access.operator();
        AccountView account = service.create(request);
        return ResponseEntity.created(URI.create("/api/accounts/" + account.id())).body(account);
    }
    @GetMapping("/accounts/{id}")
    public AccountView account(@PathVariable UUID id) { access.read(id); return service.account(id); }
    @GetMapping("/accounts/{id}/transfers")
    public List<TransferView> history(@PathVariable UUID id,
        @RequestParam(defaultValue = "0") @Min(0) int page,
        @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        access.read(id); return service.history(id, page, size);
    }
    @PostMapping("/transfers")
    public TransferView transfer(@RequestHeader("Idempotency-Key") String key,
                                 @Valid @RequestBody CreateTransfer request) {
        access.transfer(request.sourceAccountId()); return service.transfer(key, request);
    }
    @PostMapping("/accounts/{id}/deposits")
    public CashView deposit(@PathVariable UUID id, @RequestHeader("Idempotency-Key") String key,
                            @Valid @RequestBody CashRequest request) {
        access.operator(); return service.deposit(id, key, request);
    }
    @PostMapping("/accounts/{id}/withdrawals")
    public CashView withdraw(@PathVariable UUID id, @RequestHeader("Idempotency-Key") String key,
                             @Valid @RequestBody CashRequest request) {
        access.operator(); return service.withdraw(id, key, request);
    }
    @GetMapping("/accounts/{id}/journal")
    public List<JournalView> journal(@PathVariable UUID id,
        @RequestParam(defaultValue = "0") @Min(0) int page,
        @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        access.read(id); return service.journal(id, page, size);
    }

}
