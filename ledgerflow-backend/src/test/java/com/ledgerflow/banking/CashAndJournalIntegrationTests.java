package com.ledgerflow.banking;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static com.ledgerflow.banking.BankingApi.*;

@ActiveProfiles("test")
@SpringBootTest
@AutoConfigureMockMvc
class CashAndJournalIntegrationTests {
    @Autowired BankingService service;
    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;
    AccountView account(String balance) {
        return service.create(new CreateAccount("Customer", "MAD", new BigDecimal(balance)));
    }
    String key() { return UUID.randomUUID().toString(); }
    CashRequest amount(String value) { return new CashRequest(new BigDecimal(value)); }
    void reconcile(AccountView a, String expected) {
        BigDecimal actual = service.account(a.id()).balance();
        assertEquals(0,actual.compareTo(new BigDecimal(expected)));
        BigDecimal ledgerBalance = jdbc.queryForObject("""
            SELECT COALESCE(SUM(CASE WHEN credit_account_id = ? THEN amount ELSE -amount END),0)
            FROM journals WHERE credit_account_id = ? OR debit_account_id = ?
            """,BigDecimal.class,a.id(),a.id(),a.id());
        assertEquals(0,actual.compareTo(ledgerBalance));
    }
    void balanced(JournalView journal) {
        assertEquals(2,journal.entries().size());
        assertEquals("DEBIT",journal.entries().get(0).side());
        assertEquals("CREDIT",journal.entries().get(1).side());
        assertEquals(journal.entries().get(0).amount(),journal.entries().get(1).amount());
        assertEquals(journal.entries().get(0).currency(),journal.entries().get(1).currency());
        assertNotEquals(journal.entries().get(0).ledgerAccount(),journal.entries().get(1).ledgerAccount());
    }
    @Test void openingDepositWithdrawalAndTransferReconcile() {
        var a=account("100"); var b=account("0");
        service.deposit(a.id(),key(),amount("50"));
        service.withdraw(a.id(),key(),amount("20"));
        var transfer=service.transfer(key(),new CreateTransfer(a.id(),b.id(),new BigDecimal("30")));
        reconcile(a,"100"); reconcile(b,"30");
        var journal=service.journal(a.id(),0,100);
        assertEquals(4,journal.size()); journal.forEach(this::balanced);
        var shared=service.journal(b.id(),0,100);
        assertEquals(1,shared.size()); assertEquals(transfer.id(),shared.getFirst().id());
        assertEquals("CUSTOMER:"+a.id(),shared.getFirst().entries().getFirst().ledgerAccount());
        assertEquals("CUSTOMER:"+b.id(),shared.getFirst().entries().getLast().ledgerAccount());
    }
    @Test void zeroOpeningCreatesNoPosting() {
        var a=account("0"); assertTrue(service.journal(a.id(),0,100).isEmpty()); reconcile(a,"0");
    }
    @Test void depositAndWithdrawalRetriesReturnOriginalResult() {
        var a=account("100"); String depositKey=key(), withdrawalKey=key();
        var deposit=service.deposit(a.id(),depositKey,amount("10"));
        assertEquals(deposit,service.deposit(a.id(),depositKey,amount("10.00")));
        var withdrawal=service.withdraw(a.id(),withdrawalKey,amount("5"));
        assertEquals(withdrawal,service.withdraw(a.id(),withdrawalKey,amount("5.00")));
        reconcile(a,"105"); assertEquals(3,service.journal(a.id(),0,100).size());
    }
    @Test void retryAfterLaterOperationsStillReturnsOriginalPosting() {
        var a=account("20"); String requestKey=key();
        var first=service.withdraw(a.id(),requestKey,amount("20"));
        assertEquals(first,service.withdraw(a.id(),requestKey,amount("20")));
        service.deposit(a.id(),key(),amount("1"));
        assertEquals(first,service.withdraw(a.id(),requestKey,amount("20"))); reconcile(a,"1");
    }
    @Test void rejectedCashOperationsChangeNeitherBalanceNorJournal() {
        var a=account("10"); var max=account("99999999999999999.99");
        assertEquals("INSUFFICIENT_FUNDS",assertThrows(BankingException.class,
            () -> service.withdraw(a.id(),key(),amount("11"))).code);
        assertEquals("BALANCE_LIMIT",assertThrows(BankingException.class,
            () -> service.deposit(max.id(),key(),amount("0.01"))).code);
        reconcile(a,"10"); reconcile(max,"99999999999999999.99");
        assertEquals(1,service.journal(a.id(),0,100).size());
        assertEquals(1,service.journal(max.id(),0,100).size());
    }
    @Test void keysCannotBeReusedAcrossAccountsAmountsOrOperationTypes() {
        var a=account("100"); var b=account("0"); String requestKey=key();
        service.deposit(a.id(),requestKey,amount("10"));
        assertThrows(BankingException.class,() -> service.deposit(a.id(),requestKey,amount("11")));
        assertThrows(BankingException.class,() -> service.deposit(b.id(),requestKey,amount("10")));
        assertThrows(BankingException.class,() -> service.withdraw(a.id(),requestKey,amount("10")));
        assertThrows(BankingException.class,() -> service.transfer(requestKey,new CreateTransfer(a.id(),b.id(),new BigDecimal("10"))));
        String transferKey=key();
        service.transfer(transferKey,new CreateTransfer(a.id(),b.id(),new BigDecimal("1")));
        assertThrows(BankingException.class,() -> service.deposit(a.id(),transferKey,amount("1")));
        reconcile(a,"109"); reconcile(b,"1");
    }
    List<Object> concurrent(Callable<Object> first, Callable<Object> second) throws Exception {
        try(var pool=Executors.newFixedThreadPool(2)) {
            var gate=new CountDownLatch(1);
            var f1=pool.submit(() -> { gate.await(); return first.call(); });
            var f2=pool.submit(() -> { gate.await(); return second.call(); });
            gate.countDown(); return List.of(f1.get(15,TimeUnit.SECONDS),f2.get(15,TimeUnit.SECONDS));
        }
    }
    Object tryWithdraw(UUID accountId, String requestKey) {
        try { return service.withdraw(accountId,requestKey,amount("80")); }
        catch(BankingException e) { return e.code; }
    }
    @Test void concurrentWithdrawalsCannotOverdraw() throws Exception {
        var a=account("100");
        var results=concurrent(() -> tryWithdraw(a.id(),key()),() -> tryWithdraw(a.id(),key()));
        assertEquals(1,results.stream().filter(CashView.class::isInstance).count());
        assertTrue(results.contains("INSUFFICIENT_FUNDS")); reconcile(a,"20");
        assertEquals(2,service.journal(a.id(),0,100).size());
    }
    @Test void concurrentIdenticalDepositsPostOnce() throws Exception {
        var a=account("0"); String requestKey=key();
        var results=concurrent(() -> service.deposit(a.id(),requestKey,amount("10")),
            () -> service.deposit(a.id(),requestKey,amount("10")));
        assertEquals(results.get(0),results.get(1)); reconcile(a,"10");
        assertEquals(1,service.journal(a.id(),0,100).size());
    }
    @Test void transferAndWithdrawalCompeteForSameAvailableFunds() throws Exception {
        var a=account("100"); var b=account("0");
        var results=concurrent(() -> tryWithdraw(a.id(),key()), () -> {
            try { return service.transfer(key(),new CreateTransfer(a.id(),b.id(),new BigDecimal("80"))); }
            catch(BankingException e) { return e.code; }
        });
        assertTrue(results.contains("INSUFFICIENT_FUNDS")); reconcile(a,"20");
        reconcile(b,results.stream().anyMatch(TransferView.class::isInstance) ? "80" : "0");
    }
    @Test void simultaneousDepositAndTransferKeyCollisionRollsBackLoser() throws Exception {
        var a=account("100"); var b=account("0"); var c=account("0"); String requestKey=key();
        var results=concurrent(() -> {
            try { return service.deposit(c.id(),requestKey,amount("10")); }
            catch(BankingException | DataIntegrityViolationException e) { return "conflict"; }
        }, () -> {
            try { return service.transfer(requestKey,new CreateTransfer(a.id(),b.id(),new BigDecimal("20"))); }
            catch(BankingException | DataIntegrityViolationException e) { return "conflict"; }
        });
        assertEquals(1,results.stream().filter(r -> !(r instanceof String)).count());
        assertTrue(results.contains("conflict"));
        if(results.stream().anyMatch(CashView.class::isInstance)) {
            reconcile(a,"100"); reconcile(b,"0"); reconcile(c,"10");
            assertTrue(service.history(a.id(),0,100).isEmpty());
        } else { reconcile(a,"80"); reconcile(b,"20"); reconcile(c,"0"); }
    }
    @Test void cashApiValidationAndJournalPagination() throws Exception {
        var a=account("10");
        for(String path:List.of("deposits","withdrawals")) {
            for(String value:List.of("0","-1","1.001","100000000000000000","null")) {
                var response=mvc.perform(post("/api/accounts/"+a.id()+"/"+path)
                    .contentType("application/json").header("Idempotency-Key",key())
                    .content("{\"amount\":"+value+"}")).andReturn().getResponse();
                assertEquals(400,response.getStatus());
            }
            assertEquals(400,mvc.perform(post("/api/accounts/"+a.id()+"/"+path)
                .contentType("application/json").content("{\"amount\":1}")).andReturn().getResponse().getStatus());
            assertEquals(404,mvc.perform(post("/api/accounts/"+UUID.randomUUID()+"/"+path)
                .header("Idempotency-Key",key()).contentType("application/json")
                .content("{\"amount\":1}")).andReturn().getResponse().getStatus());
        }
        reconcile(a,"10");
        service.deposit(a.id(),key(),amount("1")); service.withdraw(a.id(),key(),amount("1"));
        assertEquals(1,service.journal(a.id(),1,1).size());
        assertEquals(400,mvc.perform(get("/api/accounts/"+a.id()+"/journal?size=101")).andReturn().getResponse().getStatus());
        assertEquals(400,mvc.perform(get("/api/accounts/"+a.id()+"/journal?page=-1")).andReturn().getResponse().getStatus());
    }
    @Test void cashApiSuccessRetryAndInsufficientFunds() throws Exception {
        var a=account("0"); String requestKey=key();
        var request=post("/api/accounts/"+a.id()+"/deposits").contentType("application/json")
            .header("Idempotency-Key",requestKey).content("{\"amount\":25}");
        var first=mvc.perform(request).andReturn().getResponse(); assertEquals(200,first.getStatus());
        assertEquals(first.getContentAsString(),mvc.perform(request).andReturn().getResponse().getContentAsString());
        assertEquals(200,mvc.perform(post("/api/accounts/"+a.id()+"/withdrawals")
            .header("Idempotency-Key",key()).contentType("application/json").content("{\"amount\":5}")).andReturn().getResponse().getStatus());
        assertEquals(409,mvc.perform(post("/api/accounts/"+a.id()+"/withdrawals")
            .header("Idempotency-Key",key()).contentType("application/json").content("{\"amount\":21}")).andReturn().getResponse().getStatus());
        reconcile(a,"20");
        assertEquals(200,mvc.perform(get("/api/accounts/"+a.id()+"/journal")).andReturn().getResponse().getStatus());
    }
    @Test void databaseRejectsMissingLegAndNonpositivePosting() {
        var a=account("0");
        assertThrows(DataIntegrityViolationException.class, () -> jdbc.update("""
            INSERT INTO journals(id,type,amount,currency,created_at) VALUES (?,'DEPOSIT',10,'MAD',CURRENT_TIMESTAMP)
            """,UUID.randomUUID()));
        assertThrows(DataIntegrityViolationException.class, () -> jdbc.update("""
            INSERT INTO journals(id,type,credit_account_id,amount,currency,created_at)
            VALUES (?,'OPENING',?,0,'MAD',CURRENT_TIMESTAMP)
            """,UUID.randomUUID(),a.id()));
        reconcile(a,"0");
    }
}
