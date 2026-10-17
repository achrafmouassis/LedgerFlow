package com.ledgerflow.banking;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.mock.web.MockHttpServletResponse;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import org.springframework.http.HttpStatus;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static com.ledgerflow.banking.BankingApi.*;

@ActiveProfiles("test")
@SpringBootTest
@AutoConfigureMockMvc
@org.springframework.security.test.context.support.WithMockUser(roles="OPERATOR")
class BankingIntegrationTests {
    @Autowired BankingService service;
    @Autowired MockMvc mvc;
    AccountView account(String balance) { return service.create(new CreateAccount("Test", "MAD", new BigDecimal(balance))); }
    CreateTransfer request(AccountView a, AccountView b, String amount) {
        return new CreateTransfer(a.id(), b.id(), new BigDecimal(amount));
    }
    void balance(AccountView a, String expected) {
        assertEquals(0, service.account(a.id()).balance().compareTo(new BigDecimal(expected)));
    }
    @Test void transferIsAtomicAndRetryDoesNotDuplicate() {
        var a = account("100"); var b = account("0"); String key = UUID.randomUUID().toString();
        var first = service.transfer(key, request(a, b, "25"));
        assertEquals(first, service.transfer(key, request(a, b, "25.00")));
        balance(a, "75"); balance(b, "25");
        assertEquals(1, service.history(a.id(), 0, 20).size());
        assertEquals(first, service.history(b.id(), 0, 20).getFirst());
    }
    @Test void insufficientFundsDoNotChangeBalancesOrHistory() {
        var a = account("10"); var b = account("0");
        var e = assertThrows(BankingException.class, () -> service.transfer(UUID.randomUUID().toString(), request(a,b,"11")));
        assertEquals("INSUFFICIENT_FUNDS", e.code);
        balance(a,"10"); balance(b,"0"); assertTrue(service.history(a.id(),0,20).isEmpty());
    }
    @Test void sameKeyWithDifferentPayloadIsRejected() {
        var a = account("100"); var b = account("0"); String key = UUID.randomUUID().toString();
        service.transfer(key,request(a,b,"10"));
        var e = assertThrows(BankingException.class, () -> service.transfer(key,request(a,b,"20")));
        assertEquals("IDEMPOTENCY_CONFLICT",e.code); balance(a,"90"); balance(b,"10");
    }
    @Test void rejectsCurrencyMismatchAndSelfTransfer() {
        var a = account("100");
        var b = service.create(new CreateAccount("Euro", "EUR", BigDecimal.ZERO));
        assertEquals("CURRENCY_MISMATCH", assertThrows(BankingException.class,
            () -> service.transfer(UUID.randomUUID().toString(), request(a,b,"1"))).code);
        assertEquals("SAME_ACCOUNT", assertThrows(BankingException.class,
            () -> service.transfer(UUID.randomUUID().toString(), request(a,a,"1"))).code);
        balance(a,"100"); balance(b,"0");
    }
    @Test void missingAccountIsNotFound() {
        assertEquals(HttpStatus.NOT_FOUND, assertThrows(BankingException.class,
            () -> service.account(UUID.randomUUID())).status);
    }
    @Test void destinationOverflowRollsBack() {
        var a=account("10"); var b=account("99999999999999999.99");
        assertEquals("BALANCE_LIMIT", assertThrows(BankingException.class,
            () -> service.transfer(UUID.randomUUID().toString(),request(a,b,"1"))).code);
        balance(a,"10"); balance(b,"99999999999999999.99");
    }
    List<Object> concurrent(Callable<Object> first, Callable<Object> second) throws Exception {
        try (var pool = Executors.newFixedThreadPool(2)) {
            var gate = new CountDownLatch(1);
            Callable<Object> one = () -> { gate.await(); return first.call(); };
            Callable<Object> two = () -> { gate.await(); return second.call(); };
            var f1=pool.submit(one); var f2=pool.submit(two); gate.countDown();
            return List.of(f1.get(15,TimeUnit.SECONDS), f2.get(15,TimeUnit.SECONDS));
        }
    }
    Object attempt(String key, CreateTransfer request) {
        try { return service.transfer(key,request); }
        catch (BankingException e) { return e.code; }
    }
    @Test void concurrentSpendingCannotOverdraw() throws Exception {
        var a=account("100"); var b=account("0"); var c=account("0");
        var results=concurrent(() -> attempt(UUID.randomUUID().toString(),request(a,b,"80")),
                               () -> attempt(UUID.randomUUID().toString(),request(a,c,"80")));
        assertEquals(1,results.stream().filter(TransferView.class::isInstance).count());
        assertTrue(results.contains("INSUFFICIENT_FUNDS")); balance(a,"20");
        assertEquals(0,service.account(b.id()).balance().add(service.account(c.id()).balance()).compareTo(new BigDecimal("80")));
    }
    @Test void concurrentIdenticalRetriesExecuteOnce() throws Exception {
        var a=account("100"); var b=account("0"); String key=UUID.randomUUID().toString();
        var results=concurrent(() -> service.transfer(key,request(a,b,"30")),
                               () -> service.transfer(key,request(a,b,"30")));
        assertEquals(results.get(0),results.get(1)); balance(a,"70"); balance(b,"30");
        assertEquals(1,service.history(a.id(),0,20).size());
    }
    @Test void concurrentKeyCollisionAcrossUnrelatedAccountsRollsBackLoser() throws Exception {
        var a=account("100"); var b=account("0"); var c=account("100"); var d=account("0");
        String key=UUID.randomUUID().toString();
        Callable<Object> first=() -> collisionAttempt(key,request(a,b,"10"));
        Callable<Object> second=() -> collisionAttempt(key,request(c,d,"20"));
        var results=concurrent(first,second);
        assertEquals(1,results.stream().filter(TransferView.class::isInstance).count());
        assertTrue(results.contains("conflict"));
        var winner=(TransferView)results.stream().filter(TransferView.class::isInstance).findFirst().orElseThrow();
        if(winner.sourceAccountId().equals(a.id())) {
            balance(a,"90"); balance(b,"10"); balance(c,"100"); balance(d,"0");
        } else {
            balance(a,"100"); balance(b,"0"); balance(c,"80"); balance(d,"20");
        }
    }
    Object collisionAttempt(String key, CreateTransfer request) {
        try { return service.transfer(key,request); }
        catch (org.springframework.dao.DataIntegrityViolationException | BankingException e) { return "conflict"; }
    }
    @Test void oppositeTransfersCompleteWithoutDeadlock() throws Exception {
        var a=account("100"); var b=account("100");
        concurrent(() -> service.transfer(UUID.randomUUID().toString(),request(a,b,"10")),
                   () -> service.transfer(UUID.randomUUID().toString(),request(b,a,"20")));
        balance(a,"110"); balance(b,"90");
    }
    MockHttpServletResponse post(String path, String json, String key) throws Exception {
        var builder = org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api"+path)
            .contentType("application/json").content(json);
        if (key != null) builder.header("Idempotency-Key",key);
        return mvc.perform(builder).andReturn().getResponse();
    }
    @Test void apiValidatesAmountsAndRequiresKey() throws Exception {
        var a=account("100"); var b=account("0");
        String prefix="{\"sourceAccountId\":\""+a.id()+"\",\"destinationAccountId\":\""+b.id()+"\",\"amount\":";
        for(String amount:List.of("0","-1","1.001","100000000000000000"))
            assertEquals(400,post("/transfers",prefix+amount+"}",UUID.randomUUID().toString()).getStatus());
        assertEquals(400,post("/transfers",prefix+"1}",null).getStatus());
        assertEquals(400,post("/accounts","{\"owner\":\" \",\"currency\":\"MAD\",\"openingBalance\":0}",null).getStatus());
        balance(a,"100"); balance(b,"0");
    }
    @Test void apiCreatesAccountsAndExecutesIdempotentTransfer() throws Exception {
        var created=post("/accounts","{\"owner\":\"Alice\",\"currency\":\"MAD\",\"openingBalance\":100}",null);
        assertEquals(201,created.getStatus()); assertNotNull(created.getHeader("Location"));
        var a=account("100"); var b=account("0"); String key=UUID.randomUUID().toString();
        String json="{\"sourceAccountId\":\""+a.id()+"\",\"destinationAccountId\":\""+b.id()+"\",\"amount\":10}";
        var first=post("/transfers",json,key); assertEquals(200,first.getStatus());
        assertEquals(first.getContentAsString(),post("/transfers",json,key).getContentAsString());
        var response = mvc.perform(get("/api/accounts/"+a.id()+"/transfers?size=101")).andReturn().getResponse();
        assertEquals(400,response.getStatus());
    }
}
