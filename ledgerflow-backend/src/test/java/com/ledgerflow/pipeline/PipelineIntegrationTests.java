package com.ledgerflow.pipeline;

import com.ledgerflow.banking.BankingService;
import static com.ledgerflow.banking.BankingApi.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.dao.DataIntegrityViolationException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

@ActiveProfiles("test")
@SpringBootTest
@AutoConfigureMockMvc
@org.springframework.security.test.context.support.WithMockUser(roles="OPERATOR")
class PipelineIntegrationTests {
    @Autowired BankingService banking;
    @Autowired OutboxCapture capture;
    @Autowired OutboxRelay relay;
    @Autowired AnalyticsProjection projection;
    @Autowired AnalyticsController analytics;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactions;
    @Autowired MockMvc mvc;
    AccountView account(String balance) { return banking.create(new CreateAccount("Pipeline", "MAD", new BigDecimal(balance))); }
    String key() { return UUID.randomUUID().toString(); }
    CashRequest cash(String amount) { return new CashRequest(new BigDecimal(amount)); }
    long count(String table,UUID id) { return jdbc.queryForObject("SELECT COUNT(*) FROM "+table+" WHERE event_id=?",Long.class,id); }
    OperationEvent event(UUID id) {
        return jdbc.queryForObject("SELECT * FROM outbox_events WHERE event_id=?",(rs,n) ->
            new OperationEvent(rs.getInt("schema_version"),id,rs.getObject("journal_id",UUID.class),
                rs.getString("operation_type"),rs.getObject("debit_account_id",UUID.class),rs.getObject("credit_account_id",UUID.class),
                rs.getBigDecimal("amount"),rs.getString("currency"),rs.getTimestamp("occurred_at").toInstant()),id);
    }
    void publishThrough(UUID target,EventTransport transport) {
        boolean[] delivered={false};
        for(int i=0;i<1000;i++) {
            OutboxRelay.Result result=relay.publishNext(e -> {
                if(e.eventId().equals(target)) { delivered[0]=true; transport.send(e); }
            });
            if(delivered[0]) return;
            if(result==OutboxRelay.Result.EMPTY) break;
        }
        fail("Target event was not available to relay");
    }
    @Test void oneOutboxEventPerJournalIncludingOpeningAndNoDuplicateOnRetry() {
        var a=account("100"); var b=account("0"); String requestKey=key();
        var deposit=banking.deposit(a.id(),requestKey,cash("10"));
        banking.deposit(a.id(),requestKey,cash("10.00"));
        var withdrawal=banking.withdraw(a.id(),key(),cash("5"));
        var transfer=banking.transfer(key(),new CreateTransfer(a.id(),b.id(),new BigDecimal("20")));
        var journals=banking.journal(a.id(),0,100);
        assertEquals(4,journals.size());
        journals.forEach(j -> assertEquals(1,count("outbox_events",j.id())));
        assertEquals("DEPOSIT",event(deposit.id()).operationType());
        assertEquals("WITHDRAWAL",event(withdrawal.id()).operationType());
        assertEquals("TRANSFER",event(transfer.id()).operationType());
    }
    @Test void failedBusinessOperationCreatesNoEvent() {
        var a=account("1");
        long before=jdbc.queryForObject("SELECT COUNT(*) FROM outbox_events",Long.class);
        assertThrows(RuntimeException.class,() -> banking.withdraw(a.id(),key(),cash("2")));
        assertEquals(before,jdbc.queryForObject("SELECT COUNT(*) FROM outbox_events",Long.class));
        assertEquals(new BigDecimal("1.00"),banking.account(a.id()).balance());
    }
    @Test void failureToPersistOutboxRollsBackBalanceJournalAndEvent() {
        var a=account("0"); UUID[] id={null};
        assertThrows(DataIntegrityViolationException.class,() -> new TransactionTemplate(transactions).execute(status -> {
            var deposit=banking.deposit(a.id(),key(),cash("10")); id[0]=deposit.id();
            capture.record(event(deposit.id())); // Deliberate duplicate PK failure inside the transaction.
            return null;
        }));
        assertEquals(new BigDecimal("0.00"),banking.account(a.id()).balance());
        assertTrue(banking.journal(a.id(),0,100).isEmpty()); assertEquals(0,count("outbox_events",id[0]));
    }
    @Test void captureRequiresExistingBankingTransaction() {
        UUID id=UUID.randomUUID();
        var e=new OperationEvent(1,id,id,"DEPOSIT",null,UUID.randomUUID(),BigDecimal.ONE,"MAD",Instant.now());
        assertThrows(IllegalTransactionStateException.class,() -> capture.record(e));
    }
    @Test void brokerFailureKeepsEventPendingAndRetryPublishesIt() {
        var a=account("0"); var deposit=banking.deposit(a.id(),key(),cash("10"));
        publishThrough(deposit.id(),e -> { throw new java.io.IOException("broker unavailable"); });
        assertNull(jdbc.queryForObject("SELECT published_at FROM outbox_events WHERE event_id=?",Object.class,deposit.id()));
        assertEquals(1,jdbc.queryForObject("SELECT attempts FROM outbox_events WHERE event_id=?",Integer.class,deposit.id()));
        assertEquals("IOException",jdbc.queryForObject("SELECT last_error FROM outbox_events WHERE event_id=?",String.class,deposit.id()));
        jdbc.update("UPDATE outbox_events SET next_attempt_at=CURRENT_TIMESTAMP WHERE event_id=?",deposit.id());
        publishThrough(deposit.id(),e -> {});
        assertNotNull(jdbc.queryForObject("SELECT published_at FROM outbox_events WHERE event_id=?",Object.class,deposit.id()));
        assertEquals(2,jdbc.queryForObject("SELECT attempts FROM outbox_events WHERE event_id=?",Integer.class,deposit.id()));
    }
    @Test void crashAfterBrokerDeliveryBeforeDatabaseCommitIsSafeToReplay() {
        var a=account("0"); var deposit=banking.deposit(a.id(),key(),cash("10"));
        List<OperationEvent> delivered=new ArrayList<>();
        new TransactionTemplate(transactions).executeWithoutResult(status -> {
            publishThrough(deposit.id(),delivered::add); status.setRollbackOnly();
        });
        assertNull(jdbc.queryForObject("SELECT published_at FROM outbox_events WHERE event_id=?",Object.class,deposit.id()));
        projection.apply(delivered.getFirst());
        publishThrough(deposit.id(),projection::apply);
        assertEquals(1,count("analytics.processed_events",deposit.id()));
        assertEquals(1,count("analytics.transaction_facts",deposit.id()));
    }
    @Test void duplicateConsumerDeliveryCannotDoubleCount() {
        var a=account("0"); var deposit=banking.deposit(a.id(),key(),cash("10"));
        var e=event(deposit.id()); projection.apply(e); projection.apply(e);
        assertEquals(1,count("analytics.transaction_facts",e.eventId()));
        assertEquals(1,count("analytics.processed_events",e.eventId()));
    }
    @Test void simultaneousDuplicateConsumersInsertOneFact() throws Exception {
        var a=account("0"); var e=event(banking.deposit(a.id(),key(),cash("10")).id());
        try(var pool=Executors.newFixedThreadPool(2)) {
            var gate=new CountDownLatch(1);
            var f1=pool.submit(() -> { gate.await(); projection.apply(e); return true; });
            var f2=pool.submit(() -> { gate.await(); projection.apply(e); return true; });
            gate.countDown(); assertTrue(f1.get(15,TimeUnit.SECONDS)); assertTrue(f2.get(15,TimeUnit.SECONDS));
        }
        assertEquals(1,count("analytics.transaction_facts",e.eventId()));
    }
    @Test void inboxAndFactBothRollBackOnFailure() {
        var a=account("0"); var e=event(banking.deposit(a.id(),key(),cash("10")).id());
        new TransactionTemplate(transactions).executeWithoutResult(status -> { projection.apply(e); status.setRollbackOnly(); });
        assertEquals(0,count("analytics.processed_events",e.eventId()));
        assertEquals(0,count("analytics.transaction_facts",e.eventId()));
        projection.apply(e); assertEquals(1,count("analytics.transaction_facts",e.eventId()));
    }
    @Test void conflictingPayloadAndUnsupportedSchemaAreRejected() {
        var a=account("0"); var e=event(banking.deposit(a.id(),key(),cash("10")).id()); projection.apply(e);
        var conflict=new OperationEvent(1,e.eventId(),e.journalId(),e.operationType(),e.debitAccountId(),e.creditAccountId(),
            new BigDecimal("11"),e.currency(),e.occurredAt());
        assertThrows(InvalidEventException.class,() -> projection.apply(conflict));
        var incompatible=new OperationEvent(2,e.eventId(),e.journalId(),e.operationType(),e.debitAccountId(),e.creditAccountId(),
            e.amount(),e.currency(),e.occurredAt());
        assertThrows(InvalidEventException.class,() -> projection.apply(incompatible));
        assertEquals(1,count("analytics.transaction_facts",e.eventId()));
    }
    @Test void aggregatesUseUtcDatesAndKeepCurrenciesAndTypesSeparate() {
        var date=java.time.LocalDate.parse("2020-01-01");
        var before=new HashMap<String,AnalyticsController.DailySummary>();
        analytics.daily(date,date).forEach(row -> before.put(row.currency(),row));
        UUID customer=UUID.randomUUID();
        for(String currency:List.of("MAD","EUR")) {
            UUID id=UUID.randomUUID();
            var e=new OperationEvent(1,id,id,"DEPOSIT",null,customer,new BigDecimal("10"),currency,
                Instant.parse("2020-01-02T01:00:00+02:00"));
            projection.apply(e); projection.apply(e);
        }
        var summary=analytics.daily(date,date);
        assertEquals(2,summary.size());
        summary.forEach(row -> {
            var previous=before.get(row.currency());
            assertEquals((previous==null ? 0 : previous.operations())+1,row.operations());
            BigDecimal initial=previous==null ? BigDecimal.ZERO : previous.totalAmount();
            assertEquals(0,initial.add(new BigDecimal("10.00")).compareTo(row.totalAmount()));
        });
    }
    @Test void analyticsApiRejectsInvalidDateRanges() throws Exception {
        for(String query:List.of("from=2020-01-02&to=2020-01-01","from=2020-01-01&to=2022-01-01","from=invalid"))
            assertEquals(400,mvc.perform(get("/api/analytics/daily?"+query)).andReturn().getResponse().getStatus());
        assertEquals(200,mvc.perform(get("/api/analytics/pipeline")).andReturn().getResponse().getStatus());
    }
}
