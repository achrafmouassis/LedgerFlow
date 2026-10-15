package com.ledgerflow.banking;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@ActiveProfiles("test")
@SpringBootTest
class JournalMigrationTests {
    @Autowired DataSource dataSource;
    @Test void upgradePreservesBalancesAndBackfillsOpeningAndTransfers() {
        String schema="migration_"+UUID.randomUUID().toString().replace("-", "");
        String sqlSchema="\""+schema+"\"";
        JdbcTemplate jdbc=new JdbcTemplate(dataSource);
        Flyway baseline=Flyway.configure().dataSource(dataSource).schemas(schema)
            .defaultSchema(schema).target("1").load();
        try {
            baseline.migrate();
            UUID source=UUID.randomUUID(), destination=UUID.randomUUID(), transfer=UUID.randomUUID();
            jdbc.update("INSERT INTO "+sqlSchema+".accounts(id,owner,currency,balance) VALUES (?,'Source','MAD',70)",source);
            jdbc.update("INSERT INTO "+sqlSchema+".accounts(id,owner,currency,balance) VALUES (?,'Destination','MAD',30)",destination);
            jdbc.update("INSERT INTO "+sqlSchema+".transfers(id,idempotency_key,source_account_id,destination_account_id,amount,currency,created_at) VALUES (?,'legacy-key',?,?,30,'MAD',CURRENT_TIMESTAMP)",transfer,source,destination);
            Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema).target("2").load().migrate();
            assertEquals(2,jdbc.queryForObject("SELECT COUNT(*) FROM "+sqlSchema+".journals",Integer.class));
            assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM "+sqlSchema+".journals WHERE type='OPENING' AND credit_account_id=? AND amount=100",Integer.class,source));
            assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM "+sqlSchema+".journals WHERE type='TRANSFER' AND id=? AND transfer_id=? AND idempotency_key='legacy-key'",Integer.class,transfer,transfer));
            for(UUID id:new UUID[]{source,destination}) {
                BigDecimal balance=jdbc.queryForObject("SELECT balance FROM "+sqlSchema+".accounts WHERE id=?",BigDecimal.class,id);
                BigDecimal postings=jdbc.queryForObject("SELECT SUM(CASE WHEN credit_account_id=? THEN amount ELSE -amount END) FROM "+sqlSchema+".journals WHERE debit_account_id=? OR credit_account_id=?",BigDecimal.class,id,id,id);
                assertEquals(0,balance.compareTo(postings));
            }
            assertEquals(0,jdbc.queryForObject("SELECT balance FROM "+sqlSchema+".accounts WHERE id=?",BigDecimal.class,source).compareTo(new BigDecimal("70")));
        } finally {
            // Only the randomly named schema created by this test is removed.
            jdbc.execute("DROP SCHEMA IF EXISTS "+sqlSchema+" CASCADE");
        }
    }
}
