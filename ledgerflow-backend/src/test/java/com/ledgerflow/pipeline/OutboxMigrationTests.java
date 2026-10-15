package com.ledgerflow.pipeline;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class OutboxMigrationTests {
    @Test void migrationBackfillsExistingJournalsWithoutChangingBalances() {
        var db=new SingleConnectionDataSource("jdbc:h2:mem:outbox_upgrade_"+UUID.randomUUID()+";MODE=PostgreSQL;DB_CLOSE_DELAY=-1","sa","",true);
        try {
            Flyway.configure().dataSource(db).target("2").load().migrate();
            JdbcTemplate jdbc=new JdbcTemplate(db); UUID customer=UUID.randomUUID(),journal=UUID.randomUUID();
            jdbc.update("INSERT INTO accounts(id,owner,currency,balance) VALUES (?,'Legacy','MAD',100)",customer);
            jdbc.update("INSERT INTO journals(id,type,credit_account_id,amount,currency,created_at) VALUES (?,'OPENING',?,100,'MAD',CURRENT_TIMESTAMP)",journal,customer);
            Flyway.configure().dataSource(db).load().migrate();
            assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM outbox_events WHERE event_id=? AND journal_id=? AND published_at IS NULL",Integer.class,journal,journal));
            assertEquals(100,jdbc.queryForObject("SELECT balance FROM accounts WHERE id=?",Integer.class,customer));
            assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM analytics.transaction_facts",Integer.class));
        } finally { db.destroy(); }
    }
}
