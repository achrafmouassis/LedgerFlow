package com.ledgerflow.pipeline;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

@ActiveProfiles("test")
@SpringBootTest
@AutoConfigureMockMvc
@Transactional
@org.springframework.security.test.context.support.WithMockUser(roles="OPERATOR")
class BatchAnalyticsApiTests {
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Test void servesMaterializedBatchResultsWithRunTraceability() throws Exception {
        UUID run=UUID.randomUUID();
        jdbc.update("""
            INSERT INTO analytics.batch_runs(run_id,job_name,start_date,end_date,status,started_at,source_count,group_count)
            VALUES (?,'daily-operation-summary',DATE '1971-01-01',DATE '1971-01-01','SUCCEEDED',CURRENT_TIMESTAMP,2,1)
            """,run);
        jdbc.update("""
            INSERT INTO analytics.daily_operation_summary(event_date,currency,operation_type,operation_count,total_amount,
                min_amount,max_amount,run_id,computed_at)
            VALUES (DATE '1971-01-01','MAD','DEPOSIT',2,30.30,10.10,20.20,?,CURRENT_TIMESTAMP)
            """,run);
        var response=mvc.perform(get("/api/analytics/batch/daily?from=1971-01-01&to=1971-01-01")).andReturn().getResponse();
        assertEquals(200,response.getStatus());
        var rows=mapper.readTree(response.getContentAsString());
        assertEquals(1,rows.size()); assertEquals(2,rows.get(0).get("operations").asInt());
        assertEquals(run.toString(),rows.get(0).get("runId").asString());
        assertEquals(0,new BigDecimal("30.30").compareTo(rows.get(0).get("totalAmount").decimalValue()));
        assertEquals(200,mvc.perform(get("/api/analytics/batch/runs?limit=1")).andReturn().getResponse().getStatus());
    }
    @Test void rejectsInvalidRangesAndRunLimits() throws Exception {
        for(String query:new String[]{"from=2026-01-02&to=2026-01-01","from=2020-01-01&to=2022-01-01","from=invalid"})
            assertEquals(400,mvc.perform(get("/api/analytics/batch/daily?"+query)).andReturn().getResponse().getStatus());
        for(String limit:new String[]{"0","101"})
            assertEquals(400,mvc.perform(get("/api/analytics/batch/runs?limit="+limit)).andReturn().getResponse().getStatus());
    }
    @Test void emptyBatchDayReturnsEmptyList() throws Exception {
        var response=mvc.perform(get("/api/analytics/batch/daily?from=1901-01-01&to=1901-01-01")).andReturn().getResponse();
        assertEquals(200,response.getStatus()); assertEquals(0,mapper.readTree(response.getContentAsString()).size());
    }
}
