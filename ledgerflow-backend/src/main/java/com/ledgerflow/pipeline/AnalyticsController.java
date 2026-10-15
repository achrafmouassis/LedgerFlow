package com.ledgerflow.pipeline;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {
    private final JdbcTemplate jdbc;
    public AnalyticsController(JdbcTemplate jdbc) { this.jdbc = jdbc; }
    public record DailySummary(LocalDate date,String currency,String operationType,long operations,BigDecimal totalAmount) {}
    public record PipelineStatus(long pending,long published,long processed) {}
    @GetMapping("/daily")
    public List<DailySummary> daily(
        @RequestParam(required=false) @DateTimeFormat(iso=DateTimeFormat.ISO.DATE) LocalDate from,
        @RequestParam(required=false) @DateTimeFormat(iso=DateTimeFormat.ISO.DATE) LocalDate to) {
        LocalDate end = to == null ? LocalDate.now(ZoneOffset.UTC) : to;
        LocalDate start = from == null ? end.minusDays(6) : from;
        if (end.isBefore(start) || ChronoUnit.DAYS.between(start,end) > 365)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Date range must be ordered and at most 366 days inclusive");
        return jdbc.query("""
            SELECT event_date,currency,operation_type,COUNT(*) AS operations,SUM(amount) AS total_amount
            FROM analytics.transaction_facts WHERE event_date >= ? AND event_date <= ?
            GROUP BY event_date,currency,operation_type ORDER BY event_date,currency,operation_type
            """,(rs,n) -> new DailySummary(rs.getDate("event_date").toLocalDate(),rs.getString("currency"),
                rs.getString("operation_type"),rs.getLong("operations"),rs.getBigDecimal("total_amount")),start,end);
    }
    @GetMapping("/pipeline")
    public PipelineStatus status() {
        return new PipelineStatus(count("SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL"),
            count("SELECT COUNT(*) FROM outbox_events WHERE published_at IS NOT NULL"),
            count("SELECT COUNT(*) FROM analytics.processed_events"));
    }
    private long count(String sql) { return jdbc.queryForObject(sql,Long.class); }
    public record BatchDailySummary(LocalDate date,String currency,String operationType,long operations,
        BigDecimal totalAmount,BigDecimal minAmount,BigDecimal maxAmount,java.util.UUID runId,Instant computedAt) {}
    public record BatchRun(java.util.UUID runId,String status,LocalDate startDate,LocalDate endDate,
        long sourceCount,long groupCount,Instant startedAt,Instant snapshotAt,Instant finishedAt,String errorType) {}
    @GetMapping("/batch/daily")
    public List<BatchDailySummary> batchDaily(
        @RequestParam(required=false) @DateTimeFormat(iso=DateTimeFormat.ISO.DATE) LocalDate from,
        @RequestParam(required=false) @DateTimeFormat(iso=DateTimeFormat.ISO.DATE) LocalDate to) {
        LocalDate end = to == null ? LocalDate.now(ZoneOffset.UTC) : to;
        LocalDate start = from == null ? end.minusDays(6) : from;
        if (end.isBefore(start) || ChronoUnit.DAYS.between(start,end) > 365)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Date range must be ordered and at most 366 days inclusive");
        return jdbc.query("""
            SELECT * FROM analytics.daily_operation_summary WHERE event_date BETWEEN ? AND ?
            ORDER BY event_date,currency,operation_type
            """,(rs,n) -> new BatchDailySummary(rs.getDate("event_date").toLocalDate(),rs.getString("currency"),
                rs.getString("operation_type"),rs.getLong("operation_count"),rs.getBigDecimal("total_amount"),
                rs.getBigDecimal("min_amount"),rs.getBigDecimal("max_amount"),rs.getObject("run_id",java.util.UUID.class),
                rs.getTimestamp("computed_at").toInstant()),start,end);
    }
    @GetMapping("/batch/runs")
    public List<BatchRun> batchRuns(@RequestParam(defaultValue="20") @jakarta.validation.constraints.Min(1)
            @jakarta.validation.constraints.Max(100) int limit) {
        return jdbc.query("""
            SELECT * FROM analytics.batch_runs ORDER BY started_at DESC,run_id DESC LIMIT ?
            """,(rs,n) -> new BatchRun(rs.getObject("run_id",java.util.UUID.class),rs.getString("status"),
                rs.getDate("start_date").toLocalDate(),rs.getDate("end_date").toLocalDate(),
                rs.getLong("source_count"),rs.getLong("group_count"),rs.getTimestamp("started_at").toInstant(),
                rs.getTimestamp("snapshot_at") == null ? null : rs.getTimestamp("snapshot_at").toInstant(),
                rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toInstant(),rs.getString("error_type")),limit);
    }

}
