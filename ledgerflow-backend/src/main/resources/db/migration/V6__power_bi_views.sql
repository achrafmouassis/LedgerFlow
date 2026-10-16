-- Stable, aggregate-only BI contract. No customer accounts or raw event payloads.
CREATE VIEW analytics.bi_daily_operations AS
SELECT event_date, currency, operation_type,
    CASE operation_type WHEN 'OPENING' THEN 'Ouverture'
        WHEN 'DEPOSIT' THEN 'Depot' WHEN 'WITHDRAWAL' THEN 'Retrait'
        WHEN 'TRANSFER' THEN 'Virement' ELSE operation_type END AS operation_label,
    operation_count, total_amount, min_amount, max_amount,
    CAST(total_amount AS VARCHAR(64)) AS total_amount_exact,
    computed_at, CAST(run_id AS VARCHAR(36)) AS run_id
FROM analytics.daily_operation_summary;

CREATE VIEW analytics.bi_batch_runs AS
SELECT CAST(run_id AS VARCHAR(36)) AS run_id, status, start_date, end_date,
    started_at, finished_at, source_count, group_count, error_type
FROM analytics.batch_runs;
