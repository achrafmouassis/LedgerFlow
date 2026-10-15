CREATE TABLE analytics.batch_runs (
    run_id UUID PRIMARY KEY,
    job_name VARCHAR(64) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(12) NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    snapshot_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    source_count BIGINT NOT NULL DEFAULT 0 CHECK (source_count >= 0),
    group_count BIGINT NOT NULL DEFAULT 0 CHECK (group_count >= 0),
    error_type VARCHAR(200),
    CHECK (end_date >= start_date)
);
CREATE INDEX batch_runs_started ON analytics.batch_runs(started_at);
CREATE TABLE analytics.batch_source_snapshot (
    run_id UUID NOT NULL REFERENCES analytics.batch_runs(run_id),
    event_id UUID NOT NULL,
    event_date DATE NOT NULL,
    currency VARCHAR(3) NOT NULL,
    operation_type VARCHAR(20) NOT NULL,
    amount NUMERIC(19,2) NOT NULL,
    PRIMARY KEY (run_id,event_id)
);
-- Staging accepts identical retry writes from Spark JDBC tasks. Reconciliation
-- rejects conflicting duplicates; publication selects DISTINCT complete rows.
CREATE TABLE analytics.daily_summary_stage (
    run_id UUID NOT NULL REFERENCES analytics.batch_runs(run_id),
    event_date DATE NOT NULL,
    currency VARCHAR(3) NOT NULL,
    operation_type VARCHAR(20) NOT NULL,
    operation_count BIGINT NOT NULL CHECK (operation_count > 0),
    total_amount NUMERIC(38,2) NOT NULL CHECK (total_amount > 0),
    min_amount NUMERIC(19,2) NOT NULL CHECK (min_amount > 0),
    max_amount NUMERIC(19,2) NOT NULL CHECK (max_amount >= min_amount)
);
CREATE INDEX daily_stage_run ON analytics.daily_summary_stage(run_id);
CREATE TABLE analytics.daily_operation_summary (
    event_date DATE NOT NULL,
    currency VARCHAR(3) NOT NULL,
    operation_type VARCHAR(20) NOT NULL,
    operation_count BIGINT NOT NULL CHECK (operation_count > 0),
    total_amount NUMERIC(38,2) NOT NULL CHECK (total_amount > 0),
    min_amount NUMERIC(19,2) NOT NULL CHECK (min_amount > 0),
    max_amount NUMERIC(19,2) NOT NULL CHECK (max_amount >= min_amount),
    run_id UUID NOT NULL REFERENCES analytics.batch_runs(run_id),
    computed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (event_date,currency,operation_type)
);
