-- 0003_retention_indexes: the retention sweep deletes old rows in bounded batches ordered by age; without
-- these it would scan each table end to end on every pass.

CREATE INDEX events_retention_idx ON events (received_at)
    WHERE status IN ('processed', 'duplicate', 'stale', 'rejected', 'dead');
CREATE INDEX effect_log_applied_idx ON effect_log (applied_at);
CREATE INDEX reconcile_runs_finished_idx ON reconcile_runs (finished_at);
CREATE INDEX dlq_resolved_idx ON dlq (resolved_at) WHERE resolved_at IS NOT NULL;
