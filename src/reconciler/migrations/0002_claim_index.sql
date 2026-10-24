-- 0002_claim_index: the claim query is `WHERE status = 'received' AND next_attempt_at <= now ORDER BY seq LIMIT 1`.
-- The 0001 index led with next_attempt_at, so it could not return rows in seq order and the planner fell back
-- to walking events_seq_key, filtering out every processed row first. Measured with 200k processed events
-- ahead of the backlog: 14.9 ms per claim, growing with history. A partial index keyed by seq reads only
-- unprocessed rows: 0.075 ms. Nothing else used the old index.

CREATE INDEX events_claim_seq_idx ON events (seq) WHERE status = 'received';
DROP INDEX events_claim_idx;
