-- 003. Seed data for ingestion state

CREATE TABLE IF NOT EXISTS weather.ingestion_state
(
    pipeline String,
    last_successful_run DateTime,
    window_start DateTime,
    window_end DateTime
)
ENGINE = MergeTree
ORDER BY pipeline;