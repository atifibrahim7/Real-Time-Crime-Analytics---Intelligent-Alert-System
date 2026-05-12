-- postgres_init.sql — PostgreSQL Initialization
-- ================================================
-- Creates the crime_db schema and pre-creates tables
-- that the Batch and Speed layers will populate.
--
-- This runs automatically if mounted into the PostgreSQL
-- container's /docker-entrypoint-initdb.d/ directory.

-- The database 'crime_db' is already created via POSTGRES_DB env var.
-- This script ensures the alerts table exists for the Speed Layer.

CREATE TABLE IF NOT EXISTS speed_layer_alerts (
    id              SERIAL PRIMARY KEY,
    alert_id        VARCHAR(50),
    alert_type      VARCHAR(50),
    district        VARCHAR(20),
    crime_count_in_window INTEGER,
    window_seconds  INTEGER,
    threshold       INTEGER,
    latest_crime_type    VARCHAR(100),
    latest_case_number   VARCHAR(50),
    latest_block         VARCHAR(200),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    triggered_at    TIMESTAMP,
    severity        VARCHAR(20),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster dashboard queries
CREATE INDEX IF NOT EXISTS idx_alerts_district ON speed_layer_alerts(district);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON speed_layer_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON speed_layer_alerts(triggered_at);
