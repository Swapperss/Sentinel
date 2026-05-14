CREATE TABLE IF NOT EXISTS sentinel_landing.telemetry_ingest_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_key VARCHAR(128) NOT NULL UNIQUE,
    vehicle_id VARCHAR(50),
    correlation_id VARCHAR(64),
    kafka_topic VARCHAR(255) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    source_event_ts DATETIME NULL,
    payload_json JSON,
    ingest_status VARCHAR(16) NOT NULL,
    error_message TEXT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_telemetry_event_vehicle (vehicle_id),
    INDEX idx_telemetry_event_corr (correlation_id),
    INDEX idx_telemetry_event_status (ingest_status),
    INDEX idx_telemetry_event_ingested_at (ingested_at)
);