CREATE TABLE IF NOT EXISTS sentinel_landing.vehicles (
    vin VARCHAR(17) PRIMARY KEY,
    owner_name VARCHAR(100),
    manufacturer VARCHAR(100) NULL,
    vehicle_model VARCHAR(100),
    manufacture_year INT,
    status VARCHAR(30),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sentinel_landing.vehicles_cdc_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_key VARCHAR(128) UNIQUE,
    vin VARCHAR(17),
    op_type CHAR(1),
    before_json JSON,
    after_json JSON,
    source_ts_ms BIGINT,
    kafka_topic VARCHAR(255),
    kafka_partition INT,
    kafka_offset BIGINT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_vin (vin),
    INDEX idx_op_type (op_type),
    INDEX idx_ingested_at (ingested_at)
);

CREATE TABLE IF NOT EXISTS sentinel_landing.telemetry_facts (
    file_id VARCHAR(50) PRIMARY KEY,
    file_name VARCHAR(255),
    vehicle_id VARCHAR(50),
    trip_timestamp DATETIME,
    avg_engine_load FLOAT,
    peak_temp_c INT,
    trip_distance_km FLOAT NULL,
    trip_duration_seconds INT NULL,
    correlation_id VARCHAR(64),
    INDEX idx_vehicle_trip (vehicle_id, trip_timestamp),
    INDEX idx_correlation_id (correlation_id)
);
