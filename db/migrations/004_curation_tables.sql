CREATE TABLE IF NOT EXISTS sentinel_curation.vehicles (
    vin VARCHAR(17) PRIMARY KEY,
    owner_name VARCHAR(100) NULL,
    manufacturer VARCHAR(100) NULL,
    vehicle_model VARCHAR(100) NULL,
    manufacture_year INT NULL,
    status VARCHAR(30) NULL,
    last_seen_at TIMESTAMP NULL,
    source_updated_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_curation_vehicles_vin (vin)
);

CREATE TABLE IF NOT EXISTS sentinel_curation.telemetry_facts (
    file_id VARCHAR(50) PRIMARY KEY,
    file_name VARCHAR(255) NULL,
    vehicle_id VARCHAR(50) NULL,
    trip_timestamp DATETIME NULL,
    avg_engine_load FLOAT NULL,
    peak_temp_c INT NULL,
    trip_distance_km FLOAT NULL,
    trip_duration_seconds INT NULL,
    correlation_id VARCHAR(64) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_vehicle_trip (vehicle_id, trip_timestamp),
    INDEX idx_correlation_id (correlation_id)
);

CREATE TABLE IF NOT EXISTS sentinel_curation.sentinel_ai_curation (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL,
    event_timestamp DATETIME NOT NULL,
    driver_complaint TEXT,
    technical_anomaly_detected TINYINT(1) DEFAULT 0,
    diagnostic_summary VARCHAR(255),
    correlation_id VARCHAR(64),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_event (vehicle_id, event_timestamp),
    INDEX idx_processed_at (processed_at),
    INDEX idx_correlation_id (correlation_id)
);
