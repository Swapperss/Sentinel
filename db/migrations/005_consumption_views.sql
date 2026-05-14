CREATE OR REPLACE VIEW sentinel_consumption.v_vehicles AS
SELECT
    vin,
    owner_name,
    manufacturer,
    vehicle_model,
    manufacture_year,
    status,
    last_seen_at,
    source_updated_at,
    created_at,
    updated_at
FROM sentinel_curation.vehicles;

CREATE OR REPLACE VIEW sentinel_consumption.v_telemetry_facts AS
SELECT
    file_id,
    file_name,
    vehicle_id,
    trip_timestamp,
    avg_engine_load,
    peak_temp_c,
    trip_distance_km,
    trip_duration_seconds,
    correlation_id,
    created_at,
    updated_at
FROM sentinel_curation.telemetry_facts;

CREATE OR REPLACE VIEW sentinel_consumption.v_sentinel_ai_curation AS
SELECT
    id,
    vehicle_id,
    event_timestamp,
    driver_complaint,
    technical_anomaly_detected,
    diagnostic_summary,
    correlation_id,
    processed_at
FROM sentinel_curation.sentinel_ai_curation;

CREATE OR REPLACE VIEW sentinel_consumption.v_vehicle_health_alerts AS
SELECT
    id,
    vehicle_id,
    event_timestamp,
    driver_complaint,
    technical_anomaly_detected,
    diagnostic_summary,
    correlation_id,
    processed_at
FROM sentinel_curation.sentinel_ai_curation
WHERE technical_anomaly_detected = 1;

CREATE OR REPLACE VIEW sentinel_consumption.v_fleet_summary AS
SELECT
    vehicle_id,
    COUNT(*) AS total_events,
    SUM(CASE WHEN technical_anomaly_detected = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(100 * SUM(CASE WHEN technical_anomaly_detected = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_pct,
    MAX(processed_at) AS last_processed_at
FROM sentinel_curation.sentinel_ai_curation
GROUP BY vehicle_id;
