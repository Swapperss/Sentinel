CREATE DATABASE IF NOT EXISTS sentinel_analytics;
USE sentinel_analytics;

CREATE TABLE IF NOT EXISTS dim_vehicle (
  vehicle_key BIGINT AUTO_INCREMENT PRIMARY KEY,
  vin VARCHAR(32) NOT NULL,
  owner_name VARCHAR(100) NULL,
  manufacturer VARCHAR(64) NULL,
  model VARCHAR(64),
  model_year SMALLINT,
  vehicle_status VARCHAR(32),
  first_seen_at DATETIME NULL,
  last_seen_at DATETIME NULL,
  source_updated_at DATETIME NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_dim_vehicle_vin (vin)
);

CREATE TABLE IF NOT EXISTS dim_time (
  time_key INT AUTO_INCREMENT PRIMARY KEY,
  event_ts DATETIME NOT NULL,
  event_date DATE NOT NULL,
  year SMALLINT NOT NULL,
  quarter TINYINT NOT NULL,
  month TINYINT NOT NULL,
  day TINYINT NOT NULL,
  hour TINYINT NOT NULL,
  minute TINYINT NOT NULL,
  day_of_week TINYINT NOT NULL,
  is_weekend TINYINT(1) NOT NULL,
  UNIQUE KEY uq_dim_time_event_ts (event_ts),
  KEY idx_dim_time_event_date (event_date)
);

CREATE TABLE IF NOT EXISTS dim_diagnostic_type (
  diagnostic_type_key INT AUTO_INCREMENT PRIMARY KEY,
  diagnostic_code VARCHAR(64) NOT NULL,
  diagnostic_name VARCHAR(128),
  severity VARCHAR(32),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_dim_diag_code (diagnostic_code)
);

CREATE TABLE IF NOT EXISTS fct_vehicle_telemetry (
  telemetry_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
  vehicle_key BIGINT NOT NULL,
  time_key INT NOT NULL,
  diagnostic_type_key INT NULL,
  file_id VARCHAR(50) NULL,
  file_name VARCHAR(255) NULL,
  correlation_id VARCHAR(64) NULL,
  avg_engine_load DECIMAL(6,2) NULL,
  peak_temp_c DECIMAL(6,2) NULL,
  trip_distance_km DECIMAL(12,2) NULL,
  trip_duration_seconds INT NULL,
  source_event_ts DATETIME NULL,
  ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_fct_telemetry_vehicle_key (vehicle_key),
  KEY idx_fct_telemetry_time_key (time_key),
  KEY idx_fct_telemetry_diag_key (diagnostic_type_key),
  KEY idx_fct_telemetry_corr (correlation_id),
  CONSTRAINT fk_fct_telemetry_vehicle
    FOREIGN KEY (vehicle_key) REFERENCES dim_vehicle(vehicle_key),
  CONSTRAINT fk_fct_telemetry_time
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
  CONSTRAINT fk_fct_telemetry_diag
    FOREIGN KEY (diagnostic_type_key) REFERENCES dim_diagnostic_type(diagnostic_type_key)
);

CREATE TABLE IF NOT EXISTS fct_vehicle_health_event (
  health_event_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
  vehicle_key BIGINT NOT NULL,
  time_key INT NOT NULL,
  diagnostic_type_key INT NULL,
  correlation_id VARCHAR(64) NULL,
  health_score DECIMAL(5,2) NULL,
  anomaly_flag TINYINT(1) NOT NULL DEFAULT 0,
  summary VARCHAR(255) NULL,
  recommendation VARCHAR(255) NULL,
  source_event_ts DATETIME NULL,
  ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_fct_health_vehicle_key (vehicle_key),
  KEY idx_fct_health_time_key (time_key),
  KEY idx_fct_health_diag_key (diagnostic_type_key),
  KEY idx_fct_health_corr (correlation_id),
  CONSTRAINT fk_fct_health_vehicle
    FOREIGN KEY (vehicle_key) REFERENCES dim_vehicle(vehicle_key),
  CONSTRAINT fk_fct_health_time
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
  CONSTRAINT fk_fct_health_diag
    FOREIGN KEY (diagnostic_type_key) REFERENCES dim_diagnostic_type(diagnostic_type_key)
);