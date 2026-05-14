CREATE TABLE IF NOT EXISTS sentinel_db.vehicles_sentinel (
    vin VARCHAR(17) PRIMARY KEY,
    owner_name VARCHAR(100),
    manufacturer VARCHAR(100) NULL,
    vehicle_model VARCHAR(100),
    manufacture_year INT,
    status VARCHAR(30),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
