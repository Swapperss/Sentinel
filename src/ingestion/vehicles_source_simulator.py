import os
import random
import sys
import time

import mysql.connector
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.ingestion.demo_event_factory import build_vehicle_record, get_demo_fleet
from utils.logger import get_logger

load_dotenv()

SOURCE_SCHEMA = os.getenv("MYSQL_SOURCE_SCHEMA", "sentinel_db")
SOURCE_TABLE = os.getenv("MYSQL_SOURCE_VEHICLES_TABLE", "vehicles_sentinel")
SOURCE_AUTO_INIT = os.getenv("MYSQL_SOURCE_AUTO_INIT", "false").lower() == "true"
VIN_ALLOWED_CHARS = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"


def build_source_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=(
            os.getenv("MYSQL_ADMIN_USER")
            or os.getenv("MYSQL_SOURCE_WRITE_USER")
            or os.getenv("MYSQL_DB_USER", "root")
        ),
        password=(
            os.getenv("MYSQL_ADMIN_PASSWORD")
            or os.getenv("MYSQL_SOURCE_WRITE_PASSWORD")
            or os.getenv("MYSQL_DB_PASSWORD", "root")
        ),
    )


def ensure_source_table(cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SOURCE_SCHEMA}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SOURCE_SCHEMA}.{SOURCE_TABLE} (
            vin VARCHAR(17) PRIMARY KEY,
            owner_name VARCHAR(100),
            manufacturer VARCHAR(100) NULL,
            vehicle_model VARCHAR(100),
            manufacture_year INT,
            status VARCHAR(30),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
    )


def upsert_vehicle(cursor, record):
    cursor.execute(
        f"""
        INSERT INTO {SOURCE_SCHEMA}.{SOURCE_TABLE}
        (vin, owner_name, manufacturer, vehicle_model, manufacture_year, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            owner_name = VALUES(owner_name),
            manufacturer = VALUES(manufacturer),
            vehicle_model = VALUES(vehicle_model),
            manufacture_year = VALUES(manufacture_year),
            status = VALUES(status),
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            record["vin"],
            record["owner_name"],
            record["manufacturer"],
            record["vehicle_model"],
            record["manufacture_year"],
            record["status"],
        ),
    )


def build_synthetic_vehicle(template, unique_index):
    synthetic = dict(template)
    synthetic["vin"] = build_synthetic_vin(unique_index)
    synthetic["owner_name"] = f"Demo Owner {unique_index:03d}"
    return synthetic


def build_synthetic_vin(unique_index):
    """Build deterministic 17-char VIN-like key for unique source rows."""
    value = max(0, int(unique_index))
    base = len(VIN_ALLOWED_CHARS)
    encoded = []
    while value > 0:
        value, remainder = divmod(value, base)
        encoded.append(VIN_ALLOWED_CHARS[remainder])
    suffix = "".join(reversed(encoded)) or "0"
    suffix = suffix.rjust(11, "0")[-11:]
    return f"SNTL00{suffix}"


def run_simulator(total_events=24, interval_seconds=5, distinct_rows=None):
    logger = get_logger("VEHICLES_SOURCE_SIM")
    fleet = get_demo_fleet()
    rng = random.Random()

    resolved_distinct_rows = distinct_rows
    if resolved_distinct_rows is None:
        resolved_distinct_rows = int(os.getenv("SENTINEL_SOURCE_DISTINCT_ROWS", "0") or "0")
    resolved_distinct_rows = max(0, int(resolved_distinct_rows))

    if resolved_distinct_rows > 0:
        synthetic_fleet = [
            build_synthetic_vehicle(fleet[idx % len(fleet)], idx + 1)
            for idx in range(resolved_distinct_rows)
        ]
        effective_total_events = max(int(total_events), resolved_distinct_rows)
        logger.info(
            "Distinct mode enabled: target_distinct_rows=%s total_events=%s effective_total_events=%s",
            resolved_distinct_rows,
            total_events,
            effective_total_events,
        )
    else:
        synthetic_fleet = []
        effective_total_events = int(total_events)

    db = build_source_connection()
    cursor = db.cursor()

    try:
        if SOURCE_AUTO_INIT:
            ensure_source_table(cursor)
            db.commit()
        else:
            logger.info(
                f"Using existing source table {SOURCE_SCHEMA}.{SOURCE_TABLE}; auto initialization is disabled"
            )

        logger.info(
            f"Publishing source table updates into {SOURCE_SCHEMA}.{SOURCE_TABLE} for Debezium capture"
        )
        for idx in range(effective_total_events):
            if resolved_distinct_rows > 0 and idx < resolved_distinct_rows:
                vehicle = synthetic_fleet[idx]
            elif resolved_distinct_rows > 0:
                vehicle = synthetic_fleet[idx % len(synthetic_fleet)]
            else:
                vehicle = fleet[idx % len(fleet)]

            record = build_vehicle_record(vehicle, idx, rng=rng)
            upsert_vehicle(cursor, record)
            db.commit()

            logger.info(
                f"[{idx + 1}/{effective_total_events}] upsert vin={record['vin']} manufacturer={record['manufacturer']} status={record['status']}"
            )
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        cursor.close()
        db.close()


if __name__ == "__main__":
    run_simulator()
