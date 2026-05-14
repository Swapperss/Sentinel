import os
import random
import time

import mysql.connector
from dotenv import load_dotenv

from utils.logger import get_logger

load_dotenv()

SOURCE_SCHEMA = os.getenv("MYSQL_SOURCE_SCHEMA", "sentinel_source")
SOURCE_TABLE = os.getenv("MYSQL_SOURCE_VEHICLES_TABLE", "vehicles_source")


def build_source_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_DB_USER", "root"),
        password=os.getenv("MYSQL_DB_PASSWORD", "root"),
    )


def ensure_source_table(cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SOURCE_SCHEMA}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SOURCE_SCHEMA}.{SOURCE_TABLE} (
            vin VARCHAR(17) PRIMARY KEY,
            owner_name VARCHAR(100),
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
        (vin, owner_name, vehicle_model, manufacture_year, status)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            owner_name = VALUES(owner_name),
            vehicle_model = VALUES(vehicle_model),
            manufacture_year = VALUES(manufacture_year),
            status = VALUES(status)
        """,
        (
            record["vin"],
            record["owner_name"],
            record["vehicle_model"],
            record["manufacture_year"],
            record["status"],
        ),
    )


def run_simulator(total_events=24, interval_seconds=5):
    logger = get_logger("VEHICLES_SOURCE_SIM")

    seed_data = [
        {"vin": "VIN-DUIS-1234", "owner_name": "Arun Das", "vehicle_model": "Model-X", "manufacture_year": 2022},
        {"vin": "VIN-DUIS-2026", "owner_name": "Mira Sen", "vehicle_model": "Model-S", "manufacture_year": 2023},
        {"vin": "VIN-DUIS-5674", "owner_name": "Karan Iyer", "vehicle_model": "Model-Y", "manufacture_year": 2021},
    ]
    statuses = ["ACTIVE", "SERVICE_DUE", "HEALTH_WARN", "INACTIVE"]

    db = build_source_connection()
    cursor = db.cursor()

    try:
        ensure_source_table(cursor)
        db.commit()

        logger.info(
            f"Publishing source table updates into {SOURCE_SCHEMA}.{SOURCE_TABLE} for Debezium capture"
        )
        for idx in range(total_events):
            base = random.choice(seed_data)
            record = {
                **base,
                "status": random.choice(statuses),
            }
            upsert_vehicle(cursor, record)
            db.commit()

            logger.info(
                f"[{idx + 1}/{total_events}] upsert vin={record['vin']} status={record['status']}"
            )
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        cursor.close()
        db.close()


if __name__ == "__main__":
    run_simulator()
