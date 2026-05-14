import json
import os

from confluent_kafka import Consumer
from dotenv import load_dotenv

from utils.logger import get_logger
from utils.db import get_landing_connection

load_dotenv()

CDC_TOPIC = os.getenv("KAFKA_VEHICLES_CDC_TOPIC", "cdc_prod.sentinel_source.vehicles_source")
CDC_GROUP = os.getenv("KAFKA_VEHICLES_CDC_GROUP", "sentinel-vehicles-cdc-loader")


def build_consumer():
    consumer_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("KAFKA_API_KEY"),
        "sasl.password": os.getenv("KAFKA_API_SECRET"),
        "group.id": CDC_GROUP,
        "auto.offset.reset": os.getenv("KAFKA_VEHICLES_OFFSET_RESET", "latest"),
        "enable.auto.commit": True,
    }
    return Consumer(consumer_config)


def upsert_vehicle(cursor, vehicle):
    query = """
        INSERT INTO vehicles (vin, owner_name, vehicle_model, manufacture_year, status, updated_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
            owner_name = VALUES(owner_name),
            vehicle_model = VALUES(vehicle_model),
            manufacture_year = VALUES(manufacture_year),
            status = VALUES(status),
            updated_at = CURRENT_TIMESTAMP
    """
    values = (
        vehicle.get("vin"),
        vehicle.get("owner_name"),
        vehicle.get("vehicle_model"),
        vehicle.get("manufacture_year"),
        vehicle.get("status"),
    )
    cursor.execute(query, values)


def delete_vehicle(cursor, vin):
    cursor.execute("DELETE FROM vehicles WHERE vin = %s", (vin,))


def run_loader():
    logger = get_logger("VEHICLES_CDC_CONSUMER")
    consumer = build_consumer()
    db = get_landing_connection()
    cursor = db.cursor()

    logger.info(f"Subscribing to topic {CDC_TOPIC} with group {CDC_GROUP}")
    consumer.subscribe([CDC_TOPIC])

    processed = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:
                logger.error(f"Invalid JSON payload at offset={msg.offset()}: {exc}")
                continue

            op = payload.get("op")
            after = payload.get("after")
            before = payload.get("before")

            if op in ("c", "r", "u") and after:
                upsert_vehicle(cursor, after)
                db.commit()
                processed += 1
            elif op == "d" and before and before.get("vin"):
                delete_vehicle(cursor, before["vin"])
                db.commit()
                processed += 1

            if processed and processed % 20 == 0:
                logger.info(f"Processed {processed} vehicle CDC events")

    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        cursor.close()
        db.close()
        consumer.close()
        logger.info(f"Loader stopped. total_processed={processed}")


if __name__ == "__main__":
    run_loader()
