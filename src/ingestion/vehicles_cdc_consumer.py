import json
import os
import time

from confluent_kafka import Consumer
from dotenv import load_dotenv

from utils.logger import get_logger
from utils.db import get_admin_connection

load_dotenv()

CDC_TOPIC = os.getenv("KAFKA_VEHICLES_CDC_TOPIC", "cdc_prod.sentinel_db.vehicles_sentinel")
CDC_GROUP = os.getenv("KAFKA_VEHICLES_CDC_GROUP", "sentinel-vehicles-cdc-loader")


def build_consumer(group_id=None, offset_reset=None):
    consumer_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("KAFKA_API_KEY"),
        "sasl.password": os.getenv("KAFKA_API_SECRET"),
        "group.id": group_id or CDC_GROUP,
        "auto.offset.reset": offset_reset or os.getenv("KAFKA_VEHICLES_OFFSET_RESET", "latest"),
        "enable.auto.commit": False,
    }
    return Consumer(consumer_config)


def parse_debezium_payload(raw_payload):
    # Debezium payload may come as plain payload or schema+payload envelope.
    if isinstance(raw_payload, dict) and "payload" in raw_payload and isinstance(raw_payload["payload"], dict):
        return raw_payload["payload"]
    return raw_payload


def upsert_vehicle(cursor, vehicle):
    query = """
        INSERT INTO vehicles (vin, owner_name, manufacturer, vehicle_model, manufacture_year, status, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
            owner_name = VALUES(owner_name),
            manufacturer = VALUES(manufacturer),
            vehicle_model = VALUES(vehicle_model),
            manufacture_year = VALUES(manufacture_year),
            status = VALUES(status),
            updated_at = CURRENT_TIMESTAMP
    """
    values = (
        vehicle.get("vin"),
        vehicle.get("owner_name"),
        vehicle.get("manufacturer"),
        vehicle.get("vehicle_model"),
        vehicle.get("manufacture_year"),
        vehicle.get("status"),
    )
    cursor.execute(query, values)


def delete_vehicle(cursor, vin):
    cursor.execute("DELETE FROM vehicles WHERE vin = %s", (vin,))


def upsert_vehicle_cdc_event(cursor, payload, msg):
    op = payload.get("op")
    before = payload.get("before")
    after = payload.get("after")
    source = payload.get("source") or {}
    vin = None
    if isinstance(after, dict) and after.get("vin"):
        vin = after.get("vin")
    elif isinstance(before, dict) and before.get("vin"):
        vin = before.get("vin")

    event_key = f"{msg.topic()}-{msg.partition()}-{msg.offset()}"
    source_ts_ms = source.get("ts_ms") if isinstance(source, dict) else None

    query = """
        INSERT INTO vehicles_cdc_events
        (event_key, vin, op_type, before_json, after_json, source_ts_ms, kafka_topic, kafka_partition, kafka_offset)
        VALUES (%s, %s, %s, CAST(%s AS JSON), CAST(%s AS JSON), %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            vin = VALUES(vin),
            op_type = VALUES(op_type),
            before_json = VALUES(before_json),
            after_json = VALUES(after_json),
            source_ts_ms = VALUES(source_ts_ms),
            ingested_at = CURRENT_TIMESTAMP
    """
    before_json = json.dumps(before) if before is not None else None
    after_json = json.dumps(after) if after is not None else None
    values = (
        event_key,
        vin,
        op,
        before_json,
        after_json,
        source_ts_ms,
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )
    cursor.execute(query, values)


def run_loader(max_events=None, idle_timeout_seconds=None, group_id=None, offset_reset=None):
    logger = get_logger("VEHICLES_CDC_CONSUMER")
    consumer = build_consumer(group_id=group_id, offset_reset=offset_reset)
    db = get_admin_connection(database=os.getenv("MYSQL_LANDING_SCHEMA", "sentinel_landing"))
    cursor = db.cursor()

    effective_group = group_id or CDC_GROUP
    logger.info(
        "Subscribing to topic %s with group %s offset_reset=%s max_events=%s idle_timeout_seconds=%s",
        CDC_TOPIC,
        effective_group,
        offset_reset or os.getenv("KAFKA_VEHICLES_OFFSET_RESET", "latest"),
        max_events,
        idle_timeout_seconds,
    )
    consumer.subscribe([CDC_TOPIC])

    processed = 0
    last_message_ts = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if idle_timeout_seconds is not None and (time.time() - last_message_ts) >= idle_timeout_seconds:
                    logger.info("Idle timeout reached. Stopping CDC loader.")
                    break
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                raw_payload = json.loads(msg.value().decode("utf-8"))
                payload = parse_debezium_payload(raw_payload)
            except Exception as exc:
                logger.error(f"Invalid JSON payload at offset={msg.offset()}: {exc}")
                continue

            op = payload.get("op")
            after = payload.get("after")
            before = payload.get("before")

            try:
                if op in ("c", "r", "u") and after:
                    upsert_vehicle(cursor, after)
                elif op == "d" and before and before.get("vin"):
                    delete_vehicle(cursor, before["vin"])

                upsert_vehicle_cdc_event(cursor, payload, msg)
                db.commit()
                consumer.commit(message=msg, asynchronous=False)
                processed += 1
                last_message_ts = time.time()
            except Exception as exc:
                db.rollback()
                logger.error(
                    "Failed processing CDC event topic=%s partition=%s offset=%s error=%s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    exc,
                )
                continue

            if processed and processed % 20 == 0:
                logger.info(f"Processed {processed} vehicle CDC events")

            if max_events is not None and processed >= max_events:
                logger.info("Max events reached. Stopping CDC loader.")
                break

    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        cursor.close()
        db.close()
        consumer.close()
        logger.info(f"Loader stopped. total_processed={processed}")
    return processed


if __name__ == "__main__":
    run_loader()
