import os
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from src.ingestion.telemetry_pb2 import VehicleTelemetry
from utils.logger import get_logger
from utils.db import get_landing_connection

load_dotenv()

TOPIC_NAME = os.getenv("KAFKA_TELEMETRY_TOPIC", "telemetry.raw")
CONSUMER_GROUP = os.getenv("KAFKA_TELEMETRY_CONSUMER_GROUP", "sentinel-telemetry-mysql-loader")
#CONSUMER_GROUP = os.getenv("KAFKA_TELEMETRY_CONSUMER_GROUP", "sentinel-telemetry-mysql-loader-v2")



def fahrenheit_to_celsius(temp_f):
    return (temp_f - 32.0) * 5.0 / 9.0


def build_consumer():
    consumer_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("KAFKA_API_KEY"),
        "sasl.password": os.getenv("KAFKA_API_SECRET"),
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
        #"auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": True,
    }
    return Consumer(consumer_config)


def parse_event_timestamp(telemetry):
    if telemetry.HasField("event_time"):
        dt = telemetry.event_time.ToDatetime(tzinfo=timezone.utc)
        return dt.replace(tzinfo=None)
    return datetime.utcnow()


def build_event_key(topic, partition, offset):
    return f"{topic}-{partition}-{offset}"


def upsert_telemetry(cursor, telemetry, topic, partition, offset):
    event_timestamp = parse_event_timestamp(telemetry)
    peak_temp_c = int(round(fahrenheit_to_celsius(telemetry.metrics.engine_temp_f)))

    # Metrics schema does not carry engine load yet; use speed as a temporary proxy.
    avg_engine_load = float(telemetry.metrics.speed_kmh)

    correlation_id = f"kafka-{topic}-{partition}-{offset}"
    file_id = correlation_id
    file_name = f"{topic}:{partition}:{offset}"

    query = """
        INSERT INTO telemetry_facts
        (file_id, file_name, vehicle_id, trip_timestamp, avg_engine_load, peak_temp_c, correlation_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            file_name = VALUES(file_name),
            vehicle_id = VALUES(vehicle_id),
            trip_timestamp = VALUES(trip_timestamp),
            avg_engine_load = VALUES(avg_engine_load),
            peak_temp_c = VALUES(peak_temp_c),
            correlation_id = VALUES(correlation_id)
    """
    values = (
        file_id,
        file_name,
        telemetry.vin,
        event_timestamp,
        avg_engine_load,
        peak_temp_c,
        correlation_id,
    )
    cursor.execute(query, values)
    return correlation_id, event_timestamp


def upsert_telemetry_ingest_event(
    cursor,
    event_key,
    vehicle_id,
    correlation_id,
    topic,
    partition,
    offset,
    source_event_ts,
    payload_json,
    ingest_status,
    error_message,
):
    query = """
        INSERT INTO telemetry_ingest_events
        (event_key, vehicle_id, correlation_id, kafka_topic, kafka_partition, kafka_offset,
         source_event_ts, payload_json, ingest_status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            vehicle_id = VALUES(vehicle_id),
            correlation_id = VALUES(correlation_id),
            source_event_ts = VALUES(source_event_ts),
            payload_json = VALUES(payload_json),
            ingest_status = VALUES(ingest_status),
            error_message = VALUES(error_message),
            ingested_at = CURRENT_TIMESTAMP
    """
    values = (
        event_key,
        vehicle_id,
        correlation_id,
        topic,
        partition,
        offset,
        source_event_ts,
        payload_json,
        ingest_status,
        error_message,
    )
    cursor.execute(query, values)


def run_loader():
    logger = get_logger("KAFKA_TO_MYSQL")

    schema_registry_client = SchemaRegistryClient(
        {
            "url": os.getenv("SCHEMA_REGISTRY_URL"),
            "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}",
        }
    )

    deserializer = ProtobufDeserializer(
        VehicleTelemetry,
        schema_registry_client=schema_registry_client,
    )
    consumer = build_consumer()
    db = get_landing_connection()
    cursor = db.cursor()

    logger.info(f"Subscribing to topic {TOPIC_NAME} with group {CONSUMER_GROUP}")
    consumer.subscribe([TOPIC_NAME])

    processed = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            telemetry = deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )
            if telemetry is None:
                continue

            event_key = build_event_key(msg.topic(), msg.partition(), msg.offset())
            payload_json = json.dumps(
                {
                    "vin": telemetry.vin,
                    "event_time": telemetry.event_time.ToJsonString() if telemetry.HasField("event_time") else None,
                    "speed_kmh": telemetry.metrics.speed_kmh,
                    "engine_temp_f": telemetry.metrics.engine_temp_f,
                    "fuel_level": telemetry.metrics.fuel_level,
                }
            )

            try:
                correlation_id, event_timestamp = upsert_telemetry(
                    cursor,
                    telemetry,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                upsert_telemetry_ingest_event(
                    cursor,
                    event_key=event_key,
                    vehicle_id=telemetry.vin,
                    correlation_id=correlation_id,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    source_event_ts=event_timestamp,
                    payload_json=payload_json,
                    ingest_status="SUCCESS",
                    error_message=None,
                )
                db.commit()
                processed += 1
                logger.info(
                    f"event=telemetry_ingest_processed result=success event_key={event_key} "
                    f"vehicle_id={telemetry.vin} topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
                )
            except Exception as exc:
                db.rollback()
                upsert_telemetry_ingest_event(
                    cursor,
                    event_key=event_key,
                    vehicle_id=telemetry.vin,
                    correlation_id=f"kafka-{msg.topic()}-{msg.partition()}-{msg.offset()}",
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    source_event_ts=parse_event_timestamp(telemetry),
                    payload_json=payload_json,
                    ingest_status="FAILED",
                    error_message=str(exc),
                )
                db.commit()
                logger.error(
                    f"event=telemetry_ingest_processed result=failed event_key={event_key} "
                    f"vehicle_id={telemetry.vin} topic={msg.topic()} partition={msg.partition()} "
                    f"offset={msg.offset()} error={exc}"
                )
                continue

            if processed % 20 == 0:
                logger.info(f"Processed {processed} records into telemetry_facts")

    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        cursor.close()
        db.close()
        consumer.close()
        logger.info(f"Loader stopped. total_processed={processed}")


if __name__ == "__main__":
    run_loader()
