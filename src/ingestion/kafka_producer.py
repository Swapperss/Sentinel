import os
import time
import random
import sys
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Confluent & Protobuf Imports
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Your Utils & Generated Protobuf Class
from src.ingestion.demo_event_factory import (
    build_telemetry_event,
    expand_fleet_for_distinct_rows,
    get_demo_fleet,
)
from utils.logger import get_logger
from src.ingestion.telemetry_pb2 import VehicleTelemetry

load_dotenv()

TOPIC_NAME = os.getenv("KAFKA_TELEMETRY_TOPIC", "telemetry.raw")

def run_producer(total_events=24, interval_seconds=5, distinct_rows=None):
    logger = get_logger("KAFKA_PRODUCER")
    demo_fleet = get_demo_fleet()
    resolved_distinct_rows = distinct_rows
    if resolved_distinct_rows is None:
        resolved_distinct_rows = int(os.getenv("SENTINEL_TELEMETRY_DISTINCT_ROWS", "0") or "0")
    resolved_distinct_rows = max(0, int(resolved_distinct_rows))

    if resolved_distinct_rows > 0:
        fleet = expand_fleet_for_distinct_rows(demo_fleet, resolved_distinct_rows)
        total_events = max(int(total_events), resolved_distinct_rows)
        logger.info(
            "Distinct telemetry mode enabled: target_distinct_rows=%s total_events=%s",
            resolved_distinct_rows,
            total_events,
        )
    else:
        fleet = demo_fleet

    rng = random.Random()
    
    try:
        # 1. Setup Schema Registry Client
        sr_conf = {
            'url': os.getenv('SCHEMA_REGISTRY_URL'),
            'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
        }
        schema_registry_client = SchemaRegistryClient(sr_conf)

        # 2. Setup Protobuf Serializer (FIX: Using VehicleTelemetry instead of None)
        protobuf_serializer = ProtobufSerializer(
            VehicleTelemetry,
            schema_registry_client,
            {'use.deprecated.format': False}
        )

        # 3. Setup Kafka Producer
        producer_conf = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_API_KEY'),
            'sasl.password': os.getenv('KAFKA_API_SECRET')
        }
        producer = Producer(producer_conf)

        logger.info(f"Connection established. Streaming telemetry to topic={TOPIC_NAME}")

        for idx in range(total_events):
            vehicle = fleet[idx % len(fleet)]
            telemetry_data = build_telemetry_event(vehicle, idx, rng=rng)

            # Create the Protobuf message object
            telemetry = VehicleTelemetry()
            telemetry.vin = telemetry_data["vin"]
            telemetry.event_time.FromDatetime(telemetry_data["event_time"])
            
            telemetry.metrics.engine_temp_f = telemetry_data["engine_temp_f"]
            telemetry.metrics.speed_kmh = telemetry_data["speed_kmh"]
            telemetry.metrics.battery_soc = telemetry_data["battery_soc"]

            # Delivery callback
            def delivery_report(err, msg):
                if err:
                    logger.error(f"Failed to deliver message: {err}")
                else:
                    logger.info(f"Produced key={msg.key()} topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")

            # Produce to Kafka
            producer.produce(
                topic=TOPIC_NAME,
                key=telemetry.vin,
                value=protobuf_serializer(
                    telemetry, 
                    SerializationContext(TOPIC_NAME, MessageField.VALUE)
                ),
                on_delivery=delivery_report
            )
            
            producer.poll(0)
            logger.info(
                f"[{idx + 1}/{total_events}] queued telemetry vin={telemetry.vin} speed_kmh={telemetry.metrics.speed_kmh} temp_f={telemetry.metrics.engine_temp_f}"
            )
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
    finally:
        if 'producer' in locals():
            producer.flush()

if __name__ == "__main__":
    run_producer()