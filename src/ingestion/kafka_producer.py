import os
import time
import random
from dotenv import load_dotenv

# Confluent & Protobuf Imports
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Your Utils & Generated Protobuf Class
from utils.logger import get_logger
from src.ingestion.telemetry_pb2 import VehicleTelemetry

load_dotenv()

def run_producer():
    logger = get_logger("KAFKA_PRODUCER")
    
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

        logger.info("🚀 Connection established. Streaming telemetry to South Carolina...")

        while True:
            # Create the Protobuf message object
            telemetry = VehicleTelemetry()
            telemetry.vin = "VIN-DUIS-2026"
            telemetry.event_time.GetCurrentTime() 
            
            # Updated to match the "Version 1" fields
            telemetry.metrics.engine_temp_f = round(random.uniform(190, 220), 2)
            telemetry.metrics.speed_kmh = random.randint(80, 140)
            telemetry.metrics.battery_soc = random.randint(20, 100) # Added this

            # ... rest of the code remains the same ...

            # Delivery callback
            def delivery_report(err, msg):
                if err:
                    logger.error(f"❌ Failed to deliver message: {err}")
                else:
                    logger.info(f"✅ Produced: {msg.key()} | Offset: {msg.offset()}")

            # Produce to Kafka
            producer.produce(
                topic='telemetry.raw',
                key=telemetry.vin,
                value=protobuf_serializer(
                    telemetry, 
                    SerializationContext('telemetry.raw', MessageField.VALUE)
                ),
                on_delivery=delivery_report
            )
            
            producer.poll(0)
            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("🛑 Producer stopped by user.")
    except Exception as e:
        logger.error(f"💥 Critical Error: {e}")
    finally:
        if 'producer' in locals():
            producer.flush()

if __name__ == "__main__":
    run_producer()