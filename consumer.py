#!/usr/bin/env python3
"""Kafka consumer for Confluent Cloud with Schema Registry support."""

import os
from dotenv import load_dotenv
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json

load_dotenv()

# Confluent Cloud configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

TOPIC = "no.bokbasen.export.combined.product.v3"
CONSUMER_GROUP = "metaq-backend-petter"


def create_consumer():
    """Create and configure the Kafka consumer."""
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
    }
    return Consumer(conf)


def create_schema_registry_client():
    """Create Schema Registry client."""
    conf = {
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}",
    }
    return SchemaRegistryClient(conf)


def on_assign(consumer, partitions):
    """Seek to beginning when partitions are assigned."""
    for partition in partitions:
        partition.offset = 0  # OFFSET_BEGINNING
    consumer.assign(partitions)
    print(f"Assigned partitions, seeking to beginning: {partitions}")


def main():
    consumer = create_consumer()
    schema_registry_client = create_schema_registry_client()

    # Create Avro deserializer
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer.subscribe([TOPIC], on_assign=on_assign)
    print(f"Subscribed to topic: {TOPIC}")
    print(f"Consumer group: {CONSUMER_GROUP}")
    print("Waiting for messages... (Ctrl+C to exit)\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            # Deserialize the value using Avro
            try:
                value = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )
            except Exception as e:
                print(f"Deserialization error: {e}")
                # Fall back to raw value
                value = msg.value()

            print("=" * 80)
            print(f"Topic: {msg.topic()}")
            print(f"Partition: {msg.partition()}")
            print(f"Offset: {msg.offset()}")
            print(f"Key: {msg.key()}")
            print(f"Timestamp: {msg.timestamp()}")
            print("-" * 40)

            if isinstance(value, dict):
                print(json.dumps(value, indent=2, default=str, ensure_ascii=False))
            else:
                print(f"Value: {value}")

            print()

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
