#!/usr/bin/env python3
"""Kafka consumer for Confluent Cloud with Schema Registry support."""

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json

# Confluent Cloud configuration
BOOTSTRAP_SERVERS = "pkc-e8mp5.eu-west-1.aws.confluent.cloud:9092"
KAFKA_API_KEY = "OH6I56HRLOGEX37Z"
KAFKA_API_SECRET = "F6eH+m58Vrtj+xIOI2cMr9aLeO00MO2oDmEgn1CrqiV9P+REDEQdpi61629pTPUo"

SCHEMA_REGISTRY_URL = "https://psrc-xm8wx.eu-central-1.aws.confluent.cloud"
SCHEMA_REGISTRY_API_KEY = "LLG6JI5QZMBL7UTX"
SCHEMA_REGISTRY_API_SECRET = "unzWb5L9uZ3sbF9bK+5rYgQHTNMDzOg4EqZlD5m93gPiCvb+AEuA2LWfXdFp1czt"

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
