#!/usr/bin/env python3
"""Dump full Kafka message structure to understand all available fields."""

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
CONSUMER_GROUP = "metaq-backend-petter-dump"


def create_consumer():
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
    conf = {
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}",
    }
    return SchemaRegistryClient(conf)


def on_assign(consumer, partitions):
    for partition in partitions:
        partition.offset = 0
    consumer.assign(partitions)


def convert_for_json(obj):
    """Convert objects to JSON-serializable types."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: convert_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_for_json(v) for v in obj]
    if hasattr(obj, 'isoformat'):  # datetime
        return obj.isoformat()
    if hasattr(obj, '__str__') and type(obj).__name__ == 'UUID':
        return str(obj)
    return obj


def main():
    consumer = create_consumer()
    schema_registry_client = create_schema_registry_client()
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer.subscribe([TOPIC], on_assign=on_assign)

    count = 0
    messages = []

    try:
        while count < 5:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            value = avro_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE)
            )

            if not isinstance(value, dict):
                continue

            # Convert to JSON-serializable
            value = convert_for_json(value)
            messages.append(value)
            count += 1

    finally:
        consumer.close()

    # Write full messages to file
    with open("full_messages_sample.json", "w", encoding="utf-8") as f:
        json.dump(messages, f, indent=2, ensure_ascii=False)

    print(f"Wrote {count} full messages to full_messages_sample.json")


if __name__ == "__main__":
    main()
