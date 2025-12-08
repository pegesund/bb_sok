#!/usr/bin/env python3
"""Explore work structure in Kafka data."""

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
CONSUMER_GROUP = "metaq-backend-petter-explore"


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


def explore_structure(obj, path="", max_depth=4, depth=0):
    """Recursively explore dictionary structure."""
    if depth > max_depth:
        return

    if isinstance(obj, dict):
        for key, value in obj.items():
            current_path = f"{path}.{key}" if path else key
            if isinstance(value, dict):
                print(f"{current_path}: {{dict}}")
                explore_structure(value, current_path, max_depth, depth + 1)
            elif isinstance(value, list):
                print(f"{current_path}: [list, len={len(value)}]")
                if value and depth < max_depth:
                    explore_structure(value[0], f"{current_path}[0]", max_depth, depth + 1)
            else:
                val_str = str(value)[:50] if value else "None"
                print(f"{current_path}: {type(value).__name__} = {val_str}")


def main():
    consumer = create_consumer()
    schema_registry_client = create_schema_registry_client()
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer.subscribe([TOPIC], on_assign=on_assign)

    count = 0
    samples = []

    try:
        while count < 10:
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

            manifestation = value.get("manifestation", {})
            expression = manifestation.get("expression", {})
            work = manifestation.get("work", {}) if "work" in manifestation else expression.get("work", {})

            # Look for work identifiers
            sample = {
                "ean": manifestation.get("ean"),
                "title": (manifestation.get("titles", {}).get("mainTitles", [{}]) or [{}])[0].get("value"),
                "work_id": work.get("id"),
                "work_bokpiId": work.get("bokpiId"),
                "expression_id": expression.get("id"),
                "expression_bokpiId": expression.get("bokpiId"),
                "manifestation_id": manifestation.get("id"),
                "manifestation_bokpiId": manifestation.get("bokpiId"),
                "productForm": manifestation.get("productForm"),
                "productFormDetail": manifestation.get("productFormDetail"),
            }

            samples.append(sample)
            count += 1

            if count == 1:
                print("=" * 80)
                print("FULL STRUCTURE (first message):")
                print("=" * 80)
                explore_structure(value)
                print()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print("=" * 80)
    print("SAMPLE DATA (10 records):")
    print("=" * 80)
    for s in samples:
        print(json.dumps(s, ensure_ascii=False, indent=2))
        print("-" * 40)


if __name__ == "__main__":
    main()
