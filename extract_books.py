#!/usr/bin/env python3
"""Kafka consumer to extract book titles and authors."""

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
CONSUMER_GROUP = "metaq-backend-petter-extract"

OUTPUT_FILE = "books_output.json"


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
        partition.offset = 0
    consumer.assign(partitions)
    print(f"Assigned partitions, seeking to beginning: {partitions}")


def extract_titles(manifestation):
    """Extract all titles from a manifestation."""
    titles = []
    titles_data = manifestation.get("titles", {})

    for main_title in titles_data.get("mainTitles", []):
        title_value = main_title.get("value")
        if title_value:
            titles.append(title_value)

    return titles


def extract_name_from_agent(agent_entry, agent_type):
    """Extract name from an agent entry based on type."""
    if agent_type == "verified":
        # Verified agents have nested structure: agent.nameVariants[].name/surName
        agent_data = agent_entry.get("agent", {})
        name_variants = agent_data.get("nameVariants") or []
        if name_variants:
            first_variant = name_variants[0]
            first_name = first_variant.get("name", "")
            sur_name = first_variant.get("surName", "")
            if sur_name:
                return f"{first_name} {sur_name}".strip()
            elif first_name:
                return first_name.strip()
    else:
        # Unverified/unnamed agents have direct name field
        name = agent_entry.get("name")
        if name and name.strip():
            return name.strip()
    return None


def extract_authors(manifestation):
    """Extract all authors from a manifestation."""
    authors = []

    # Check expression agents
    expression = manifestation.get("expression", {})
    agents = expression.get("agents", {})

    for agent_type in ["verified", "unverified", "unnamed"]:
        agent_list = agents.get(agent_type) or []
        for agent_entry in agent_list:
            name = extract_name_from_agent(agent_entry, agent_type)
            if name and name not in authors:
                authors.append(name)

    # Check work agents
    work = manifestation.get("work", {})
    work_agents = work.get("agents", {})

    for agent_type in ["verified", "unverified", "unnamed"]:
        agent_list = work_agents.get(agent_type) or []
        for agent_entry in agent_list:
            name = extract_name_from_agent(agent_entry, agent_type)
            if name and name not in authors:
                authors.append(name)

    return authors


def main():
    consumer = create_consumer()
    schema_registry_client = create_schema_registry_client()
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer.subscribe([TOPIC], on_assign=on_assign)
    print(f"Subscribed to topic: {TOPIC}")
    print(f"Consumer group: {CONSUMER_GROUP}")
    print(f"Output file: {OUTPUT_FILE}")
    print("Waiting for messages... (Ctrl+C to exit)\n")

    count = 0

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"Error: {msg.error()}")
                    continue

                try:
                    value = avro_deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE)
                    )
                except Exception as e:
                    print(f"Deserialization error: {e}")
                    continue

                if not isinstance(value, dict):
                    continue

                manifestation = value.get("manifestation", {})
                ean = manifestation.get("ean", "Unknown")
                titles = extract_titles(manifestation)
                authors = extract_authors(manifestation)

                count += 1
                book = {
                    "ean": ean,
                    "titles": titles,
                    "authors": authors
                }
                f.write(json.dumps(book, ensure_ascii=False) + "\n")
                f.flush()

                if count % 100 == 0:
                    print(f"Processed {count} books...")

    except KeyboardInterrupt:
        print(f"\nShutting down... Processed {count} books total.")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
