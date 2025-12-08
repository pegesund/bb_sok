#!/usr/bin/env python3
"""Kafka consumer to extract book titles and authors."""

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
CONSUMER_GROUP = "metaq-backend-petter-extract-v2"

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


# ONIX contributor role codes
AUTHOR_ROLES = {"A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09"}  # Written by, co-author, etc.
TRANSLATOR_ROLES = {"B06", "B08"}  # Translated by, Translator (ONIX 3)


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


def get_agent_roles(agent_entry):
    """Extract role codes from an agent entry."""
    return set(agent_entry.get("roleCodes") or [])


def extract_contributors(manifestation):
    """Extract authors and translators separately from a manifestation."""
    authors = []
    translators = []

    def process_agents(agents_dict):
        for agent_type in ["verified", "unverified", "unnamed"]:
            agent_list = agents_dict.get(agent_type) or []
            for agent_entry in agent_list:
                name = extract_name_from_agent(agent_entry, agent_type)
                if not name:
                    continue

                roles = get_agent_roles(agent_entry)

                # Check if translator
                if roles & TRANSLATOR_ROLES:
                    if name not in translators:
                        translators.append(name)
                # Check if author (or no roles = default to author)
                elif roles & AUTHOR_ROLES or not roles:
                    if name not in authors:
                        authors.append(name)

    # Check expression agents (translators typically here)
    expression = manifestation.get("expression", {})
    process_agents(expression.get("agents", {}))

    # Check work agents (authors typically here)
    work = manifestation.get("work", {})
    process_agents(work.get("agents", {}))

    return authors, translators


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
    no_message_count = 0

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    no_message_count += 1
                    if no_message_count > 10:
                        print(f"\nNo more messages. Processed {count} books.")
                        break
                    continue

                no_message_count = 0

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
                authors, translators = extract_contributors(manifestation)
                published_year = manifestation.get("publishedYear")

                count += 1
                book = {
                    "ean": ean,
                    "titles": titles,
                    "authors": authors,
                    "translators": translators,
                    "published_year": published_year
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
