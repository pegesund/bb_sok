#!/usr/bin/env python3
"""Extract all manifestations from Kafka with work/expression info."""

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json
import signal
import sys

# Confluent Cloud configuration
BOOTSTRAP_SERVERS = "pkc-e8mp5.eu-west-1.aws.confluent.cloud:9092"
KAFKA_API_KEY = "OH6I56HRLOGEX37Z"
KAFKA_API_SECRET = "F6eH+m58Vrtj+xIOI2cMr9aLeO00MO2oDmEgn1CrqiV9P+REDEQdpi61629pTPUo"

SCHEMA_REGISTRY_URL = "https://psrc-xm8wx.eu-central-1.aws.confluent.cloud"
SCHEMA_REGISTRY_API_KEY = "LLG6JI5QZMBL7UTX"
SCHEMA_REGISTRY_API_SECRET = "unzWb5L9uZ3sbF9bK+5rYgQHTNMDzOg4EqZlD5m93gPiCvb+AEuA2LWfXdFp1czt"

TOPIC = "no.bokbasen.export.combined.product.v3"
CONSUMER_GROUP = "metaq-backend-petter-manifestations"

OUTPUT_FILE = "manifestations_output.json"

# ONIX product form codes
PRODUCT_FORMS = {
    "AA": "Audio", "AB": "Audio cassette", "AC": "Audio CD",
    "AI": "Audiobook (downloadable)", "AJ": "Audiobook (streaming)",
    "BA": "Book", "BB": "Hardback", "BC": "Paperback",
    "BD": "Loose-leaf", "BE": "Spiral bound", "BH": "Board book",
    "EA": "Digital download", "EB": "Digital download and online",
    "EC": "Digital online", "ED": "Digital download only",
}

# ONIX contributor role codes
AUTHOR_ROLES = {"A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09"}
TRANSLATOR_ROLES = {"B06", "B08"}

shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    print("\nShutdown requested, finishing current batch...")
    shutdown_requested = True


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
    print(f"Assigned partitions, seeking to beginning: {partitions}")


def extract_titles(manifestation):
    titles = []
    titles_data = manifestation.get("titles", {})
    for main_title in titles_data.get("mainTitles", []):
        title_value = main_title.get("value")
        if title_value:
            titles.append(title_value)
    return titles


def extract_name_from_agent(agent_entry, agent_type):
    if agent_type == "verified":
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
        name = agent_entry.get("name")
        if name and name.strip():
            return name.strip()
    return None


def get_agent_roles(agent_entry):
    return set(agent_entry.get("roleCodes") or [])


def extract_contributors(manifestation):
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
                if roles & TRANSLATOR_ROLES:
                    if name not in translators:
                        translators.append(name)
                elif roles & AUTHOR_ROLES or not roles:
                    if name not in authors:
                        authors.append(name)

    expression = manifestation.get("expression", {})
    process_agents(expression.get("agents", {}))
    work = manifestation.get("work", {})
    process_agents(work.get("agents", {}))

    return authors, translators


def get_product_form_description(code):
    return PRODUCT_FORMS.get(code, code or "Unknown")


def main():
    global shutdown_requested

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer = create_consumer()
    schema_registry_client = create_schema_registry_client()
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer.subscribe([TOPIC], on_assign=on_assign)
    print(f"Subscribed to topic: {TOPIC}")
    print(f"Output file: {OUTPUT_FILE}")

    count = 0
    no_message_count = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        while not shutdown_requested:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                no_message_count += 1
                if no_message_count > 10:  # 10 seconds of no messages = end of topic
                    print(f"\nNo more messages. Processed {count} manifestations.")
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
            work = manifestation.get("work", {})
            expression = manifestation.get("expression", {})
            product_form = manifestation.get("productForm", {})

            work_id = work.get("id")
            expression_id = expression.get("id")

            # Get language from expression
            language_codes = expression.get("languageCodes", [])
            language = language_codes[0] if language_codes else None

            titles = extract_titles(manifestation)
            authors, translators = extract_contributors(manifestation)

            product_form_code = product_form.get("productFormCode")

            data = {
                "ean": manifestation.get("ean"),
                "titles": titles,
                "authors": authors,
                "translators": translators,
                "work_id": str(work_id) if work_id else None,
                "expression_id": str(expression_id) if expression_id else None,
                "language": language,
                "product_form": product_form_code,
                "product_form_desc": get_product_form_description(product_form_code),
                "published_year": manifestation.get("publishedYear"),
                "edition": manifestation.get("edition"),
            }

            f.write(json.dumps(data, ensure_ascii=False) + "\n")

            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} manifestations...")
                f.flush()

    consumer.close()
    print(f"Done. Total: {count} manifestations")


if __name__ == "__main__":
    main()
