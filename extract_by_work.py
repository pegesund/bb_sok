#!/usr/bin/env python3
"""Extract books from Kafka and group by work."""

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
CONSUMER_GROUP = "metaq-backend-petter-works"

OUTPUT_FILE = "works_output.json"

# ONIX product form codes
PRODUCT_FORMS = {
    "AA": "Audio",
    "AB": "Audio cassette",
    "AC": "Audio CD",
    "AD": "Audio DAT",
    "AE": "Audio disc",
    "AF": "Audio tape",
    "AG": "Audio MiniDisc",
    "AH": "Super Audio CD",
    "AI": "Audiobook (downloadable)",
    "AJ": "Audiobook (streaming)",
    "AZ": "Other audio format",
    "BA": "Book",
    "BB": "Hardback",
    "BC": "Paperback",
    "BD": "Loose-leaf",
    "BE": "Spiral bound",
    "BF": "Pamphlet",
    "BG": "Leather",
    "BH": "Board book",
    "BI": "Rag book",
    "BJ": "Bath book",
    "BK": "Novelty book",
    "BL": "Slide bound",
    "BM": "Big book",
    "BN": "Part-work",
    "BO": "Fold-out book",
    "BP": "Foam book",
    "BZ": "Other book format",
    "DA": "Digital (other)",
    "DB": "CD-ROM",
    "DC": "CD-I",
    "DD": "Game cartridge",
    "DE": "DVD-ROM",
    "DF": "Secure Digital (SD) Memory Card",
    "DG": "Compact Flash Memory Card",
    "DH": "Memory Stick Memory Card",
    "DI": "USB Flash Drive",
    "DJ": "Double-sided CD/DVD",
    "DK": "Blu-ray",
    "DZ": "Other digital carrier",
    "EA": "Digital download",
    "EB": "Digital download and online",
    "EC": "Digital online",
    "ED": "Digital download only",
}

# ONIX contributor role codes
AUTHOR_ROLES = {"A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09"}
TRANSLATOR_ROLES = {"B06", "B08"}


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
    """Extract authors and translators separately."""
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
    """Get human-readable product form description."""
    return PRODUCT_FORMS.get(code, code or "Unknown")


def extract_manifestation_data(manifestation):
    """Extract all relevant data from a manifestation."""
    work = manifestation.get("work", {})
    expression = manifestation.get("expression", {})
    product_form = manifestation.get("productForm", {})

    work_id = work.get("id")
    if work_id:
        work_id = str(work_id)

    expression_id = expression.get("id")
    if expression_id:
        expression_id = str(expression_id)

    titles = extract_titles(manifestation)
    authors, translators = extract_contributors(manifestation)

    product_form_code = product_form.get("productFormCode")
    product_form_details = product_form.get("productFormDetailCodes", [])

    return {
        "ean": manifestation.get("ean"),
        "isbn": manifestation.get("isbn"),
        "titles": titles,
        "authors": authors,
        "translators": translators,
        "work_id": work_id,
        "expression_id": expression_id,
        "product_form": product_form_code,
        "product_form_desc": get_product_form_description(product_form_code),
        "product_form_details": product_form_details,
        "published_year": manifestation.get("publishedYear"),
        "edition": manifestation.get("edition"),
        "status": manifestation.get("status"),
    }


def main():
    consumer = create_consumer()
    schema_registry_client = create_schema_registry_client()
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer.subscribe([TOPIC], on_assign=on_assign)
    print(f"Subscribed to topic: {TOPIC}")
    print(f"Consumer group: {CONSUMER_GROUP}")
    print(f"Output file: {OUTPUT_FILE}")
    print("Waiting for messages... (Ctrl+C to exit)\n")

    # Group manifestations by work_id
    works = {}
    count = 0

    try:
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
            data = extract_manifestation_data(manifestation)

            work_id = data["work_id"]
            if not work_id:
                work_id = f"unknown_{data['ean']}"

            if work_id not in works:
                works[work_id] = {
                    "work_id": work_id,
                    "titles": set(),
                    "authors": [],
                    "manifestations": []
                }

            # Add titles from this manifestation
            for t in data["titles"]:
                works[work_id]["titles"].add(t)

            # Add authors if not already present
            for a in data["authors"]:
                if a not in works[work_id]["authors"]:
                    works[work_id]["authors"].append(a)

            # Add manifestation
            works[work_id]["manifestations"].append({
                "ean": data["ean"],
                "isbn": data["isbn"],
                "title": data["titles"][0] if data["titles"] else None,
                "translators": data["translators"],
                "product_form": data["product_form"],
                "product_form_desc": data["product_form_desc"],
                "published_year": data["published_year"],
                "edition": data["edition"],
                "expression_id": data["expression_id"],
            })

            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} manifestations, {len(works)} works...")

    except KeyboardInterrupt:
        print(f"\nShutting down... Processed {count} manifestations total.")
    finally:
        consumer.close()
        print(f"Found {len(works)} unique works.")

        # Convert sets to lists for JSON serialization
        for work_id, work_data in works.items():
            work_data["titles"] = list(work_data["titles"])

        # Write output
        print(f"Writing to {OUTPUT_FILE}...")
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for work_id, work_data in works.items():
                f.write(json.dumps(work_data, ensure_ascii=False) + "\n")

        print("Done.")


if __name__ == "__main__":
    main()
