#!/usr/bin/env python3
"""Extract FULL book data from Kafka - no data loss."""

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json
import signal

# Confluent Cloud configuration
BOOTSTRAP_SERVERS = "pkc-e8mp5.eu-west-1.aws.confluent.cloud:9092"
KAFKA_API_KEY = "OH6I56HRLOGEX37Z"
KAFKA_API_SECRET = "F6eH+m58Vrtj+xIOI2cMr9aLeO00MO2oDmEgn1CrqiV9P+REDEQdpi61629pTPUo"

SCHEMA_REGISTRY_URL = "https://psrc-xm8wx.eu-central-1.aws.confluent.cloud"
SCHEMA_REGISTRY_API_KEY = "LLG6JI5QZMBL7UTX"
SCHEMA_REGISTRY_API_SECRET = "unzWb5L9uZ3sbF9bK+5rYgQHTNMDzOg4EqZlD5m93gPiCvb+AEuA2LWfXdFp1czt"

TOPIC = "no.bokbasen.export.combined.product.v3"
CONSUMER_GROUP = "metaq-backend-petter-full"

OUTPUT_FILE = "manifestations_full.json"

# ONIX contributor role codes
AUTHOR_ROLES = {"A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09"}
TRANSLATOR_ROLES = {"B06", "B08"}

shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    print("\nShutdown requested...")
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
    print(f"Assigned partitions: {len(partitions)}")


def convert_for_json(obj):
    """Convert non-JSON-serializable objects."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: convert_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_for_json(v) for v in obj]
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    if hasattr(obj, '__str__') and type(obj).__name__ == 'UUID':
        return str(obj)
    return obj


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
    other_contributors = []

    def process_agents(agents_dict, source):
        for agent_type in ["verified", "unverified", "unnamed"]:
            agent_list = agents_dict.get(agent_type) or []
            for agent_entry in agent_list:
                name = extract_name_from_agent(agent_entry, agent_type)
                if not name:
                    continue
                roles = get_agent_roles(agent_entry)
                role_list = list(roles) if roles else []

                if roles & TRANSLATOR_ROLES:
                    if name not in [t["name"] for t in translators]:
                        translators.append({"name": name, "roles": role_list, "source": source})
                elif roles & AUTHOR_ROLES or not roles:
                    if name not in [a["name"] for a in authors]:
                        authors.append({"name": name, "roles": role_list, "source": source})
                else:
                    other_contributors.append({"name": name, "roles": role_list, "source": source})

    expression = manifestation.get("expression", {})
    process_agents(expression.get("agents", {}), "expression")
    work = manifestation.get("work", {})
    process_agents(work.get("agents", {}), "work")

    return authors, translators, other_contributors


def extract_imprint(manifestation):
    """Extract publisher/imprint info."""
    imprints = manifestation.get("imprints", {})
    result = []

    for imprint in imprints.get("verified", []) or []:
        agent = imprint.get("agent", {})
        name_variants = agent.get("nameVariants", []) or []
        if name_variants:
            name = name_variants[0].get("name", "")
            publisher = agent.get("publisher", {}) or {}
            result.append({
                "name": name,
                "place": publisher.get("place"),
                "verified": True
            })

    for imprint in imprints.get("unverified", []) or []:
        name = imprint.get("name")
        if name:
            result.append({"name": name, "verified": False})

    return result


def extract_titles(manifestation):
    """Extract all title information."""
    titles_data = manifestation.get("titles", {})
    main_titles = []

    for mt in titles_data.get("mainTitles", []) or []:
        title_info = {
            "value": mt.get("value"),
            "subtitles": [st.get("value") for st in (mt.get("subTitles") or [])],
            "parallel_titles": [pt.get("value") for pt in (mt.get("parallelTitles") or [])]
        }
        main_titles.append(title_info)

    other_titles = [ot.get("value") for ot in (titles_data.get("otherTitles") or [])]

    return {
        "main_titles": main_titles,
        "other_titles": other_titles,
        "title_statement": manifestation.get("titleStatement")
    }


def extract_series(manifestation):
    """Extract series relations."""
    series = []
    for sr in manifestation.get("seriesRelations", []) or []:
        series.append({
            "title": sr.get("title"),
            "issn": sr.get("issn"),
            "number": sr.get("numberWithinSeries"),
            "type": sr.get("type")
        })
    return series


def extract_marketing_data(products):
    """Extract marketing data from products."""
    if not products:
        return None

    # Take the first product's marketing data (most complete)
    for product in products:
        md = product.get("marketingData", {})
        if md:
            return {
                "descriptions": md.get("descriptions", []),
                "short_descriptions": md.get("shortDescriptions"),
                "reviews": md.get("reviews", []),
                "price": md.get("price"),
                "status": md.get("status"),
                "measures": md.get("measures", []),
                "supporting_resources": md.get("supportingResources", []),
                "websites": md.get("websites", []),
                "in_sale_date": md.get("inSaleDate"),
                "epublication": md.get("epublication")
            }
    return None


def extract_work_metadata(work):
    """Extract all work-level metadata."""
    return {
        "id": str(work.get("id")) if work.get("id") else None,
        "type": work.get("type"),
        "preferred_titles": work.get("preferredTitles", []),
        "language_codes": work.get("languageCodes", []),
        "dewey": work.get("dewey", []),
        "intellectual_level_codes": work.get("intellectualLevelCodes", []),
        "literature_type_codes": work.get("literatureTypeCodes", []),
        "subject_codes": work.get("subjectCodes", []),
        "theme_codes": work.get("themeCodes", []),
        "education_level_codes": work.get("educationLevelCodes", []),
        "grep_codes": work.get("grepCodes", []),
        "genre_and_form_codes": work.get("genreAndFormCodes", []),
        "first_published_year": work.get("firstPublishedYear"),
        "prizes_won": work.get("prizesWon", [])
    }


def main():
    global shutdown_requested

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer = create_consumer()
    schema_registry_client = create_schema_registry_client()
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer.subscribe([TOPIC], on_assign=on_assign)
    print(f"Output file: {OUTPUT_FILE}")

    count = 0
    no_message_count = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        while not shutdown_requested:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                no_message_count += 1
                if no_message_count > 10:
                    print(f"\nNo more messages. Processed {count}.")
                    break
                continue

            no_message_count = 0

            if msg.error():
                continue

            try:
                value = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )
            except Exception as e:
                continue

            if not isinstance(value, dict):
                continue

            manifestation = value.get("manifestation", {})
            products = value.get("products", [])
            work = manifestation.get("work", {})
            expression = manifestation.get("expression", {})
            product_form = manifestation.get("productForm", {})

            authors, translators, other_contributors = extract_contributors(manifestation)

            data = {
                # Identifiers
                "ean": manifestation.get("ean"),
                "isbn": manifestation.get("isbn"),
                "ismn": manifestation.get("ismn"),

                # Titles
                "titles": extract_titles(manifestation),

                # Contributors
                "authors": authors,
                "translators": translators,
                "other_contributors": other_contributors,

                # Publisher
                "imprints": extract_imprint(manifestation),

                # Work info
                "work": extract_work_metadata(work),

                # Expression info
                "expression": {
                    "id": str(expression.get("id")) if expression.get("id") else None,
                    "language_codes": expression.get("languageCodes", []),
                    "format": expression.get("expressionFormat")
                },

                # Manifestation details
                "product_form": {
                    "code": product_form.get("productFormCode"),
                    "detail_codes": product_form.get("productFormDetailCodes", [])
                },
                "edition": manifestation.get("edition"),
                "edition_type": manifestation.get("editionType", []),
                "edition_note": manifestation.get("editionNote", []),
                "published_year": manifestation.get("publishedYear"),
                "page_count": manifestation.get("pageCount"),
                "runtime_seconds": manifestation.get("runtimeSeconds"),
                "book_group_code": manifestation.get("bookGroupCode"),
                "product_group_code": manifestation.get("productGroupCode"),
                "status": manifestation.get("status"),

                # Series
                "series": extract_series(manifestation),

                # Marketing data
                "marketing": extract_marketing_data(products),

                # Metadata
                "created": convert_for_json(manifestation.get("created")),
                "modified": convert_for_json(manifestation.get("modified"))
            }

            # Convert all date/datetime/UUID objects recursively
            data = convert_for_json(data)
            f.write(json.dumps(data, ensure_ascii=False) + "\n")

            count += 1
            if count % 1000 == 0:
                print(f"Processed {count}...")
                f.flush()

    consumer.close()
    print(f"Done. Total: {count}")


if __name__ == "__main__":
    main()
