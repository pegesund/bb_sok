#!/usr/bin/env python3
"""Group manifestations by work from the extracted JSON file."""

import json
from collections import defaultdict

INPUT_FILE = "manifestations_output.json"
OUTPUT_FILE = "works_output.json"


def main():
    works = defaultdict(lambda: {
        "work_id": None,
        "titles": set(),
        "authors": [],
        "languages": set(),
        "manifestations": []
    })

    print(f"Reading from {INPUT_FILE}...")
    count = 0

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            data = json.loads(line)
            work_id = data.get("work_id")

            if not work_id:
                work_id = f"unknown_{data['ean']}"

            work = works[work_id]
            work["work_id"] = work_id

            # Add titles
            for t in data.get("titles", []):
                work["titles"].add(t)

            # Add authors
            for a in data.get("authors", []):
                if a not in work["authors"]:
                    work["authors"].append(a)

            # Add language
            if data.get("language"):
                work["languages"].add(data["language"])

            # Add manifestation
            work["manifestations"].append({
                "ean": data.get("ean"),
                "title": data["titles"][0] if data.get("titles") else None,
                "translators": data.get("translators", []),
                "language": data.get("language"),
                "product_form": data.get("product_form"),
                "product_form_desc": data.get("product_form_desc"),
                "published_year": data.get("published_year"),
                "edition": data.get("edition"),
                "expression_id": data.get("expression_id"),
            })

            count += 1
            if count % 10000 == 0:
                print(f"Processed {count} manifestations, {len(works)} works...")

    print(f"\nTotal: {count} manifestations, {len(works)} unique works")

    # Convert sets to lists for JSON serialization
    print(f"Writing to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for work_id, work_data in works.items():
            work_data["titles"] = list(work_data["titles"])
            work_data["languages"] = list(work_data["languages"])
            f.write(json.dumps(work_data, ensure_ascii=False) + "\n")

    print("Done!")

    # Print some statistics
    multi_manifestation = sum(1 for w in works.values() if len(w["manifestations"]) > 1)
    multi_language = sum(1 for w in works.values() if len(w["languages"]) > 1)
    print(f"\nStatistics:")
    print(f"  Works with multiple manifestations: {multi_manifestation}")
    print(f"  Works with multiple languages: {multi_language}")


if __name__ == "__main__":
    main()
