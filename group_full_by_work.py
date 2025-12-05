#!/usr/bin/env python3
"""Group full manifestations by work from the extracted JSON file."""

import json
from collections import defaultdict

INPUT_FILE = "manifestations_full.json"
OUTPUT_FILE = "works_full.json"


def merge_unique_items(target_list, new_items, key_func=None):
    """Add items to target_list if not already present."""
    if not new_items:
        return
    for item in new_items:
        if key_func:
            if key_func(item) not in [key_func(x) for x in target_list]:
                target_list.append(item)
        elif item not in target_list:
            target_list.append(item)


def get_best_marketing_data(existing, new):
    """Merge marketing data, preferring non-empty values."""
    if not existing:
        return new
    if not new:
        return existing

    # Merge descriptions
    if new.get("descriptions") and not existing.get("descriptions"):
        existing["descriptions"] = new["descriptions"]
    elif new.get("descriptions"):
        merge_unique_items(existing["descriptions"], new["descriptions"])

    # Merge reviews
    if new.get("reviews") and not existing.get("reviews"):
        existing["reviews"] = new["reviews"]
    elif new.get("reviews"):
        merge_unique_items(existing["reviews"], new["reviews"])

    # Keep first non-null short_descriptions
    if new.get("short_descriptions") and not existing.get("short_descriptions"):
        existing["short_descriptions"] = new["short_descriptions"]

    return existing


def main():
    works = defaultdict(lambda: {
        "work_id": None,
        "work_metadata": None,
        "authors": [],
        "translators": [],
        "other_contributors": [],
        "titles": set(),  # Unique titles across manifestations
        "languages": set(),
        "marketing": None,  # Merged marketing data
        "series": [],
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
            work_data = data.get("work", {})
            work_id = work_data.get("id")

            if not work_id:
                work_id = f"unknown_{data['ean']}"

            work = works[work_id]
            work["work_id"] = work_id

            # Store work metadata (should be same for all manifestations)
            if not work["work_metadata"] and work_data:
                work["work_metadata"] = work_data

            # Add titles
            titles = data.get("titles", {})
            for mt in titles.get("main_titles", []):
                if mt.get("value"):
                    work["titles"].add(mt["value"])
                    for st in mt.get("subtitles", []):
                        if st:
                            work["titles"].add(f"{mt['value']}: {st}")
            for ot in titles.get("other_titles", []):
                if ot:
                    work["titles"].add(ot)

            # Add authors (by name to avoid duplicates)
            for a in data.get("authors", []):
                if a.get("name") and a["name"] not in [x.get("name") for x in work["authors"]]:
                    work["authors"].append(a)

            # Add translators
            for t in data.get("translators", []):
                if t.get("name") and t["name"] not in [x.get("name") for x in work["translators"]]:
                    work["translators"].append(t)

            # Add other contributors
            for c in data.get("other_contributors", []):
                if c.get("name") and c["name"] not in [x.get("name") for x in work["other_contributors"]]:
                    work["other_contributors"].append(c)

            # Add languages
            expr = data.get("expression", {})
            for lang in expr.get("language_codes", []):
                work["languages"].add(lang)

            # Merge marketing data
            if data.get("marketing"):
                work["marketing"] = get_best_marketing_data(work["marketing"], data["marketing"])

            # Add series
            for s in data.get("series", []):
                if s.get("title") and s["title"] not in [x.get("title") for x in work["series"]]:
                    work["series"].append(s)

            # Add manifestation
            work["manifestations"].append({
                "ean": data.get("ean"),
                "isbn": data.get("isbn"),
                "title": titles.get("main_titles", [{}])[0].get("value") if titles.get("main_titles") else None,
                "expression_id": expr.get("id"),
                "language": expr.get("language_codes", [None])[0] if expr.get("language_codes") else None,
                "format": expr.get("format"),
                "product_form": data.get("product_form", {}),
                "edition": data.get("edition"),
                "edition_type": data.get("edition_type"),
                "published_year": data.get("published_year"),
                "page_count": data.get("page_count"),
                "runtime_seconds": data.get("runtime_seconds"),
                "status": data.get("status"),
                "imprints": data.get("imprints", []),
                "created": data.get("created"),
                "modified": data.get("modified")
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

    # Print statistics
    multi_manifestation = sum(1 for w in works.values() if len(w["manifestations"]) > 1)
    multi_language = sum(1 for w in works.values() if len(w["languages"]) > 1)
    with_descriptions = sum(1 for w in works.values() if w["marketing"] and w["marketing"].get("descriptions"))
    with_reviews = sum(1 for w in works.values() if w["marketing"] and w["marketing"].get("reviews"))

    print(f"\nStatistics:")
    print(f"  Works with multiple manifestations: {multi_manifestation}")
    print(f"  Works with multiple languages: {multi_language}")
    print(f"  Works with descriptions: {with_descriptions}")
    print(f"  Works with reviews: {with_reviews}")


if __name__ == "__main__":
    main()
