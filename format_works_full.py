#!/usr/bin/env python3
"""Format works_full.json to a pretty-printed JSON array."""

import json

INPUT_FILE = "works_full.json"
OUTPUT_FILE = "works_full_formatted.json"


def main():
    print(f"Reading from {INPUT_FILE}...")
    works = []

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            works.append(json.loads(line))

    print(f"Loaded {len(works)} works")
    print(f"Writing formatted JSON to {OUTPUT_FILE}...")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(works, f, indent=2, ensure_ascii=False)

    print("Done!")


if __name__ == "__main__":
    main()
