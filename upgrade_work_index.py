import requests
import time
import json

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"
ALIAS = "metaq-work-alias"
NEW_INDEX = "metaq-work-search-12"

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

# Updated settings with max_gram: 4 for ngram tokenizer
new_settings = {
    "index": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "max_ngram_diff": 3
    },
    "analysis": {
        "char_filter": {
            "nordic_normalize": {
                "type": "mapping",
                "mappings": [
                    "ö => ø", "Ö => Ø", "ä => æ", "Ä => Æ",
                    "ü => y", "Ü => Y", "é => e", "É => E",
                    "è => e", "È => E", "ê => e", "Ê => E",
                    "ë => e", "Ë => E", "á => a", "Á => A",
                    "à => a", "À => A", "â => a", "Â => A",
                    "ã => a", "Ã => A", "í => i", "Í => I",
                    "ì => i", "Ì => I", "î => i", "Î => I",
                    "ï => i", "Ï => I", "ó => o", "Ó => O",
                    "ò => o", "Ò => O", "ô => o", "Ô => O",
                    "õ => o", "Õ => O", "ú => u", "Ú => U",
                    "ù => u", "Ù => U", "û => u", "Û => U",
                    "ñ => n", "Ñ => N", "ç => c", "Ç => C",
                    "ÿ => y", "Ÿ => Y", "ß => ss"
                ]
            },
            "remove_special": {
                "type": "pattern_replace",
                "pattern": "[^\\p{L}\\p{N}\\s]",
                "replacement": ""
            }
        },
        "tokenizer": {
            "ngram_tokenizer": {
                "type": "ngram",
                "min_gram": 2,
                "max_gram": 4,
                "token_chars": ["letter", "digit"]
            }
        },
        "analyzer": {
            "norwegian_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "norwegian_stemmer"]
            },
            "norwegian_search_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "norwegian_stemmer", "synonyms_filter"]
            },
            "suggest_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "ngram_preserve", "edge_ngram_preserve"]
            },
            "suggest_search_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "truncate_filter", "synonyms_filter"]
            },
            "ngram_analyzer": {
                "type": "custom",
                "char_filter": ["nordic_normalize", "remove_special"],
                "tokenizer": "ngram_tokenizer",
                "filter": ["lowercase"]
            },
            "edge_ngram_analyzer": {
                "type": "custom",
                "char_filter": ["nordic_normalize", "remove_special"],
                "tokenizer": "standard",
                "filter": ["lowercase", "edge_ngram_filter"]
            },
            "search_analyzer": {
                "type": "custom",
                "char_filter": ["nordic_normalize", "remove_special"],
                "tokenizer": "standard",
                "filter": ["lowercase"]
            },
            "standard_normalized": {
                "type": "custom",
                "char_filter": ["nordic_normalize", "remove_special"],
                "tokenizer": "standard",
                "filter": ["lowercase"]
            }
        },
        "normalizer": {
            "keyword_lowercase": {
                "type": "custom",
                "filter": ["lowercase"]
            }
        },
        "filter": {
            "norwegian_stemmer": {
                "type": "stemmer",
                "language": "norwegian"
            },
            "ngram_preserve": {
                "type": "ngram",
                "preserve_original": True
            },
            "edge_ngram_preserve": {
                "type": "edge_ngram",
                "preserve_original": True,
                "min_gram": 1,
                "max_gram": 10
            },
            "truncate_filter": {
                "type": "truncate",
                "length": 10
            },
            "synonyms_filter": {
                "type": "synonym_graph",
                "synonyms_set": "metaq-synonyms",
                "updateable": True
            },
            "edge_ngram_filter": {
                "type": "edge_ngram",
                "min_gram": 1,
                "max_gram": 20
            },
            "ascii_fold": {
                "type": "asciifolding",
                "preserve_original": False
            }
        }
    }
}


def get_current_index():
    """Get the current index name from the alias"""
    response = requests.get(f"{ES_HOST}/_alias/{ALIAS}", headers=headers)
    if response.status_code == 200:
        indices = list(response.json().keys())
        if indices:
            return indices[0]
    return None


def get_mapping(index_name):
    """Get mapping from existing index"""
    response = requests.get(f"{ES_HOST}/{index_name}/_mapping", headers=headers)
    if response.status_code == 200:
        return response.json()[index_name]["mappings"]
    return None


def create_new_index(mapping):
    """Create the new index with updated settings"""
    print(f"Creating new index '{NEW_INDEX}'...")

    # First check if it already exists
    response = requests.head(f"{ES_HOST}/{NEW_INDEX}", headers=headers)
    if response.status_code == 200:
        print(f"  Index '{NEW_INDEX}' already exists. Deleting...")
        requests.delete(f"{ES_HOST}/{NEW_INDEX}", headers=headers)

    index_config = {
        "settings": new_settings,
        "mappings": mapping
    }

    response = requests.put(
        f"{ES_HOST}/{NEW_INDEX}",
        headers=headers,
        json=index_config
    )

    if response.status_code == 200:
        print(f"  Index created successfully!")
        return True
    else:
        print(f"  Error creating index: {response.status_code} - {response.text}")
        return False


def reindex(source_index):
    """Reindex from source to new index"""
    print(f"\nReindexing from '{source_index}' to '{NEW_INDEX}'...")

    reindex_payload = {
        "source": {"index": source_index},
        "dest": {"index": NEW_INDEX}
    }

    response = requests.post(
        f"{ES_HOST}/_reindex?wait_for_completion=false",
        headers=headers,
        json=reindex_payload
    )

    if response.status_code == 200:
        task_id = response.json().get("task")
        print(f"  Reindex started, task ID: {task_id}")
        return task_id
    else:
        print(f"  Error starting reindex: {response.status_code} - {response.text}")
        return None


def monitor_reindex(task_id):
    """Monitor reindex progress"""
    print("\nMonitoring reindex progress...")
    while True:
        response = requests.get(f"{ES_HOST}/_tasks/{task_id}", headers=headers)
        if response.status_code == 200:
            task_info = response.json()
            completed = task_info.get("completed", False)
            status = task_info.get("task", {}).get("status", {})
            total = status.get("total", 0)
            created = status.get("created", 0)
            updated = status.get("updated", 0)

            print(f"  Progress: {created + updated}/{total} documents processed", end="\r")

            if completed:
                print(f"\n  Reindex completed!")
                print(f"  Created: {created}, Updated: {updated}, Total: {total}")
                return True
        else:
            print(f"  Error checking task: {response.status_code}")
            return False

        time.sleep(5)


def switch_alias(old_index):
    """Switch alias from old index to new index"""
    print(f"\nSwitching alias '{ALIAS}' from '{old_index}' to '{NEW_INDEX}'...")

    alias_payload = {
        "actions": [
            {"remove": {"index": old_index, "alias": ALIAS}},
            {"add": {"index": NEW_INDEX, "alias": ALIAS}}
        ]
    }

    response = requests.post(
        f"{ES_HOST}/_aliases",
        headers=headers,
        json=alias_payload
    )

    if response.status_code == 200:
        print(f"  Alias switched successfully!")
        return True
    else:
        print(f"  Error switching alias: {response.status_code} - {response.text}")
        return False


def main():
    print("=" * 60)
    print("Work Index Upgrade Script (max_gram: 4)")
    print("=" * 60)

    # Step 1: Get current index
    print("\nStep 1: Getting current index from alias...")
    current_index = get_current_index()
    if not current_index:
        print("  Error: Could not find current index for alias")
        return
    print(f"  Current index: {current_index}")

    # Step 2: Get mapping from current index
    print("\nStep 2: Getting mapping from current index...")
    mapping = get_mapping(current_index)
    if not mapping:
        print("  Error: Could not get mapping from current index")
        return
    print(f"  Mapping retrieved successfully")

    # Step 3: Create new index
    print("\nStep 3: Creating new index with max_gram: 4...")
    if not create_new_index(mapping):
        return

    # Step 4: Reindex
    print("\nStep 4: Reindexing documents...")
    task_id = reindex(current_index)
    if not task_id:
        return

    # Step 5: Monitor reindex
    if not monitor_reindex(task_id):
        return

    # Step 6: Switch alias
    print("\nStep 5: Switching alias...")
    if not switch_alias(current_index):
        return

    print("\n" + "=" * 60)
    print("Upgrade completed successfully!")
    print(f"Old index: {current_index}")
    print(f"New index: {NEW_INDEX}")
    print(f"Alias: {ALIAS} -> {NEW_INDEX}")
    print("=" * 60)

    print("\nNote: You may want to delete the old index after verifying everything works:")
    print(f"  curl -X DELETE '{ES_HOST}/{current_index}'")


if __name__ == "__main__":
    main()
