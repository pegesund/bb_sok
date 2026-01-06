import requests
import time
import json

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"
ALIAS = "metaq-agent-alias"
NEW_INDEX = "metaq-agent-v3"

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

# Index settings with ngram analyzers (matching work index)
index_settings = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "max_ngram_diff": 3,
            "analysis": {
                "filter": {
                    "truncate_filter": {
                        "length": "10",
                        "type": "truncate"
                    },
                    "edge_ngram_preserve": {
                        "min_gram": "1",
                        "type": "edge_ngram",
                        "preserve_original": "true",
                        "max_gram": "10"
                    },
                    "edge_ngram_filter": {
                        "type": "edge_ngram",
                        "min_gram": "1",
                        "max_gram": "20"
                    },
                    "ngram_preserve": {
                        "type": "ngram",
                        "preserve_original": "true"
                    }
                },
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
                        "pattern": "[^\\p{L}\\p{N}\\s]",
                        "type": "pattern_replace",
                        "replacement": ""
                    }
                },
                "analyzer": {
                    "suggest_analyzer": {
                        "filter": ["lowercase", "ngram_preserve", "edge_ngram_preserve"],
                        "type": "custom",
                        "tokenizer": "standard"
                    },
                    "suggest_search_analyzer": {
                        "filter": ["lowercase", "truncate_filter"],
                        "type": "custom",
                        "tokenizer": "standard"
                    },
                    "ngram_analyzer": {
                        "filter": ["lowercase"],
                        "char_filter": ["nordic_normalize", "remove_special"],
                        "type": "custom",
                        "tokenizer": "ngram_tokenizer"
                    },
                    "standard_normalized": {
                        "filter": ["lowercase"],
                        "char_filter": ["nordic_normalize", "remove_special"],
                        "type": "custom",
                        "tokenizer": "standard"
                    },
                    "search_analyzer": {
                        "filter": ["lowercase"],
                        "char_filter": ["nordic_normalize", "remove_special"],
                        "type": "custom",
                        "tokenizer": "standard"
                    },
                    "edge_ngram_analyzer": {
                        "filter": ["lowercase", "edge_ngram_filter"],
                        "char_filter": ["nordic_normalize", "remove_special"],
                        "type": "custom",
                        "tokenizer": "standard"
                    }
                },
                "tokenizer": {
                    "ngram_tokenizer": {
                        "token_chars": ["letter", "digit"],
                        "min_gram": "2",
                        "type": "ngram",
                        "max_gram": "4"
                    }
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "names": {
                "type": "text",
                "analyzer": "standard_normalized",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "suggest": {
                        "type": "text",
                        "analyzer": "suggest_analyzer",
                        "search_analyzer": "suggest_search_analyzer"
                    },
                    "ngram": {
                        "type": "text",
                        "analyzer": "ngram_analyzer"
                    },
                    "edge": {
                        "type": "text",
                        "analyzer": "edge_ngram_analyzer",
                        "search_analyzer": "search_analyzer"
                    },
                    "completion": {
                        "type": "completion",
                        "analyzer": "simple",
                        "preserve_separators": True,
                        "preserve_position_increments": True,
                        "max_input_length": 50
                    }
                }
            },
            "name_inverted": {"type": "keyword"},
            "roles": {"type": "keyword"},
            "portfolio_count": {"type": "rank_feature"},
            "agent_type": {"type": "keyword"},
            "countries": {
                "type": "nested",
                "properties": {
                    "code": {"type": "keyword"},
                    "labels": {
                        "type": "nested",
                        "properties": {
                            "lang": {"type": "keyword"},
                            "value": {"type": "text"}
                        }
                    }
                }
            },
            "birth_year": {"type": "integer"},
            "death_year": {"type": "integer"},
            "place": {"type": "text"}
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


def create_new_index():
    """Create the new index with ngram analyzers"""
    print(f"Creating new index '{NEW_INDEX}'...")

    # First check if it already exists
    response = requests.head(f"{ES_HOST}/{NEW_INDEX}", headers=headers)
    if response.status_code == 200:
        print(f"  Index '{NEW_INDEX}' already exists. Deleting...")
        requests.delete(f"{ES_HOST}/{NEW_INDEX}", headers=headers)

    response = requests.put(
        f"{ES_HOST}/{NEW_INDEX}",
        headers=headers,
        json=index_settings
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

        time.sleep(2)


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
    print("Agent Index Upgrade Script")
    print("=" * 60)

    # Step 1: Get current index
    print("\nStep 1: Getting current index from alias...")
    current_index = get_current_index()
    if not current_index:
        print("  Error: Could not find current index for alias")
        return
    print(f"  Current index: {current_index}")

    # Step 2: Create new index
    print("\nStep 2: Creating new index with ngram analyzers...")
    if not create_new_index():
        return

    # Step 3: Reindex
    print("\nStep 3: Reindexing documents...")
    task_id = reindex(current_index)
    if not task_id:
        return

    # Step 4: Monitor reindex
    if not monitor_reindex(task_id):
        return

    # Step 5: Switch alias
    print("\nStep 4: Switching alias...")
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
