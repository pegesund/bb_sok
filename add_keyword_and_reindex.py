import requests
import time

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"
INDEX = "metaq-work-search-11"

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

# Step 1: Update mapping to add keyword sub-field
print("Step 1: Updating mapping to add authors.keyword...")
mapping_payload = {
    "properties": {
        "authors": {
            "type": "text",
            "analyzer": "standard_normalized",
            "copy_to": ["combined"],
            "fields": {
                "ngram": {
                    "type": "text",
                    "analyzer": "ngram_analyzer"
                },
                "keyword": {
                    "type": "keyword"
                }
            }
        }
    }
}

response = requests.put(
    f"{ES_HOST}/{INDEX}/_mapping",
    headers=headers,
    json=mapping_payload
)

if response.status_code == 200:
    print(f"  Mapping updated: {response.json()}")
else:
    print(f"  Error updating mapping: {response.status_code} - {response.text}")
    exit(1)

# Step 2: Reindex to same index (async)
print("\nStep 2: Starting reindex...")
reindex_payload = {
    "source": {"index": INDEX},
    "dest": {"index": INDEX}
}

response = requests.post(
    f"{ES_HOST}/_reindex?wait_for_completion=false",
    headers=headers,
    json=reindex_payload
)

if response.status_code == 200:
    task_id = response.json().get("task")
    print(f"  Reindex started, task ID: {task_id}")
else:
    print(f"  Error starting reindex: {response.status_code} - {response.text}")
    exit(1)

# Step 3: Monitor reindex progress
print("\nStep 3: Monitoring reindex progress...")
while True:
    response = requests.get(
        f"{ES_HOST}/_tasks/{task_id}",
        headers=headers
    )
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
            break
    else:
        print(f"  Error checking task: {response.status_code}")

    time.sleep(2)

print("\nDone! The authors.keyword field is now available.")
