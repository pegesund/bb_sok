import requests
from elasticsearch import Elasticsearch

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"

es = Elasticsearch(ES_HOST, api_key=API_KEY)

# Headers for direct API calls
headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

indices = es.cat.indices(format="json")

print("Elasticsearch Indexes:")
print("-" * 40)
for idx in indices:
    print(f"  {idx['index']}")

print("\n")
print("Search Templates:")
print("-" * 40)

# Use direct HTTP request to get stored scripts/search templates
response = requests.get(
    f"{ES_HOST}/_cluster/state/metadata?filter_path=metadata.stored_scripts",
    headers=headers
)
if response.status_code == 200:
    data = response.json()
    stored_scripts = data.get("metadata", {}).get("stored_scripts", {})
    for name in stored_scripts.keys():
        print(f"  {name}")
else:
    print(f"  Error: {response.status_code} - {response.text}")
