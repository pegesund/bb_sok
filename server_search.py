import sys
import argparse
import requests

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"
INDEX = "metaq-work-search-11"
TEMPLATE = "test_search"

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

parser = argparse.ArgumentParser(description="Search Elasticsearch using template")
parser.add_argument("query", nargs="+", help="Search query")
parser.add_argument("--from", dest="from_", type=int, default=0, help="Start from result (default: 0)")
parser.add_argument("--size", type=int, default=10, help="Number of results (default: 10)")
args = parser.parse_args()

query_string = " ".join(args.query)

payload = {
    "id": TEMPLATE,
    "params": {
        "query_string": query_string,
        "from": args.from_,
        "size": args.size
    }
}

response = requests.post(
    f"{ES_HOST}/{INDEX}/_search/template",
    headers=headers,
    json=payload
)

if response.status_code == 200:
    data = response.json()
    hits = data.get("hits", {}).get("hits", [])
    total = data['hits']['total']['value']
    print(f"Found {total} results for: {query_string}")
    print(f"Showing {args.from_ + 1}-{args.from_ + len(hits)} of {total}\n")
    for i, hit in enumerate(hits, args.from_ + 1):
        source = hit.get("_source", {})
        title = source.get("titles", ["No title"])[0] if source.get("titles") else "No title"
        authors = ", ".join(source.get("authors", [])) or "Unknown author"
        ean = source.get("ean", "N/A")
        score = hit.get("_score", 0)
        print(f"{i}. {title}")
        print(f"   Author: {authors}")
        print(f"   EAN: {ean}")
        print(f"   Score: {score:.2f}")
        print()
else:
    print(f"Error: {response.status_code} - {response.text}")
