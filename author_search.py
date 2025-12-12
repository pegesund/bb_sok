import argparse
import requests

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"
INDEX = "metaq-work-search-11"
TEMPLATE = "author_search"

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

parser = argparse.ArgumentParser(description="Search Elasticsearch by author name")
parser.add_argument("query", nargs="+", help="Author name to search")
parser.add_argument("--from", dest="from_", type=int, default=0, help="Start from result (default: 0)")
parser.add_argument("--size", type=int, default=10, help="Number of results (default: 10)")
args = parser.parse_args()

query_string = " ".join(args.query)

# Fetch extra results to account for duplicates after deduplication
fetch_size = args.size * 5

payload = {
    "id": TEMPLATE,
    "params": {
        "query_string": query_string,
        "from": 0,
        "size": fetch_size
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

    # Deduplicate by author (keep first/highest scoring per unique author set)
    seen_authors = set()
    unique_hits = []
    for hit in hits:
        source = hit.get("_source", {})
        authors_list = source.get("authors", [])
        author_key = tuple(sorted(set(authors_list)))
        if author_key not in seen_authors:
            seen_authors.add(author_key)
            unique_hits.append(hit)

    # Apply pagination to deduplicated results
    paginated = unique_hits[args.from_:args.from_ + args.size]

    print(f"Found {total} results for author: {query_string}")
    print(f"Showing {len(paginated)} unique authors (from {args.from_ + 1})\n")

    for i, hit in enumerate(paginated, args.from_ + 1):
        source = hit.get("_source", {})
        title = source.get("titles", ["No title"])[0] if source.get("titles") else "No title"
        authors = ", ".join(set(source.get("authors", []))) or "Unknown author"
        ean = source.get("eans", ["N/A"])[0] if source.get("eans") else "N/A"
        score = hit.get("_score", 0)
        print(f"{i}. {title}")
        print(f"   Author: {authors}")
        print(f"   EAN: {ean}")
        print(f"   Score: {score:.2f}")
        print()
else:
    print(f"Error: {response.status_code} - {response.text}")
