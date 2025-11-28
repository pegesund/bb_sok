import urllib3
import warnings
from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Connecting to .* using TLS with verify_certs=False is insecure")

ES_HOST = "https://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "wMC4mty00n3IxVwak1oB"
INDEX_NAME = "books"


def main():
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    query = "harri rovling"

    # Test fuzzy match on combined field
    print(f"=== Testing fuzzy match for '{query}' on combined field ===\n")

    result = es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "match": {
                    "combined": {
                        "query": query,
                        "fuzziness": "AUTO",
                        "operator": "and"
                    }
                }
            },
            "size": 5
        }
    )

    print(f"Total hits: {result['hits']['total']['value']}")
    for i, hit in enumerate(result["hits"]["hits"][:5], 1):
        src = hit["_source"]
        titles = ", ".join(src.get("titles", []))
        authors = ", ".join(src.get("authors", [])) or "Unknown"
        print(f"{i}. [{hit['_score']:.2f}] {titles}")
        print(f"   Author(s): {authors}")
    print()

    # Check what a Harry Potter book's combined field looks like
    print("=== Sample Harry Potter book ===\n")

    result = es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "bool": {
                    "must": [
                        {"match": {"titles": "Harry Potter"}},
                        {"match": {"authors": "Rowling"}}
                    ]
                }
            },
            "size": 1
        }
    )

    if result["hits"]["hits"]:
        hit = result["hits"]["hits"][0]
        src = hit["_source"]
        print(f"EAN: {src.get('ean')}")
        print(f"Titles: {src.get('titles')}")
        print(f"Authors: {src.get('authors')}")

        # Get the termvectors to see what's in combined field
        tv = es.termvectors(
            index=INDEX_NAME,
            id=hit["_id"],
            body={
                "fields": ["combined"],
                "term_statistics": False,
                "field_statistics": False,
                "positions": False,
                "offsets": False
            }
        )
        if "combined" in tv.get("term_vectors", {}):
            terms = list(tv["term_vectors"]["combined"]["terms"].keys())
            print(f"Combined field terms: {sorted(terms)}")


if __name__ == "__main__":
    main()
