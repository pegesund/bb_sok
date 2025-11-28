import sys
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
    if len(sys.argv) < 2:
        print("Usage: python debug_search.py <query>")
        return

    query = " ".join(sys.argv[1:])

    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    # First, let's see how the query is analyzed
    print(f"=== Analyzing query: '{query}' ===\n")

    # Analyze with ngram_analyzer
    analysis = es.indices.analyze(
        index=INDEX_NAME,
        body={
            "analyzer": "ngram_analyzer",
            "text": query
        }
    )
    ngram_tokens = [t["token"] for t in analysis["tokens"]]
    print(f"Ngram tokens ({len(ngram_tokens)} total):")
    print(", ".join(ngram_tokens[:30]))
    if len(ngram_tokens) > 30:
        print(f"... and {len(ngram_tokens) - 30} more")
    print()

    # Now let's search with ONLY the titles.ngram field to isolate the issue
    print("=== Testing titles.ngram with 70% minimum_should_match ===\n")

    result = es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "match": {
                    "titles.ngram": {
                        "query": query,
                        "minimum_should_match": "70%"
                    }
                }
            },
            "size": 5
        }
    )

    print(f"Total hits: {result['hits']['total']['value']}")
    print()

    for i, hit in enumerate(result["hits"]["hits"][:5], 1):
        src = hit["_source"]
        titles = ", ".join(src.get("titles", []))
        authors = ", ".join(src.get("authors", [])) or "Unknown"
        print(f"{i}. [{hit['_score']:.2f}] {titles}")
        print(f"   Author(s): {authors}")
        print()

    # Now let's use _explain on the top hit to see why it matched
    if result["hits"]["hits"]:
        top_hit_id = result["hits"]["hits"][0]["_id"]
        print(f"=== Explain for top hit (ID: {top_hit_id}) ===\n")

        explain = es.explain(
            index=INDEX_NAME,
            id=top_hit_id,
            body={
                "query": {
                    "match": {
                        "titles.ngram": {
                            "query": query,
                            "minimum_should_match": "70%"
                        }
                    }
                }
            }
        )

        # Print a simplified explanation
        def print_explanation(exp, indent=0):
            prefix = "  " * indent
            desc = exp.get("description", "")
            value = exp.get("value", 0)

            # Only print interesting parts
            if "weight" in desc or "score" in desc or "match" in desc.lower() or "sum of" in desc:
                print(f"{prefix}{value:.4f} - {desc[:100]}")

            for detail in exp.get("details", []):
                print_explanation(detail, indent + 1)

        print_explanation(explain["explanation"])


if __name__ == "__main__":
    main()
