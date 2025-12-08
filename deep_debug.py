import os
import urllib3
import warnings
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Connecting to .* using TLS with verify_certs=False is insecure")

ES_HOST = os.getenv("ES_HOST", "https://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD")
INDEX_NAME = "books"


def main():
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    query = "houllebecq"

    # First, let's check if there are any Houellebecq books in the index
    print("=== Searching for actual Houellebecq books ===\n")

    result = es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "match": {
                    "authors": "Houellebecq"
                }
            },
            "size": 5
        }
    )

    print(f"Books by Houellebecq: {result['hits']['total']['value']}")
    for hit in result["hits"]["hits"]:
        src = hit["_source"]
        print(f"  - {src.get('titles', [])} by {src.get('authors', [])}")
    print()

    # Now let's try a term query to see what's happening with ngrams
    print("=== Testing with explicit term queries ===\n")

    # Check which ngrams from query exist in which documents
    ngrams_to_check = ["ho", "hou", "ou", "oul", "ul", "ull", "ll", "lle", "le", "leb", "eb", "ebe", "be", "bec", "ec", "ecq", "cq"]

    # First doc: Tulle book
    result = es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "match": {
                    "titles": "Tulle Trulle Mulle"
                }
            },
            "size": 1
        }
    )

    if result["hits"]["hits"]:
        tulle_id = result["hits"]["hits"][0]["_id"]
        print(f"Tulle book ID: {tulle_id}")

        # Check which ngrams match using termvectors
        tv = es.termvectors(
            index=INDEX_NAME,
            id=tulle_id,
            body={
                "fields": ["titles.ngram"],
                "term_statistics": False,
                "field_statistics": False,
                "positions": False,
                "offsets": False
            }
        )

        if "titles.ngram" in tv.get("term_vectors", {}):
            doc_ngrams = set(tv["term_vectors"]["titles.ngram"]["terms"].keys())
            query_ngrams = set(ngrams_to_check)
            matching = doc_ngrams & query_ngrams
            print(f"Tulle doc has these query ngrams: {sorted(matching)}")
            print(f"Match count: {len(matching)}/{len(query_ngrams)} = {len(matching)/len(query_ngrams)*100:.1f}%")
        print()

    # Now let's check what the actual rendered query looks like
    print("=== Validate query using _validate/query ===\n")

    validate = es.indices.validate_query(
        index=INDEX_NAME,
        body={
            "query": {
                "match": {
                    "titles.ngram": {
                        "query": query,
                        "minimum_should_match": "70%"
                    }
                }
            }
        },
        rewrite=True
    )
    print(f"Query is valid: {validate['valid']}")
    if "explanations" in validate:
        for exp in validate["explanations"]:
            print(f"Explanation: {exp.get('explanation', 'N/A')}")
    print()

    # Let's also check what happens with a bool query explicitly constructed
    print("=== Manual bool query with explicit ngram terms ===\n")

    ngram_clauses = [{"term": {"titles.ngram": ng}} for ng in ngrams_to_check]

    result = es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "bool": {
                    "should": ngram_clauses,
                    "minimum_should_match": "70%"
                }
            },
            "size": 5
        }
    )

    print(f"Manual bool query hits: {result['hits']['total']['value']}")
    for i, hit in enumerate(result["hits"]["hits"][:5], 1):
        src = hit["_source"]
        titles = ", ".join(src.get("titles", []))
        authors = ", ".join(src.get("authors", [])) or "Unknown"
        print(f"{i}. [{hit['_score']:.2f}] {titles}")
        print(f"   Author(s): {authors}")
    print()


if __name__ == "__main__":
    main()
