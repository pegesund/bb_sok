import os
import sys
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


def search_with_template(es: Elasticsearch, query: str, size: int = 10):
    """Search using the stored template"""
    result = es.search_template(
        index=INDEX_NAME,
        body={
            "id": "book_search",
            "params": {
                "query_string": query,
                "size": size
            }
        }
    )
    return result["hits"]["hits"]


def main():
    if len(sys.argv) < 2:
        print("Usage: python search_books.py <query>")
        return

    query = " ".join(sys.argv[1:])

    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    print(f"Searching for: '{query}'\n")

    hits = search_with_template(es, query)

    for i, hit in enumerate(hits, 1):
        src = hit["_source"]
        score = hit["_score"]
        titles = ", ".join(src.get("titles", []))
        authors = ", ".join(src.get("authors", [])) or "Unknown"
        print(f"{i}. [{score:.2f}] {titles}")
        print(f"   Author(s): {authors}")
        print()


if __name__ == "__main__":
    main()
