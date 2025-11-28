import json
import os
import urllib3
import warnings
from elasticsearch import Elasticsearch, helpers

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Connecting to .* using TLS with verify_certs=False is insecure")

ES_HOST = "https://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "wMC4mty00n3IxVwak1oB"
INDEX_NAME = "books"


def generate_docs(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            yield {
                "_index": INDEX_NAME,
                "_id": doc["ean"],
                "_source": {
                    "ean": doc["ean"],
                    "titles": doc.get("titles", []),
                    "authors": doc.get("authors", [])
                }
            }


def main():
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    filepath = os.path.join(os.path.dirname(__file__), "books_output.json")

    print("Indexing documents...")
    success, failed = helpers.bulk(es, generate_docs(filepath), stats_only=True)
    print(f"Indexed {success} documents, {failed} failed")


if __name__ == "__main__":
    main()
