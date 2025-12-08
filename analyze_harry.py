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


def analyze(es, text):
    """Get ngram tokens for a text"""
    result = es.indices.analyze(
        index=INDEX_NAME,
        body={
            "analyzer": "ngram_analyzer",
            "text": text
        }
    )
    return [t["token"] for t in result["tokens"]]


def main():
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    queries = ["harri", "harry", "harri rovling", "harry rowling", "Harry Potter"]

    for q in queries:
        ngrams = analyze(es, q)
        print(f"'{q}' → {len(ngrams)} ngrams: {sorted(set(ngrams))}")
    print()

    # Check overlap between harri and harry
    harri_ngrams = set(analyze(es, "harri"))
    harry_ngrams = set(analyze(es, "harry"))
    overlap = harri_ngrams & harry_ngrams
    print(f"Overlap 'harri' vs 'harry': {len(overlap)}/{len(harri_ngrams)} = {len(overlap)/len(harri_ngrams)*100:.1f}%")
    print(f"  Matching: {sorted(overlap)}")
    print()

    # Check overlap between harri rovling and harry rowling
    harri_rovling_ngrams = set(analyze(es, "harri rovling"))
    harry_rowling_ngrams = set(analyze(es, "harry rowling"))
    overlap2 = harri_rovling_ngrams & harry_rowling_ngrams
    print(f"Overlap 'harri rovling' vs 'harry rowling': {len(overlap2)}/{len(harri_rovling_ngrams)} = {len(overlap2)/len(harri_rovling_ngrams)*100:.1f}%")
    print(f"  Matching: {sorted(overlap2)}")
    print()

    # Check overlap between harri rovling and Harry Potter by J.K. Rowling
    harri_rovling_ngrams = set(analyze(es, "harri rovling"))
    hp_rowling_ngrams = set(analyze(es, "Harry Potter J.K. Rowling"))
    overlap3 = harri_rovling_ngrams & hp_rowling_ngrams
    print(f"Overlap 'harri rovling' vs 'Harry Potter J.K. Rowling': {len(overlap3)}/{len(harri_rovling_ngrams)} = {len(overlap3)/len(harri_rovling_ngrams)*100:.1f}%")
    print(f"  Matching: {sorted(overlap3)}")


if __name__ == "__main__":
    main()
