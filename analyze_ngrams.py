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

    query = "houllebecq"
    title1 = "Tulle, Trulle, Mulle og menneskene"
    title2 = "Houellebecq"
    title3 = "Intervensjoner"

    query_ngrams = set(analyze(es, query))
    title1_ngrams = set(analyze(es, title1))
    title2_ngrams = set(analyze(es, title2))
    title3_ngrams = set(analyze(es, title3))

    print(f"Query: '{query}'")
    print(f"  Ngrams ({len(query_ngrams)}): {sorted(query_ngrams)}")
    print()

    print(f"Title 1: '{title1}'")
    print(f"  Ngrams ({len(title1_ngrams)}): {sorted(title1_ngrams)[:20]}...")
    overlap1 = query_ngrams & title1_ngrams
    print(f"  Overlap with query ({len(overlap1)}/{len(query_ngrams)} = {len(overlap1)/len(query_ngrams)*100:.1f}%): {sorted(overlap1)}")
    print()

    print(f"Title 2: '{title2}'")
    print(f"  Ngrams ({len(title2_ngrams)}): {sorted(title2_ngrams)}")
    overlap2 = query_ngrams & title2_ngrams
    print(f"  Overlap with query ({len(overlap2)}/{len(query_ngrams)} = {len(overlap2)/len(query_ngrams)*100:.1f}%): {sorted(overlap2)}")
    print()

    print(f"Title 3: '{title3}'")
    print(f"  Ngrams ({len(title3_ngrams)}): {sorted(title3_ngrams)}")
    overlap3 = query_ngrams & title3_ngrams
    print(f"  Overlap with query ({len(overlap3)}/{len(query_ngrams)} = {len(overlap3)/len(query_ngrams)*100:.1f}%): {sorted(overlap3)}")
    print()

    print("=" * 60)
    print(f"\nminimum_should_match at 70% requires {int(len(query_ngrams) * 0.7)} of {len(query_ngrams)} ngrams")
    print(f"  - Tulle... has {len(overlap1)} matching -> {'PASS' if len(overlap1) >= len(query_ngrams) * 0.7 else 'FAIL'}")
    print(f"  - Houellebecq has {len(overlap2)} matching -> {'PASS' if len(overlap2) >= len(query_ngrams) * 0.7 else 'FAIL'}")


if __name__ == "__main__":
    main()
