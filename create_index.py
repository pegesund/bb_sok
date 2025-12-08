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

INDEX_SETTINGS = {
    "settings": {
        "analysis": {
            "char_filter": {
                "nordic_normalize": {
                    "type": "mapping",
                    "mappings": [
                        # Swedish/German → Norwegian (preserve Nordic letters)
                        "ö => ø",
                        "Ö => Ø",
                        "ä => æ",
                        "Ä => Æ",
                        # German ü → Norwegian y
                        "ü => y",
                        "Ü => Y",
                        # French/Spanish accents on e
                        "é => e", "É => E",
                        "è => e", "È => E",
                        "ê => e", "Ê => E",
                        "ë => e", "Ë => E",
                        # Accents on a (but NOT æ)
                        "á => a", "Á => A",
                        "à => a", "À => A",
                        "â => a", "Â => A",
                        "ã => a", "Ã => A",
                        # Accents on i
                        "í => i", "Í => I",
                        "ì => i", "Ì => I",
                        "î => i", "Î => I",
                        "ï => i", "Ï => I",
                        # Accents on o (but NOT ø)
                        "ó => o", "Ó => O",
                        "ò => o", "Ò => O",
                        "ô => o", "Ô => O",
                        "õ => o", "Õ => O",
                        # Accents on u
                        "ú => u", "Ú => U",
                        "ù => u", "Ù => U",
                        "û => u", "Û => U",
                        # Other
                        "ñ => n", "Ñ => N",
                        "ç => c", "Ç => C",
                        "ÿ => y", "Ÿ => Y",
                        "ß => ss",
                    ]
                },
                "remove_special": {
                    "type": "pattern_replace",
                    "pattern": "[^\\p{L}\\p{N}\\s]",  # Remove non-letter, non-digit, non-space
                    "replacement": ""
                }
            },
            "tokenizer": {
                "ngram_tokenizer": {
                    "type": "ngram",
                    "min_gram": 2,
                    "max_gram": 3,
                    "token_chars": ["letter", "digit"]
                }
            },
            "filter": {
                "edge_ngram_filter": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 20
                },
                "ascii_fold": {
                    "type": "asciifolding",
                    "preserve_original": False
                }
            },
            "analyzer": {
                "ngram_analyzer": {
                    "type": "custom",
                    "char_filter": ["nordic_normalize", "remove_special"],
                    "tokenizer": "ngram_tokenizer",
                    "filter": ["lowercase"]
                },
                "edge_ngram_analyzer": {
                    "type": "custom",
                    "char_filter": ["nordic_normalize", "remove_special"],
                    "tokenizer": "standard",
                    "filter": ["lowercase", "edge_ngram_filter"]
                },
                "search_analyzer": {
                    "type": "custom",
                    "char_filter": ["nordic_normalize", "remove_special"],
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                },
                "standard_normalized": {
                    "type": "custom",
                    "char_filter": ["nordic_normalize", "remove_special"],
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                }
            }
        },
        "index.max_ngram_diff": 1
    },
    "mappings": {
        "properties": {
            "ean": {"type": "keyword"},
            "titles": {
                "type": "text",
                "analyzer": "standard_normalized",
                "copy_to": "combined",
                "fields": {
                    "ngram": {
                        "type": "text",
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "ngram_analyzer"
                    }
                }
            },
            "authors": {
                "type": "text",
                "analyzer": "standard_normalized",
                "copy_to": "combined",
                "fields": {
                    "ngram": {
                        "type": "text",
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "ngram_analyzer"
                    }
                }
            },
            "translators": {
                "type": "text",
                "analyzer": "standard_normalized",
                "fields": {
                    "ngram": {
                        "type": "text",
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "ngram_analyzer"
                    }
                }
            },
            "combined": {
                "type": "text",
                "analyzer": "standard_normalized",
                "fields": {
                    "ngram": {
                        "type": "text",
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "ngram_analyzer"
                    },
                    "edge": {
                        "type": "text",
                        "analyzer": "edge_ngram_analyzer",
                        "search_analyzer": "search_analyzer"
                    }
                }
            },
            "published_year": {"type": "integer"}
        }
    }
}


def main():
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    if es.indices.exists(index=INDEX_NAME):
        print(f"Deleting existing index '{INDEX_NAME}'...")
        es.indices.delete(index=INDEX_NAME)

    print(f"Creating index '{INDEX_NAME}'...")
    es.indices.create(index=INDEX_NAME, body=INDEX_SETTINGS)
    print("Index created.")


if __name__ == "__main__":
    main()
