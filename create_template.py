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

SEARCH_TEMPLATE = {
    "script": {
        "lang": "mustache",
        "source": {
            "query": {
                "function_score": {
                    "query": {
                        "dis_max": {
                            "queries": [
                                {
                                    "term": {
                                        "ean": {
                                            "value": "{{query_string}}",
                                            "boost": 1000
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "combined": {
                                            "query": "{{query_string}}",
                                            "operator": "and",
                                            "boost": 50
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "titles.ngram": {
                                            "query": "{{query_string}}",
                                            "minimum_should_match": "60%",
                                            "auto_generate_synonyms_phrase_query": False,
                                            "boost": 30
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "authors.ngram": {
                                            "query": "{{query_string}}",
                                            "minimum_should_match": "60%",
                                            "auto_generate_synonyms_phrase_query": False,
                                            "boost": 20
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "translators": {
                                            "query": "{{query_string}}",
                                            "operator": "and",
                                            "boost": 3
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "translators.ngram": {
                                            "query": "{{query_string}}",
                                            "minimum_should_match": "60%",
                                            "auto_generate_synonyms_phrase_query": False,
                                            "boost": 2
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "combined.edge": {
                                            "query": "{{query_string}}",
                                            "operator": "and",
                                            "boost": 200
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "combined.ngram": {
                                            "query": "{{query_string}}",
                                            "minimum_should_match": "40%",
                                            "auto_generate_synonyms_phrase_query": False,
                                            "boost": 8
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "combined": {
                                            "query": "{{query_string}}",
                                            "fuzziness": "AUTO",
                                            "operator": "and",
                                            "boost": 100
                                        }
                                    }
                                },
                                {
                                    "multi_match": {
                                        "query": "{{query_string}}",
                                        "fields": ["titles^3", "authors^2", "translators^0.3"],
                                        "fuzziness": "AUTO",
                                        "type": "best_fields",
                                        "operator": "and",
                                        "boost": 100
                                    }
                                }
                            ],
                            "tie_breaker": 0.2
                        }
                    },
                    "functions": [
                        {
                            "linear": {
                                "published_year": {
                                    "origin": 2025,
                                    "scale": 20,
                                    "offset": 2,
                                    "decay": 0.7
                                }
                            },
                            "weight": 1.2
                        }
                    ],
                    "score_mode": "multiply",
                    "boost_mode": "multiply"
                }
            },
            "size": "{{size}}{{^size}}10{{/size}}"
        }
    }
}


def main():
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    print("Creating search template 'book_search'...")
    es.put_script(id="book_search", body=SEARCH_TEMPLATE)
    print("Search template created.")


if __name__ == "__main__":
    main()
