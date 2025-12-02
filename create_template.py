import urllib3
import warnings
from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Connecting to .* using TLS with verify_certs=False is insecure")

ES_HOST = "https://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "wMC4mty00n3IxVwak1oB"

SEARCH_TEMPLATE = {
    "script": {
        "lang": "mustache",
        "source": {
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
                                "combined.edge": {
                                    "query": "{{query_string}}",
                                    "operator": "and",
                                    "boost": 15
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
                                "fields": ["titles^3", "authors^2"],
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
