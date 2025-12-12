import requests
import json

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"
TEMPLATE = "test_search"

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

# Updated template with 'from' for pagination
template_source = {
    "query": {
        "function_score": {
            "query": {
                "dis_max": {
                    "queries": [
                        {"term": {"ean": {"value": "{{query_string}}", "boost": 1000}}},
                        {"match": {"combined": {"query": "{{query_string}}", "operator": "and", "boost": 50}}},
                        {"match": {"titles.ngram": {"query": "{{query_string}}", "minimum_should_match": "60%", "auto_generate_synonyms_phrase_query": False, "boost": 30}}},
                        {"match": {"authors.ngram": {"query": "{{query_string}}", "minimum_should_match": "60%", "auto_generate_synonyms_phrase_query": False, "boost": 20}}},
                        {"match": {"translators": {"query": "{{query_string}}", "operator": "and", "boost": 3}}},
                        {"match": {"translators.ngram": {"query": "{{query_string}}", "minimum_should_match": "60%", "auto_generate_synonyms_phrase_query": False, "boost": 2}}},
                        {"match": {"combined.edge": {"query": "{{query_string}}", "operator": "and", "boost": 200}}},
                        {"match": {"combined.ngram": {"query": "{{query_string}}", "minimum_should_match": "40%", "auto_generate_synonyms_phrase_query": False, "boost": 8}}},
                        {"match": {"combined": {"query": "{{query_string}}", "fuzziness": "AUTO", "operator": "and", "boost": 100}}},
                        {"multi_match": {"query": "{{query_string}}", "fields": ["titles^3", "authors^2", "translators^0.3"], "fuzziness": "AUTO", "type": "best_fields", "operator": "and", "boost": 100}}
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
    "from": "{{from}}{{^from}}0{{/from}}",
    "size": "{{size}}{{^size}}10{{/size}}"
}

payload = {
    "script": {
        "lang": "mustache",
        "source": json.dumps(template_source),
        "options": {
            "content_type": "application/json;charset=utf-8"
        }
    }
}

response = requests.put(
    f"{ES_HOST}/_scripts/{TEMPLATE}",
    headers=headers,
    json=payload
)

if response.status_code == 200:
    print(f"Template '{TEMPLATE}' updated successfully with pagination support!")
    print(f"Response: {response.json()}")
else:
    print(f"Error: {response.status_code} - {response.text}")
