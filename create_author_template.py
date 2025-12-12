import requests
import json

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"
TEMPLATE = "author_search"

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

template_source = {
    "query": {
        "dis_max": {
            "queries": [
                {"match": {"authors": {"query": "{{query_string}}", "operator": "and", "boost": 100}}},
                {"match": {"authors": {"query": "{{query_string}}", "fuzziness": "AUTO", "operator": "and", "boost": 80}}},
                {"match": {"authors.ngram": {"query": "{{query_string}}", "minimum_should_match": "60%", "auto_generate_synonyms_phrase_query": False, "boost": 12}}}
            ],
            "tie_breaker": 0.2
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
    print(f"Template '{TEMPLATE}' created successfully!")
    print(f"Response: {response.json()}")
else:
    print(f"Error: {response.status_code} - {response.text}")
