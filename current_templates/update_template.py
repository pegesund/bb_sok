import requests

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

# Main search template with sorting support
work_search_template = '''{
    "query": {
        "function_score": {
            "query": {
                "dis_max": {
                    "queries": [
                        {"term": {"eans": {"value": "{{query_string}}", "boost": 1000}}},
                        {"match": {"combined": {"query": "{{query_string}}", "operator": "and", "boost": 50}}},
                        {"match": {"titles.ngram": {"query": "{{query_string}}", "minimum_should_match": "60%", "auto_generate_synonyms_phrase_query": false, "boost": 30}}},
                        {"match": {"authors.ngram": {"query": "{{query_string}}", "minimum_should_match": "60%", "auto_generate_synonyms_phrase_query": false, "boost": 20}}},
                        {"match": {"translators": {"query": "{{query_string}}", "operator": "and", "boost": 3}}},
                        {"match": {"translators.ngram": {"query": "{{query_string}}", "minimum_should_match": "60%", "auto_generate_synonyms_phrase_query": false, "boost": 2}}},
                        {"match": {"combined.edge": {"query": "{{query_string}}", "operator": "and", "boost": 200}}},
                        {"match": {"combined.ngram": {"query": "{{query_string}}", "minimum_should_match": "40%", "auto_generate_synonyms_phrase_query": false, "boost": 8}}},
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
    "post_filter": {
        "bool": {
            "filter": {{#filters}}{{#toJson}}filters{{/toJson}}{{/filters}}{{^filters}}[{"match_all":{}}]{{/filters}}
        }
    },
    "from": "{{from}}{{^from}}0{{/from}}",
    "size": "{{size}}{{^size}}10{{/size}}",
    "sort": [
        {{#sort_date_asc}}
        {"publishedYear": {"order": "asc"}},
        {"_score": {"order": "desc"}}
        {{/sort_date_asc}}
        {{#sort_date_desc}}
        {"publishedYear": {"order": "desc"}},
        {"_score": {"order": "desc"}}
        {{/sort_date_desc}}
        {{^sort_date_asc}}{{^sort_date_desc}}
        {"_score": {"order": "desc"}},
        {"publishedYear": {"order": "desc"}}
        {{/sort_date_desc}}{{/sort_date_asc}}
    ]
}'''

# Agent search template - for searching agents/authors (uses ngram like work index)
agent_search_template = '''{
    "query": {
        "function_score": {
            "query": {
                "bool": {
                    "must": {
                        "dis_max": {
                            "queries": [
                                {"match": {"names.keyword": {"query": "{{query_string}}", "operator": "and", "boost": 1000}}},
                                {"prefix": {"names.keyword": {"value": "{{query_string}}", "boost": 300}}},
                                {"match_phrase_prefix": {"names": {"query": "{{query_string}}", "boost": 400}}},
                                {"match": {"names": {"query": "{{query_string}}", "operator": "and", "boost": 500}}},
                                {"match": {"names.edge": {"query": "{{query_string}}", "operator": "and", "boost": 200}}},
                                {"match": {"names": {"query": "{{query_string}}", "fuzziness": "AUTO", "operator": "and", "boost": 100}}},
                                {"match": {"names.ngram": {"query": "{{query_string}}", "minimum_should_match": "30%", "boost": 10}}}
                            ],
                            "tie_breaker": 0.2
                        }
                    },
                    "should": [
                        {"rank_feature": {"field": "portfolio_count", "log": {"scaling_factor": 1}, "boost": 400}}
                    ]
                }
            },
            "functions": [
                {
                    "script_score": {
                        "script": {
                            "source": "def query = params.query.toLowerCase(); def firstChar = query.substring(0, 1); for (name in doc['names.keyword']) { def parts = name.toLowerCase().splitOnToken(' '); for (part in parts) { if (part.length() > 0 && part.substring(0, 1) == firstChar) { return 2.0; } } } return 1.0;",
                            "params": {"query": "{{query_string}}"}
                        }
                    },
                    "weight": 1.5
                }
            ],
            "score_mode": "multiply",
            "boost_mode": "multiply"
        }
    },
    "from": "{{from}}{{^from}}0{{/from}}",
    "size": "{{size}}{{^size}}10{{/size}}"
}'''

# EAN search template - exact match only, no fuzziness
ean_search_template = '''{
    "query": {
        "bool": {
            "filter": [
                {
                    "terms": {
                        "eans": {{#toJson}}eans{{/toJson}}
                    }
                }
            ]
        }
    },
    "from": "{{from}}{{^from}}0{{/from}}",
    "size": "{{size}}{{^size}}10{{/size}}"
}'''

def upload_template(name, source):
    payload = {
        "script": {
            "lang": "mustache",
            "source": source,
            "options": {
                "content_type": "application/json;charset=utf-8"
            }
        }
    }

    response = requests.put(
        f"{ES_HOST}/_scripts/{name}",
        headers=headers,
        json=payload
    )

    if response.status_code == 200:
        print(f"Template '{name}' updated successfully!")
    else:
        print(f"Error uploading '{name}': {response.status_code} - {response.text}")

if __name__ == "__main__":
    upload_template("work_search", work_search_template)
    upload_template("ean_search", ean_search_template)
    upload_template("agent_search", agent_search_template)
