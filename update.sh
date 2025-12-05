#!/bin/bash

ES_HOST="https://localhost:9200"
ES_USER="elastic"
ES_PASSWORD="wMC4mty00n3IxVwak1oB"
INDEX_NAME="books"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Elasticsearch Index & Template Update ==="

# Delete existing index
echo "Deleting existing index '$INDEX_NAME'..."
curl -s -k -X DELETE "$ES_HOST/$INDEX_NAME" \
  -u "$ES_USER:$ES_PASSWORD" | jq .

# Create index with settings
echo "Creating index '$INDEX_NAME'..."
curl -s -k -X PUT "$ES_HOST/$INDEX_NAME" \
  -u "$ES_USER:$ES_PASSWORD" \
  -H "Content-Type: application/json" \
  -d @"$SCRIPT_DIR/index_settings.json" | jq .

# Create search template
echo "Creating search template 'book_search'..."
curl -s -k -X PUT "$ES_HOST/_scripts/book_search" \
  -u "$ES_USER:$ES_PASSWORD" \
  -H "Content-Type: application/json" \
  -d @"$SCRIPT_DIR/search_template.json" | jq .

echo "=== Done ==="
