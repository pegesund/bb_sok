#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load environment variables from .env file
if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
fi

ES_HOST="${ES_HOST:-https://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
INDEX_NAME="books"

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
