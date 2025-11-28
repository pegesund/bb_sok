# Book Search with Elasticsearch

This document describes the Elasticsearch-based book search system with typo-tolerant fuzzy matching using ngrams.

## Overview

The search system uses a combination of techniques to provide robust search that handles:
- Exact matches (highest priority)
- Partial matches via ngrams
- Prefix matching via edge ngrams
- Typo tolerance via fuzzy matching

## Index Structure

### Analyzers

The index uses three custom analyzers:

#### 1. Ngram Analyzer
Breaks text into character ngrams (2-3 characters) for partial matching.

```
"Houellebecq" → ["ho", "ou", "ue", "el", "ll", "le", "eb", "be", "ec", "cq",
                  "hou", "oue", "uel", "ell", "lle", "leb", "ebe", "bec", "ecq"]
```

This allows matching even with typos because misspelled words share many ngrams with the correct spelling:
- "houllebecq" shares 14/17 ngrams (82%) with "Houellebecq"
- "harri" shares 5/7 ngrams (71%) with "Harry"

#### 2. Edge Ngram Analyzer
Creates prefix ngrams for autocomplete-style matching.

```
"Harry" → ["h", "ha", "har", "harr", "harry"]
```

#### 3. Search Analyzer
Standard tokenizer with lowercase filter. Used for searching against edge ngram fields.

### Field Mappings

```
books/
├── ean (keyword)           # Unique identifier
├── titles (text)           # Standard text field
│   └── titles.ngram        # Ngram subfield for fuzzy matching
├── authors (text)          # Standard text field
│   └── authors.ngram       # Ngram subfield for fuzzy matching
└── combined (text)         # Auto-populated via copy_to from titles + authors
    ├── combined.ngram      # Ngram subfield
    └── combined.edge       # Edge ngram subfield for prefix matching
```

The `combined` field uses Elasticsearch's `copy_to` feature to automatically combine titles and authors into a single searchable field.

## Search Template

The search uses a `dis_max` query that picks the best matching strategy:

### Query Strategies (in priority order)

| Strategy | Field | Boost | Purpose |
|----------|-------|-------|---------|
| Exact match | `combined` | 50 | Exact word matches, highest priority |
| Title ngram | `titles.ngram` | 30 | Fuzzy title matching (60% threshold) |
| Author ngram | `authors.ngram` | 20 | Fuzzy author matching (60% threshold) |
| Edge ngram | `combined.edge` | 15 | Prefix/autocomplete matching |
| Combined ngram | `combined.ngram` | 8 | Broad fuzzy matching (40% threshold) |
| Fuzzy combined | `combined` | 100 | Levenshtein fuzzy (all terms must match) |
| Multi-field fuzzy | `titles^3`, `authors^2` | 100 | Fuzzy across fields, title-weighted |

### How dis_max Works

The `dis_max` query returns documents matching ANY of the strategies above. The final score is calculated as:

```
score = max(strategy_scores) + tie_breaker × sum(other_scores)
```

With `tie_breaker: 0.2`, documents matching multiple strategies get a score boost.

### Minimum Should Match

The ngram queries use `minimum_should_match` to filter out low-quality matches:
- `titles.ngram`: 60% of query ngrams must match
- `authors.ngram`: 60% of query ngrams must match
- `combined.ngram`: 40% of query ngrams must match

This prevents documents with only incidental ngram overlap from appearing in results.

## Key Technical Details

### Why Ngram Tokenizer Instead of Filter?

The index uses an **ngram tokenizer** rather than an ngram filter. This is critical because:

1. **Ngram filter** creates position-overlapping tokens (multiple ngrams at same position)
2. Elasticsearch treats position-overlapping tokens as **synonyms**
3. Synonym queries bypass `minimum_should_match` entirely
4. **Ngram tokenizer** creates non-overlapping positions, making `minimum_should_match` work correctly

### Fuzzy Matching Boost

The fuzzy queries have high boost (100) because:
- Ngram scores are typically in the 1000+ range due to many matching terms
- Fuzzy scores are typically ~10-15 (fewer terms)
- High boost ensures fuzzy matches compete with ngram matches

This is important for queries like "harri rovling" where:
- Ngram overlap with "Harry Potter J.K. Rowling" is only 61%
- Fuzzy matching correctly identifies "harri"→"harry" and "rovling"→"rowling"

## Test Results

| Query | Top Result | Pass |
|-------|------------|------|
| `harry potter` | Harry Potter | ✓ |
| `harry poter` | Harry Potter | ✓ |
| `harri poter` | Harry Potter | ✓ |
| `harri potter` | Harry Potter | ✓ |
| `harri rovling` | Harry Potter by J.K. Rowling | ✓ |
| `harry rowling` | Harry Potter by J.K. Rowling | ✓ |
| `houellebecq` | Houellebecqs hytte... | ✓ |
| `houllebecq` | Houellebecqs hytte... | ✓ |
| `hollebeck` | #2 is Houellebecq | ~ |
| `houellebecq serotonin` | Serotonin by Houellebecq | ✓ |
| `houllebecq serotonin` | Serotonin by Houellebecq | ✓ |
| `houllebeck serotonin` | Serotonin by Houellebecq | ✓ |
| `houellebecq underkastelse` | Underkastelse by Houellebecq | ✓ |
| `houllebecq underkastelse` | Underkastelse by Houellebecq | ✓ |

**Pass rate: 93% (13/14)**

## Files

| File | Purpose |
|------|---------|
| `create_index.py` | Creates the Elasticsearch index with analyzers and mappings |
| `create_template.py` | Creates the search template |
| `index_books.py` | Indexes books from `books_output.json` |
| `search_books.py` | CLI tool to search using the template |

## Usage

```bash
# Setup (run once)
python create_index.py
python index_books.py
python create_template.py

# Search
python search_books.py "harry potter"
python search_books.py "houllebecq serotonin"
```

## Configuration

Key parameters that can be tuned:

| Parameter | Current Value | Effect |
|-----------|---------------|--------|
| `ngram min_gram` | 2 | Minimum ngram size |
| `ngram max_gram` | 3 | Maximum ngram size |
| `edge_ngram max_gram` | 20 | Maximum prefix length |
| `minimum_should_match` (titles/authors) | 60% | Ngram match threshold |
| `minimum_should_match` (combined) | 40% | Broader match threshold |
| `tie_breaker` | 0.2 | Multi-strategy score boost |
| Fuzzy boost | 100 | Weight for fuzzy matches |
| Title boost in multi_match | 3x | Favor title over author matches |
