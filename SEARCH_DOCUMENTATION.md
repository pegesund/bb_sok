# Book Search with Elasticsearch

This document describes the Elasticsearch-based book search system with typo-tolerant fuzzy matching using ngrams, optimized for Nordic languages.

## Overview

The search system uses a combination of techniques to provide robust search that handles:
- Exact matches (highest priority)
- Partial matches via character n-grams
- Prefix matching via edge n-grams (autocomplete)
- Typo tolerance via Levenshtein fuzzy matching
- Nordic character normalization (Swedish/German → Norwegian)

---

## Index Structure

### Character Filters

#### 1. Nordic Normalize

Maps non-Norwegian characters to their Norwegian equivalents, preserving Nordic letters (æ, ø, å) while normalizing foreign accents.

| Input | Output | Language |
|-------|--------|----------|
| ö, Ö | ø, Ø | Swedish/German → Norwegian |
| ä, Ä | æ, Æ | Swedish/German → Norwegian |
| ü, Ü | y, Y | German → Norwegian |
| é, è, ê, ë | e | French/Spanish |
| á, à, â, ã | a | French/Spanish/Portuguese |
| í, ì, î, ï | i | Romance languages |
| ó, ò, ô, õ | o | Romance languages |
| ú, ù, û | u | Romance languages |
| ñ | n | Spanish |
| ç | c | French/Portuguese |
| ß | ss | German |

This ensures searches like "Strömberg" match "Strømberg" and vice versa.

#### 2. Remove Special

Strips all non-alphanumeric characters using regex pattern `[^\p{L}\p{N}\s]`. Keeps only:
- Letters (any language)
- Numbers
- Whitespace

---

### Tokenizers

#### Ngram Tokenizer

Splits text into overlapping character sequences of 2-3 characters.

```
"Harry" → ["Ha", "ar", "rr", "ry", "Har", "arr", "rry"]
```

This enables fuzzy matching because misspelled words share many n-grams with correct spellings:
- "houllebecq" shares 82% of n-grams with "Houellebecq"
- "harri" shares 71% of n-grams with "Harry"

**Configuration:**
- `min_gram`: 2
- `max_gram`: 3
- `token_chars`: letters and digits only

---

### Filters

#### Edge Ngram Filter

Creates prefix tokens from 1-20 characters for autocomplete functionality.

```
"Potter" → ["P", "Po", "Pot", "Pott", "Potte", "Potter"]
```

#### ASCII Folding Filter

Defined but not currently used in analyzers. Could be enabled as fallback for edge cases.

---

### Analyzers

| Analyzer | Char Filters | Tokenizer | Token Filters | Purpose |
|----------|--------------|-----------|---------------|---------|
| `ngram_analyzer` | nordic_normalize, remove_special | ngram_tokenizer | lowercase | Fuzzy matching via character n-grams |
| `edge_ngram_analyzer` | nordic_normalize, remove_special | standard | lowercase, edge_ngram_filter | Prefix/autocomplete matching |
| `search_analyzer` | nordic_normalize, remove_special | standard | lowercase | Query-time analyzer (no n-grams) |
| `standard_normalized` | nordic_normalize, remove_special | standard | lowercase | Default text analysis with Nordic support |

---

### Field Mappings

```
books/
├── ean (keyword)              # Unique identifier (ISBN/barcode), exact match only
├── titles (text)              # Book titles
│   ├── analyzer: standard_normalized
│   ├── copy_to: combined
│   └── titles.ngram           # Ngram subfield for fuzzy matching
├── authors (text)             # Author names
│   ├── analyzer: standard_normalized
│   ├── copy_to: combined
│   └── authors.ngram          # Ngram subfield for fuzzy matching
└── combined (text)            # Auto-populated from titles + authors
    ├── analyzer: standard_normalized
    ├── combined.ngram         # Ngram subfield for broad fuzzy matching
    └── combined.edge          # Edge ngram subfield for prefix/autocomplete
```

The `combined` field uses Elasticsearch's `copy_to` feature to automatically merge titles and authors into a single searchable field.

---

## Search Template

The search uses a stored template (`book_search`) with a `dis_max` query that runs multiple strategies in parallel and returns the best match.

### Query Strategies

| # | Strategy | Field | Boost | Match Requirement | Purpose |
|---|----------|-------|-------|-------------------|---------|
| 1 | EAN exact | `ean` | 1000 | Exact match | ISBN/barcode lookup (highest priority) |
| 2 | Exact match | `combined` | 50 | All terms (AND) | Exact word matches |
| 3 | Title n-gram | `titles.ngram` | 30 | 60% of terms | Fuzzy title matching |
| 4 | Author n-gram | `authors.ngram` | 20 | 60% of terms | Fuzzy author matching |
| 5 | Edge n-gram | `combined.edge` | 15 | All terms (AND) | Prefix/autocomplete |
| 6 | Combined n-gram | `combined.ngram` | 8 | 40% of terms | Broad fuzzy fallback |
| 7 | Fuzzy exact | `combined` | 100 | All terms + fuzziness | Typo-tolerant exact match |
| 8 | Multi-field fuzzy | `titles^3`, `authors^2` | 100 | All terms + fuzziness | Cross-field typo tolerance |

### Scoring: How dis_max Works

The `dis_max` query returns documents matching ANY strategy. The final score:

```
score = max(strategy_scores) + tie_breaker × sum(other_matching_scores)
```

With `tie_breaker: 0.2`, documents matching multiple strategies receive a 20% bonus from secondary matches.

**Example:** A search for "harry potter" might match:
- Strategy 1 (exact): score 50
- Strategy 4 (edge): score 15
- Strategy 6 (fuzzy): score 100

Final score: `100 + 0.2 × (50 + 15) = 113`

### Minimum Should Match

N-gram queries use `minimum_should_match` to filter low-quality matches:

| Field | Threshold | Effect |
|-------|-----------|--------|
| `titles.ngram` | 60% | At least 60% of query n-grams must match |
| `authors.ngram` | 60% | At least 60% of query n-grams must match |
| `combined.ngram` | 40% | Looser threshold for broader matching |

---

## Technical Details

### Why Ngram Tokenizer Instead of Filter?

The index uses an **ngram tokenizer** rather than an ngram filter. This is critical:

1. **Ngram filter** creates position-overlapping tokens (multiple n-grams at same position)
2. Elasticsearch treats position-overlapping tokens as **synonyms**
3. Synonym queries bypass `minimum_should_match` entirely
4. **Ngram tokenizer** creates sequential positions, making `minimum_should_match` work correctly

### Fuzzy Matching Boost Values

The fuzzy queries (6, 7) have high boost (100) because:
- N-gram scores are typically 1000+ (many matching terms)
- Fuzzy scores are typically 10-15 (fewer terms)
- High boost ensures fuzzy matches compete with n-gram matches

This matters for queries like "harri rovling" where:
- N-gram overlap with "Harry Potter J.K. Rowling" is only 61%
- Fuzzy matching correctly identifies "harri"→"harry" and "rovling"→"rowling"

### Auto-generated Synonyms Disabled

N-gram queries set `auto_generate_synonyms_phrase_query: false` to prevent Elasticsearch from creating additional synonym expansions that would interfere with scoring.

---

## Test Results

| Query | Expected | Result | Pass |
|-------|----------|--------|------|
| `harry potter` | Harry Potter | Harry Potter | ✓ |
| `harry poter` | Harry Potter | Harry Potter | ✓ |
| `harri poter` | Harry Potter | Harry Potter | ✓ |
| `harri potter` | Harry Potter | Harry Potter | ✓ |
| `harri rovling` | Harry Potter by J.K. Rowling | Harry Potter by J.K. Rowling | ✓ |
| `harry rowling` | Harry Potter by J.K. Rowling | Harry Potter by J.K. Rowling | ✓ |
| `houellebecq` | Houellebecq book | Houellebecqs hytte... | ✓ |
| `houllebecq` | Houellebecq book | Houellebecqs hytte... | ✓ |
| `hollebeck` | Houellebecq book | #2 is Houellebecq | ~ |
| `houellebecq serotonin` | Serotonin | Serotonin by Houellebecq | ✓ |
| `houllebecq serotonin` | Serotonin | Serotonin by Houellebecq | ✓ |
| `houllebeck serotonin` | Serotonin | Serotonin by Houellebecq | ✓ |
| `houellebecq underkastelse` | Underkastelse | Underkastelse by Houellebecq | ✓ |
| `houllebecq underkastelse` | Underkastelse | Underkastelse by Houellebecq | ✓ |

**Pass rate: 93% (13/14)**

---

## Files

| File | Purpose |
|------|---------|
| `create_index.py` | Creates the Elasticsearch index with analyzers and mappings |
| `create_template.py` | Creates the search template |
| `index_books.py` | Indexes books from `books_output.json` |
| `search_books.py` | CLI tool to search using the template |
| `elasticsearch_schema.json` | Exported schema (index settings + search template) |

---

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

---

## Configuration Reference

| Parameter | Location | Current Value | Effect |
|-----------|----------|---------------|--------|
| `min_gram` | ngram_tokenizer | 2 | Minimum n-gram size |
| `max_gram` | ngram_tokenizer | 3 | Maximum n-gram size |
| `max_gram` | edge_ngram_filter | 20 | Maximum prefix length |
| `minimum_should_match` | titles.ngram, authors.ngram | 60% | N-gram match threshold |
| `minimum_should_match` | combined.ngram | 40% | Broader match threshold |
| `tie_breaker` | dis_max | 0.2 | Multi-strategy score bonus |
| `boost` | ean exact | 1000 | Weight for EAN/ISBN matches (highest) |
| `boost` | fuzzy queries | 100 | Weight for fuzzy matches |
| `boost` | exact match | 50 | Weight for exact matches |
| `boost` | titles in multi_match | 3x | Favor title over author |
| `boost` | authors in multi_match | 2x | Secondary weight for authors |
| `fuzziness` | fuzzy queries | AUTO | 0 edits for 1-2 chars, 1 for 3-5, 2 for 6+ |

---

## Elasticsearch Requirements

- Elasticsearch 7.x or 8.x
- `index.max_ngram_diff: 1` (set in index settings)
