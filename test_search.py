#!/usr/bin/env python3
"""
Test suite for book search functionality.
A test passes if the expected result appears in positions 1-3.
"""

import urllib3
import warnings
from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Connecting to .* using TLS with verify_certs=False is insecure")

ES_HOST = "https://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "wMC4mty00n3IxVwak1oB"
INDEX_NAME = "books"

# Test cases: (query, expected_substring, category)
TEST_CASES = [
    # EAN exact match
    ("9788202792947", "harry potter", "EAN lookup"),

    # Prefix matching
    ("laszlo kra", "krasznahorkai", "Prefix match"),

    # László Krasznahorkai - Hungarian diacritics (á, ó)
    ("László Krasznahorkai", "krasznahorkai", "Hungarian á ó"),
    ("Laszlo Krasznahorkai", "krasznahorkai", "Hungarian á ó"),
    ("krasznahorkai", "krasznahorkai", "Last name only"),
    ("krasznahorkai laszlo", "krasznahorkai", "Name order"),
    ("Krasznahorkai László", "krasznahorkai", "Name order"),
    ("krasnahorkai", "krasznahorkai", "Fuzzy/typo"),
    ("krasznahokai", "krasznahorkai", "Fuzzy/typo"),
    ("satantango", "krasznahorkai", "Title search"),
    ("satantango krasznahorkai", "krasznahorkai", "Title + author"),

    # Harry Potter variants
    ("harry potter", "harry potter", "Exact match"),
    ("harry poter", "harry potter", "Fuzzy/typo"),
    ("harri poter", "harry potter", "Fuzzy/typo"),
    ("harri potter", "harry potter", "Fuzzy/typo"),
    ("harry j.k. rowling", "rowling", "Fuzzy/typo"),
    ("harry rowling", "rowling", "Title + author"),
    ("harry potter dødstalism", "dødstalismanene", "Fuzzy/typo"),

    # Houellebecq variants - single word
    ("houellebecq", "houellebecq", "Exact match"),
    ("houllebecq", "houellebecq", "Fuzzy/typo"),
    ("hollebeck", "houellebecq", "Fuzzy/typo"),
    ("hullebeck", "houellebecq", "Fuzzy/typo"),

    # Houellebecq variants - with book titles
    ("houellebecq serotonin", "houellebecq", "Title + author"),
    ("houllebecq serotonin", "houellebecq", "Title + author"),
    ("houllebeck serotonin", "houellebecq", "Title + author"),
    ("houellebecq underkastelse", "houellebecq", "Title + author"),
    ("houllebecq underkastelse", "houellebecq", "Title + author"),

    # Diacritical variants - ë (diaeresis)
    ("Bronte", "brontë", "Diacritic ë"),
    ("Brontë", "brontë", "Diacritic ë"),
    ("Charlotte Bronte", "brontë", "Diacritic ë"),

    # Diacritical variants - ö (Swedish/German)
    ("Marcel Moring", "möring", "Diacritic ö"),
    ("Marcel Möring", "möring", "Diacritic ö"),

    # Diacritical variants - é (French accent)
    ("Regine Deforges", "régine", "Diacritic é"),
    ("Régine Deforges", "régine", "Diacritic é"),

    # Special character removal
    ("JK Rowling", "rowling", "Punctuation"),
    ("J.K. Rowling", "rowling", "Punctuation"),
    ("J K Rowling", "rowling", "Punctuation"),

    # Nordic character variants (ö→ø, ä→æ)
    ("Jäger", "jæger", "Nordic ä↔æ"),
    ("Jæger", "jæger", "Nordic ä↔æ"),
    ("Marcel Møring", "möring", "Nordic ö↔ø"),
    ("Marcel Möring", "möring", "Nordic ö↔ø"),

    # === AUTOCOMPLETE TESTS ===
    # Simulating real user typing behavior

    # Partial title typing (user still typing)
    ("harry pot", "harry potter", "Autocomplete"),
    ("harry potte", "harry potter", "Autocomplete"),
    ("seroton", "serotonin", "Autocomplete"),
    ("underkas", "underkastelse", "Autocomplete"),
    ("satanta", "satantango", "Autocomplete"),

    # Partial author typing
    ("kraszna", "krasznahorkai", "Autocomplete"),
    ("houelle", "houellebecq", "Autocomplete"),
    ("rowli", "rowling", "Autocomplete"),

    # Partial + typo (user typing fast, makes mistake)
    ("hary potter", "harry potter", "Partial+typo"),
    ("harr potr", "harry potter", "Partial+typo"),
    ("houllebec", "houellebecq", "Partial+typo"),
    ("kraszn", "krasznahorkai", "Partial+typo"),
    ("laszlo krasz", "krasznahorkai", "Partial+typo"),

    # Partial title + partial author (autocomplete combos)
    ("harry rowl", "rowling", "Partial combo"),
    ("harry row", "rowling", "Partial combo"),
    ("potter rowl", "rowling", "Partial combo"),
    ("serot houel", "houellebecq", "Partial combo"),
    ("sero houe", "houellebecq", "Partial combo"),
    ("satan lasz", "krasznahorkai", "Partial combo"),
    ("satan kras", "krasznahorkai", "Partial combo"),
    ("underkas houel", "houellebecq", "Partial combo"),
    ("bront charl", "brontë", "Partial combo"),
    ("jæger bjørn", "bjørnstad", "Partial combo"),

    # First letters only (very early typing)
    ("har pot", "harry potter", "Short prefix"),
    ("har row", "rowling", "Short prefix"),
    ("j.k. row", "rowling", "Short prefix"),
    ("jk row", "rowling", "Short prefix"),
    ("las kra", "krasznahorkai", "Short prefix"),

    # Autocomplete with diacritics omitted
    ("laszlo kras", "krasznahorkai", "Partial+diacr"),
    ("moring store lengselen", "möring", "Partial+diacr"),
    ("regine def", "régine", "Partial+diacr"),
    ("charlotte bron", "brontë", "Partial+diacr"),

    # Common keyboard mistakes (adjacent keys, double letters)
    ("harrry potter", "harry potter", "Keyboard typo"),
    ("harry pottr", "harry potter", "Keyboard typo"),

    # Mixed case partial (user switches case mid-word)
    ("Harry POT", "harry potter", "Case mixed"),
    ("LASZLO kra", "krasznahorkai", "Case mixed"),

    # Book series partial
    ("harry potter død", "dødstalismanene", "Series partial"),

    # === DATE BOOSTING TESTS ===
    # Newer editions should rank higher than older editions
    # These tests verify that recent publications appear first

    # Classic works with multiple editions - expect recent year in top result
    ("sult hamsun", "2024", "Date boost"),
    ("harry potter rowling", "2023", "Date boost"),
]


def search(es: Elasticsearch, query: str, size: int = 3):
    """Search using the stored template and return top results."""
    result = es.search_template(
        index=INDEX_NAME,
        body={
            "id": "book_search",
            "params": {
                "query_string": query,
                "size": size
            }
        }
    )
    return result["hits"]["hits"]


def check_result(hits: list, expected: str) -> tuple[bool, int | None]:
    """
    Check if expected substring appears in any of the top 3 results.
    Returns (passed, position) where position is 1-indexed or None if not found.
    Also checks published_year for date boost tests.
    """
    for i, hit in enumerate(hits):
        titles = hit["_source"].get("titles", [])
        authors = hit["_source"].get("authors", [])
        published_year = hit["_source"].get("published_year")

        # Combine all text and check for expected substring
        full_text = " ".join(titles + authors).lower()

        # Check if expected is in text or matches published_year
        if expected.lower() in full_text:
            return True, i + 1
        if published_year and str(published_year) == expected:
            return True, i + 1

    return False, None


def format_result(hits: list, position: int | None = None, max_len: int = 55) -> tuple[str, float | None]:
    """Format a result for display. Shows matched position if provided, else top result."""
    if not hits:
        return "No results", None

    # Use matched position (1-indexed) or default to first result
    idx = (position - 1) if position else 0
    if idx >= len(hits):
        idx = 0

    hit = hits[idx]
    titles = ", ".join(hit["_source"].get("titles", []))
    authors = ", ".join(hit["_source"].get("authors", [])) or "Unknown"
    year = hit["_source"].get("published_year", "")
    score = hit.get("_score")

    year_str = f"({year}) " if year else ""
    result = f"{year_str}{titles[:18]} | {authors[:28]}"
    return result[:max_len], score


def run_tests():
    """Run all test cases and print results."""
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    # Check connection
    if not es.ping():
        print("ERROR: Cannot connect to Elasticsearch")
        return False

    print("=" * 135)
    print("BOOK SEARCH TEST SUITE")
    print("=" * 135)
    print(f"Checking if expected result appears in positions 1-3")
    print()

    passed = 0
    failed = 0
    results = []

    for query, expected, category in TEST_CASES:
        hits = search(es, query)
        success, position = check_result(hits, expected)

        if success:
            passed += 1
            status = f"✓ PASS (#{position})"
        else:
            failed += 1
            status = "✗ FAIL"

        matched_result, score = format_result(hits, position)
        results.append((category, query, expected, status, matched_result, score))

    # Print results table
    print(f"{'Category':<15} {'Query':<28} {'Expected':<15} {'Status':<12} {'Matched Result':<57} {'Score'}")
    print("-" * 135)

    for category, query, expected, status, matched_result, score in results:
        score_str = f"{score:.1f}" if score else "-"
        print(f"{category:<15} {query:<28} {expected:<15} {status:<12} {matched_result:<57} {score_str}")

    print("-" * 135)
    print()

    # Summary
    total = passed + failed
    percentage = (passed / total) * 100 if total > 0 else 0

    print(f"RESULTS: {passed}/{total} passed ({percentage:.0f}%)")
    print()

    if failed == 0:
        print("🎉 All tests passed!")
    else:
        print(f"⚠️  {failed} test(s) failed")

    return failed == 0


def run_single_test(query: str, expected: str):
    """Run a single test case with detailed output."""
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )

    print(f"Query: '{query}'")
    print(f"Expected: '{expected}'")
    print()

    hits = search(es, query, size=5)
    success, position = check_result(hits[:3], expected)

    print("Top 5 results:")
    for i, hit in enumerate(hits, 1):
        titles = ", ".join(hit["_source"].get("titles", []))
        authors = ", ".join(hit["_source"].get("authors", [])) or "Unknown"
        score = hit["_score"]

        marker = " ←" if i == position else ""
        print(f"  {i}. [{score:.2f}] {titles[:40]}")
        print(f"      by {authors[:40]}{marker}")

    print()
    if success:
        print(f"✓ PASS - Found at position #{position}")
    else:
        print("✗ FAIL - Not found in top 3")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Run single test with provided query
        query = " ".join(sys.argv[1:])
        # Try to find matching test case
        matching = [(q, e, c) for q, e, c in TEST_CASES if q == query]
        if matching:
            run_single_test(matching[0][0], matching[0][1])
        else:
            print(f"No test case found for '{query}'")
            print("Running as ad-hoc search...")
            run_single_test(query, query.split()[0])
    else:
        # Run full test suite
        success = run_tests()
        sys.exit(0 if success else 1)
