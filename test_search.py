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

# Test cases: (query, expected_substring_in_title_or_author)
TEST_CASES = [
    # Harry Potter variants
    ("harry potter", "harry potter"),
    ("harry poter", "harry potter"),
    ("harri poter", "harry potter"),
    ("harri potter", "harry potter"),
    ("harri rovling", "rowling"),
    ("harry rowling", "rowling"),
    ("harry potter dødstalism", "dødstalismanene"),

    # Houellebecq variants - single word
    ("houellebecq", "houellebecq"),
    ("houllebecq", "houellebecq"),
    ("hollebeck", "houellebecq"),
    ("hullebeck", "houellebecq"),

    # Houellebecq variants - with book titles
    ("houellebecq serotonin", "houellebecq"),
    ("houllebecq serotonin", "houellebecq"),
    ("houllebeck serotonin", "houellebecq"),
    ("houellebecq underkastelse", "houellebecq"),
    ("houllebecq underkastelse", "houellebecq"),

    # Diacritical variants - ë (diaeresis)
    ("Bronte", "brontë"),           # Without diacritic
    ("Brontë", "brontë"),           # With diacritic
    ("Charlotte Bronte", "brontë"), # Full name without diacritic

    # Diacritical variants - ö (Swedish/German)
    ("Marcel Moring", "möring"),    # Without diacritic
    ("Marcel Möring", "möring"),    # With diacritic

    # Diacritical variants - é (French accent)
    ("Regine Deforges", "régine"),  # Without accent
    ("Régine Deforges", "régine"),  # With accent

    # Special character removal
    ("JK Rowling", "rowling"),      # Without periods
    ("J.K. Rowling", "rowling"),    # With periods
    ("J K Rowling", "rowling"),     # With spaces

    # Nordic character variants (ö→ø, ä→æ, but o≠ø, a≠æ)
    ("Jäger", "jæger"),             # German ä → Norwegian æ ✓
    ("Jæger", "jæger"),             # Norwegian æ preserved ✓
    ("Marcel Møring", "möring"),    # Norwegian ø → finds Swedish ö ✓
    ("Marcel Möring", "möring"),    # Swedish ö → finds Swedish ö (same after normalization)
    # Note: "Jager" does NOT find "Jæger" - they are different letters!
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
    """
    for i, hit in enumerate(hits):
        titles = hit["_source"].get("titles", [])
        authors = hit["_source"].get("authors", [])

        # Combine all text and check for expected substring
        full_text = " ".join(titles + authors).lower()

        if expected.lower() in full_text:
            return True, i + 1

    return False, None


def format_result(hits: list, max_len: int = 50) -> str:
    """Format the top result for display."""
    if not hits:
        return "No results"

    hit = hits[0]
    titles = ", ".join(hit["_source"].get("titles", []))
    authors = ", ".join(hit["_source"].get("authors", [])) or "Unknown"

    result = f"{titles[:30]} | {authors[:20]}"
    return result[:max_len]


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

    print("=" * 80)
    print("BOOK SEARCH TEST SUITE")
    print("=" * 80)
    print(f"Checking if expected result appears in positions 1-3")
    print()

    passed = 0
    failed = 0
    results = []

    for query, expected in TEST_CASES:
        hits = search(es, query)
        success, position = check_result(hits, expected)

        if success:
            passed += 1
            status = f"✓ PASS (#{position})"
        else:
            failed += 1
            status = "✗ FAIL"

        top_result = format_result(hits)
        results.append((query, expected, status, top_result))

    # Print results table
    print(f"{'Query':<30} {'Expected':<15} {'Status':<12} {'Top Result'}")
    print("-" * 80)

    for query, expected, status, top_result in results:
        print(f"{query:<30} {expected:<15} {status:<12} {top_result}")

    print("-" * 80)
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
        matching = [(q, e) for q, e in TEST_CASES if q == query]
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
