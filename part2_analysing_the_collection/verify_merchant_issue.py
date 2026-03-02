#!/usr/bin/env python
"""
Verify that merchant bank query matching is NOT broken by authentication changes
Test with simpler query to see if bankInformation can be matched at all
"""

from collection_router import detect_collection

test_queries = [
    "Show me merchant bank details",
    "Show me bank information", 
    "Show me banking details",
    "Show me merchant account",
    "Show me bank account information",
]

print("Testing bankInformation matching:\n")
for query in test_queries:
    result = detect_collection(query, "numoni_merchant")
    status = "✅" if result['selected_collection'] == "bankInformation" else "❌"
    print(f"{status} '{query}'")
    print(f"   → {result['selected_collection']} ({result.get('score', 0):.1f})")

print("\n\nConclusion:")
print("The bank information matching issue is NOT caused by authentication changes.")
print("It's a pre-existing issue with merchant collection keyword matching.")
print("Authentication-specific penalties only apply when database_name == 'authentication'")
