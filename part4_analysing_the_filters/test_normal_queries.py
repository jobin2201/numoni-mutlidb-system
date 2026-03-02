"""
Test that normal 'show me' queries are NOT affected
"""
import sys
sys.path.append('.')
from field_extractor import extract_requested_fields

# Queries that should NOT trigger field extraction
normal_queries = [
    "show me all customers",
    "show me merchants in Lagos",
    "show me transactions for Zenith Bank",
    "get me details of Abia State",
    "show me regions in Nigeria",
    "show payout notification",
    "list all merchants",
]

print("="*80)
print("TESTING NORMAL 'SHOW ME' QUERIES (should NOT extract fields)")
print("="*80)

all_pass = True
for query in normal_queries:
    result = extract_requested_fields(query)
    if result is None:
        print(f"✅ '{query}' → None (correct)")
    else:
        print(f"❌ '{query}' → {result} (SHOULD BE None!)")
        all_pass = False

print("="*80)
if all_pass:
    print("✅ ALL TESTS PASSED - Normal queries not affected")
else:
    print("❌ SOME TESTS FAILED - Normal queries are being affected!")
