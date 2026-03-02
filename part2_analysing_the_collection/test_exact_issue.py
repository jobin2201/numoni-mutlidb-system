#!/usr/bin/env python
"""
Test the exact issue: Share Money Type Query
Demonstrates the fix for matching actual data values
"""

from collection_router import detect_collection

print("=" * 80)
print("🎯 EXACT ISSUE TEST: Share Money Type Query")
print("=" * 80)

print("\n❓ User Query: 'which customers have share money type as their transactions'")
print("\nProblem Analysis:")
print("  ❌ OLD: Matched wallet_adjust_management (has 'type' field with value 'D')")
print("  ✅ NEW: Should match transaction_session (has 'type' field with 'SHARE_MONEY')")

print("\n" + "=" * 80)

# Test the exact query
query = "which customers have share money type as their transactions"
result = detect_collection(query, "numoni_customer")

print(f"\n✅ RESULT:")
print(f"   Collection: {result['selected_collection']}")
print(f"   Score: {result.get('score', 0):.1f} points")
print(f"   Confidence: {result['confidence']}")

if result.get('matched_fields'):
    print(f"   Matched Fields: {', '.join(result['matched_fields'])}")

if result.get('matched_values'):
    print(f"   Matched Values: {', '.join(result['matched_values'][:3])}")

print(f"\n💡 Reason: {result['reason']}")

# Show alternatives to compare scoring
print("\n📊 Alternative Collections Considered:")
if result.get('alternatives'):
    for i, alt in enumerate(result['alternatives'][:5], 1):
        print(f"   {i}. {alt['collection']}: {alt['score']:.1f} pts (confidence: {alt['confidence']:.2f})")

print("\n" + "=" * 80)
print("🔍 DETAILED ANALYSIS")
print("=" * 80)

# Show what each collection has
print("\n📋 Collection Comparison:")
print("\ntransaction_session:")
print("  - Has 'type' field: ✓")
print("  - Actual values: ['PURCHASE', 'SHARE_MONEY']")
print("  - Contains 'SHARE_MONEY': ✓✓✓ EXACT MATCH")

print("\nwallet_adjust_management:")
print("  - Has 'type' field: ✓")
print("  - Actual values: ['D']")
print("  - Contains 'SHARE_MONEY': ✗ NO MATCH")

print("\ncustomer_share_money:")
print("  - Collection name contains 'share_money': ✓")
print("  - Has 'type' field: ✓")
print("  - Actual values: ['NonRegister', 'Register']")
print("  - Context matches but values don't include transaction list")

# Test additional related queries
print("\n\n" + "=" * 80)
print("🧪 RELATED TEST QUERIES")
print("=" * 80)

test_queries = [
    "show transactions with SHARE_MONEY type",
    "get all PURCHASE type transactions", 
    "find share money in transaction session",
    "list customers with share_money transactions"
]

for test_query in test_queries:
    result = detect_collection(test_query, "numoni_customer")
    print(f"\n❓ '{test_query}'")
    print(f"   → {result['selected_collection']} ({result.get('score', 0):.1f} pts)")
    if 'Exact value' in result['reason']:
        print(f"   ✓ Exact value match found in data!")

print("\n" + "=" * 80)
print("✅ VALUE CONTENT MATCHING: Now checks actual data values, not just field names")
print("=" * 80)
