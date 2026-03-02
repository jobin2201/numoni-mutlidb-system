#!/usr/bin/env python
"""
Test Value + Context Matching
Demonstrates how router now matches collections based on:
1. Collection name semantic match with requested values
2. Field structure (correct column names)
3. Actual data values (record contents)
"""

from collection_router import detect_collection

print("=" * 80)
print("🎯 VALUE + CONTEXT MATCHING TEST")
print("=" * 80)
print("\nProblem: User asks for 'Share_money' type")
print("  Old behavior: Matches any collection with 'type' field")
print("  New behavior: Matches 'customer_share_money' collection specifically")
print("\nScoring Enhancement:")
print("  • Collection name matches requested value: +60 pts (HIGHEST)")
print("  • Exact value in data: +30 pts")
print("  • Value context + field match: +20 pts bonus")
print("=" * 80)

# Test Case 1: The exact problem mentioned
print("\n" + "=" * 80)
print("TEST 1: Share Money Query (The Original Problem)")
print("=" * 80)

share_money_queries = [
    "Show me types as Share_money",
    "Get Share_money transactions",
    "List all share_money records",
    "Find customer share money with type",
    "Display share money status"
]

print("\n🔍 Testing Share Money Queries:")
print("-" * 80)

for query in share_money_queries:
    result = detect_collection(query, "numoni_customer")
    print(f"\n❓ Query: '{query}'")
    print(f"✅ Collection: {result['selected_collection']}")
    print(f"📊 Score: {result.get('score', 0):.1f} | Confidence: {result['confidence']}")
    
    if result.get('matched_fields'):
        print(f"🔑 Matched Fields: {', '.join(result['matched_fields'])}")
    
    print(f"💡 Reason: {result['reason'][:120]}")
    
    # Show alternatives to see what was considered
    if result.get('alternatives'):
        print(f"   📋 Alternatives considered:")
        for alt in result['alternatives'][:2]:
            print(f"      - {alt['collection']}: {alt['score']:.1f} pts")

# Test Case 2: Other similar value context queries
print("\n\n" + "=" * 80)
print("TEST 2: Other Value Context Queries")
print("=" * 80)

value_context_tests = [
    {
        "query": "Show transactions with type PURCHASE",
        "db": "numoni_customer",
        "expected": "transaction_session (has type='PURCHASE' in sample)",
        "note": "Should match because exact value exists in data"
    },
    {
        "query": "Get load_money transactions",
        "db": "numoni_customer",
        "expected": "customer_load_money",
        "note": "Collection name matches the value term"
    },
    {
        "query": "Find SUCCESSFUL status in load money",
        "db": "numoni_customer",
        "expected": "customer_load_money",
        "note": "Collection + value both match"
    },
    {
        "query": "List deals with type MERCHANT",
        "db": "numoni_merchant",
        "expected": "deals (has createdBy='MERCHANT')",
        "note": "Value exists in sample data"
    }
]

print("\n🔍 Testing Value + Context Matching:")
print("-" * 80)

for test in value_context_tests:
    result = detect_collection(test["query"], test["db"])
    print(f"\n❓ Query: '{test['query']}'")
    print(f"   Expected: {test['expected']}")
    print(f"   Note: {test['note']}")
    print(f"✅ Detected: {result['selected_collection']}")
    print(f"📊 Score: {result.get('score', 0):.1f}")
    
    if result.get('matched_fields'):
        print(f"🔑 Fields: {', '.join(result['matched_fields'][:3])}")
    
    if result.get('matched_values'):
        print(f"📄 Values: {', '.join(result['matched_values'][:2])}")

# Test Case 3: Demonstrate prioritization
print("\n\n" + "=" * 80)
print("TEST 3: Scoring Comparison - Collection Name vs Field Name")
print("=" * 80)

comparison = [
    {
        "query": "Get share money type",
        "db": "numoni_customer",
        "note": "'share_money' in collection name should score higher than just 'type' field"
    },
    {
        "query": "Show transaction type",
        "db": "numoni_customer",
        "note": "Multiple collections have 'type' field, but transaction_session most relevant"
    }
]

for test in comparison:
    result = detect_collection(test["query"], test["db"])
    print(f"\n❓ '{test['query']}'")
    print(f"   {test['note']}")
    print(f"✅ Selected: {result['selected_collection']} ({result.get('score', 0):.1f} pts)")
    
    # Show top 3 alternatives with scores
    print(f"   📊 Ranking:")
    print(f"      1. {result['selected_collection']}: {result.get('score', 0):.1f} pts")
    if result.get('alternatives'):
        for i, alt in enumerate(result['alternatives'][:2], 2):
            print(f"      {i}. {alt['collection']}: {alt['score']:.1f} pts")

print("\n" + "=" * 80)
print("✅ VALUE CONTEXT MATCHING: Collection Name + Data Values > Field Names Only")
print("=" * 80)
print("\nKey Improvements:")
print("  ✓ Collection name matching requested values: +60 pts")
print("  ✓ Exact data value found: +30 pts")  
print("  ✓ Combined value context + field match: +20 pts bonus")
print("  ✓ Semantic similarity for fuzzy matches")
print("=" * 80)
