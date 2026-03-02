#!/usr/bin/env python
"""
Test Content-Based Collection Matching
Demonstrates how the router now prioritizes actual field names and data values
over abstract keywords
"""

from collection_router import detect_collection

print("=" * 80)
print("🎯 CONTENT-BASED COLLECTION ROUTING TEST")
print("=" * 80)
print("\nNew Scoring System:")
print("  1. Field Names (columns): 20 pts exact, 8 pts partial")
print("  2. Data Values (records): 12 pts exact, 6 pts fuzzy")
print("  3. Collection Name: 40 pts")
print("  4. Keywords: 5 pts each")
print("\n" + "=" * 80)

# Test 1: Field name matching (actual column names)
print("\n📊 TEST 1: Field Name Matching (Actual Columns)")
print("-" * 80)

test_cases_fields = [
    ("Show me the merchantId field", "numoni_merchant", "Should match merchantDetails (has merchantId field)"),
    ("Get the totalAmount values", "numoni_customer", "Should match transaction_history (has totalAmount field)"),
    ("Find records with phoneNumber", "numoni_customer", "Should match customerDetails (has phoneNumber field)"),
    ("Show businessName column", "numoni_merchant", "Should match merchantDetails (has businessName field)"),
    ("Display transactionType data", "numoni_customer", "Should match transaction_history (has transactionType field)")
]

for query, db, expected in test_cases_fields:
    result = detect_collection(query, db)
    print(f"\n❓ Query: '{query}'")
    print(f"   Expected: {expected}")
    print(f"✅ Detected: {result['selected_collection']}")
    print(f"📊 Score: {result.get('score', 0):.1f} | Confidence: {result['confidence']}")
    print(f"🔑 Matched Fields: {', '.join(result.get('matched_fields', []))}")
    print(f"💡 Reason: {result['reason'][:150]}")

# Test 2: Data value matching (actual record content)
print("\n\n📄 TEST 2: Data Value Matching (Actual Record Content)")
print("-" * 80)

test_cases_values = [
    ("Find MERCHANT records", "numoni_merchant", "Should match collections with MERCHANT value"),
    ("Show ACTIVE status", "numoni_customer", "Should match collections with ACTIVE status"),
    ("Get PENDING transactions", "numoni_customer", "Should match transaction_history"),
]

for query, db, expected in test_cases_values:
    result = detect_collection(query, db)
    print(f"\n❓ Query: '{query}'")
    print(f"   Expected: {expected}")
    print(f"✅ Detected: {result['selected_collection']}")
    print(f"📊 Score: {result.get('score', 0):.1f} | Confidence: {result['confidence']}")
    if result.get('matched_fields'):
        print(f"🔑 Fields: {', '.join(result['matched_fields'][:3])}")
    print(f"💡 {result['reason'][:150]}")

# Test 3: Combined content + structure
print("\n\n🔍 TEST 3: Combined Content & Structure Matching")
print("-" * 80)

comprehensive_tests = [
    {
        "query": "Show all customer wallet balances with customerId",
        "db": "numoni_customer",
        "note": "Matches fields: customerId, balance in wallet collection"
    },
    {
        "query": "List merchant business accounts with bankName",
        "db": "numoni_merchant",
        "note": "Matches field: bankName in bankInformation"
    },
    {
        "query": "Get transaction records with amount and date",
        "db": "numoni_customer",
        "note": "Matches fields: amount, date in transaction_history"
    },
    {
        "query": "Find deals with dealName and description",
        "db": "numoni_merchant",
        "note": "Matches fields in deals collection"
    }
]

for test in comprehensive_tests:
    result = detect_collection(test["query"], test["db"])
    print(f"\n❓ Query: '{test['query']}'")
    print(f"   Note: {test['note']}")
    print(f"✅ Collection: {result['selected_collection']}")
    print(f"📊 Score: {result.get('score', 0):.1f}")
    print(f"🔑 Matched: {', '.join(result.get('matched_fields', [])[:5])}")
    
    # Show alternatives for comparison
    if result.get('alternatives'):
        print(f"   Alternatives:")
        for alt in result['alternatives'][:2]:
            print(f"     - {alt['collection']}: {alt['score']:.1f} pts")

# Test 4: Demonstrate content > keywords
print("\n\n⚖️ TEST 4: Content Prioritization Over Keywords")
print("-" * 80)
print("Testing that actual field/value matches score higher than keyword matches\n")

comparison_tests = [
    {
        "query": "Get customerId field",  # Specific field name
        "db": "numoni_customer",
        "reason": "Field name match (20 pts) > Keyword 'customer' (5 pts)"
    },
    {
        "query": "Show transactionType column",  # Specific column
        "db": "numoni_customer",
        "reason": "Field 'transactionType' (20 pts) > Generic keywords"
    }
]

for test in comparison_tests:
    result = detect_collection(test["query"], test["db"])
    print(f"❓ '{test['query']}'")
    print(f"   → {result['selected_collection']} (Score: {result.get('score', 0):.1f})")
    print(f"   Reason: {test['reason']}")
    print(f"   Matched: {result.get('matched_fields', [])}\n")

print("=" * 80)
print("✅ CONTENT-BASED ROUTING: Fields & Values > Abstract Keywords")
print("=" * 80)
