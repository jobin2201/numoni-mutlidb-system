#!/usr/bin/env python
"""
Test that customer and merchant collections still work correctly
Verify we didn't break existing functionality
"""

from collection_router import detect_collection

print("=" * 80)
print("✅ CUSTOMER & MERCHANT COLLECTION DETECTION TEST")
print("=" * 80)

# Test customer queries
customer_tests = [
    {
        "query": "Show me customer details",
        "expected": "customerDetails",
        "description": "Customer profile"
    },
    {
        "query": "Show me wallet balance",
        "expected": "customer_wallet_ledger",
        "description": "Wallet transactions"
    },
    {
        "query": "Show me customer locations",
        "expected": "customerlocation",
        "description": "Customer locations"
    },
    {
        "query": "Show me favorite deals",
        "expected": "favourite_deal",
        "description": "Favorite deals"
    },
]

# Test merchant queries
merchant_tests = [
    {
        "query": "Show me merchant details",
        "expected": "merchantDetails",
        "description": "Merchant profile"
    },
    {
        "query": "Show me merchant bank information",
        "expected": "bankInformation",
        "description": "Bank account info"
    },
    {
        "query": "Show me deals and offers",
        "expected": "deals",
        "description": "Merchant deals"
    },
    {
        "query": "Show me merchant locations",
        "expected": "merchantlocation",
        "description": "Merchant locations"
    },
]

customer_passed = 0
customer_failed = 0
merchant_passed = 0
merchant_failed = 0

print("\n👥 CUSTOMER DATABASE TESTS:\n")
for test in customer_tests:
    result = detect_collection(test["query"], "numoni_customer")
    selected = result['selected_collection']
    expected = test["expected"]
    is_correct = selected == expected
    
    status = "✅" if is_correct else "❌"
    if is_correct:
        customer_passed += 1
    else:
        customer_failed += 1
    
    print(f"{status} {test['description']}")
    print(f"   Expected: {expected}, Got: {selected}")
    print(f"   Confidence: {result.get('confidence', 0):.2f}")
    print()

print("\n🏪 MERCHANT DATABASE TESTS:\n")
for test in merchant_tests:
    result = detect_collection(test["query"], "numoni_merchant")
    selected = result['selected_collection']
    expected = test["expected"]
    is_correct = selected == expected
    
    status = "✅" if is_correct else "❌"
    if is_correct:
        merchant_passed += 1
    else:
        merchant_failed += 1
    
    print(f"{status} {test['description']}")
    print(f"   Expected: {expected}, Got: {selected}")
    print(f"   Confidence: {result.get('confidence', 0):.2f}")
    print()

print("=" * 80)
print("📊 RESULTS:")
print(f"   Customer: {customer_passed} passed, {customer_failed} failed")
print(f"   Merchant: {merchant_passed} passed, {merchant_failed} failed")
total_passed = customer_passed + merchant_passed
total_failed = customer_failed + merchant_failed
print(f"   TOTAL: {total_passed} passed, {total_failed} failed")
print("=" * 80)

if total_failed == 0:
    print("\n✅ SUCCESS! All customer and merchant collections still work correctly.")
    print("   No regressions detected - existing functionality preserved.")
else:
    print(f"\n⚠️ {total_failed} test(s) failed - regression detected!")

