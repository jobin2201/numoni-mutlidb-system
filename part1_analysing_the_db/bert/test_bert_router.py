"""
Test script for BERT-based database router
Compares BERT vs Fuzzy matching performance
"""

print("=" * 70)
print("🤖 Testing BERT Database Router")
print("=" * 70)

# Test queries that should show BERT improvements
test_queries = [
    # Semantic understanding tests
    ("show me digital wallet transactions", "numoni_customer", "Semantic: digital wallet = customer wallet"),
    ("business payment settlements", "numoni_merchant", "Semantic: business = merchant"),
    ("client money transfers", "numoni_customer", "Semantic: client = customer"),
    ("shop locations in Lagos", "numoni_merchant", "Semantic: shop = merchant/business"),
    
    # Intent recognition tests
    ("where are vendors located", "numoni_merchant", "Intent: vendors = merchants"),
    ("buyer transaction history", "numoni_customer", "Intent: buyer = customer"),
    ("store revenue data", "numoni_merchant", "Intent: store revenue = merchant"),
    
    # Context-aware tests
    ("account balance for clients", "numoni_customer", "Context: client accounts"),
    ("business bank account details", "numoni_merchant", "Context: business banking"),
    ("user login sessions", "authentication", "Context: authentication system"),
    
    # Multi-word understanding
    ("merchant payment terminal settlements", "numoni_merchant", "Multi-word: merchant context"),
    ("customer wallet topup history", "numoni_customer", "Multi-word: customer wallet"),
    
    # Original working queries (should still work)
    ("show me customers", "numoni_customer", "Direct keyword"),
    ("merchant details", "numoni_merchant", "Direct keyword"),
    ("login activities", "authentication", "Direct keyword"),
]

print("\n📊 Running Test Queries...\n")

try:
    from db_keyword_router_bert import detect_database
    
    passed = 0
    failed = 0
    
    for query, expected_db, test_type in test_queries:
        result = detect_database(query)
        detected_db = result['selected_dbs'][0] if result['selected_dbs'] else 'unknown'
        confidence = result.get('confidence', 0)
        
        status = "✅ PASS" if detected_db == expected_db else "❌ FAIL"
        if detected_db == expected_db:
            passed += 1
        else:
            failed += 1
        
        print(f"{status} | Query: \"{query}\"")
        print(f"      Expected: {expected_db} | Got: {detected_db} | Confidence: {confidence:.1%}")
        print(f"      Test Type: {test_type}")
        print(f"      Reason: {result['reason']}")
        print()
    
    print("=" * 70)
    print(f"📈 Results: {passed}/{len(test_queries)} passed ({passed/len(test_queries)*100:.1f}%)")
    print("=" * 70)
    
    if failed > 0:
        print(f"\n⚠️  {failed} tests failed. Review the results above.")
    else:
        print("\n🎉 All tests passed! BERT router is working correctly.")

except ImportError as e:
    print(f"\n❌ Error: Could not import BERT router")
    print(f"   {e}")
    print("\n💡 Make sure to install requirements:")
    print("   pip install sentence-transformers")
    print("   pip install torch")

except Exception as e:
    print(f"\n❌ Error running tests: {e}")
    import traceback
    traceback.print_exc()
