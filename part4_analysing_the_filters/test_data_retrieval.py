"""Test the new data retrieval query mode"""
import sys
sys.path.insert(0, '.')

from field_filter import extract_field_names, extract_date_range

# Test queries that should trigger data retrieval mode
test_queries = [
    "get me all the data for which the users have purchased points on 16th of Feb. I need these following fields: Customer ID, Sender Name, Transaction Reference, total Amount",
    "get me data with columns: customerId, senderName, amount on 2025-02-16",
    "show me data these fields: merchant, amount, status from February 16",
]

print("=" * 80)
print("DATA RETRIEVAL QUERY TESTS")
print("=" * 80)

for i, query in enumerate(test_queries, 1):
    print(f"\n📝 Test {i}:")
    print(f"Query: {query[:70]}...")
    
    # Test field extraction
    fields = extract_field_names(query)
    print(f"✓ Fields extracted: {fields}")
    
    # Test date extraction
    dates = extract_date_range(query)
    if dates:
        print(f"✓ Date extracted: {dates[0].strftime('%B %d, %Y')}")
    else:
        print("✗ No date found")
    
    # Check if it would trigger data retrieval mode
    query_lower = query.lower()
    is_data_query = any(phrase in query_lower for phrase in [
        'get me all the data', 'get me data', 'show me data', 'get data',
        'get all data', 'retrieve data', 'fetch data'
    ]) and (any(phrase in query_lower for phrase in ['field', 'column', 'these', 'following', 'with']) or ',' in query_lower)
    
    print(f"{'✓' if is_data_query else '✗'} Would trigger DATA RETRIEVAL mode: {is_data_query}")

print("\n" + "=" * 80)
print("✅ All tests completed")
print("=" * 80)
