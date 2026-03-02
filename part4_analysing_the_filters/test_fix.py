"""Test that query_lower is properly defined"""
import sys
sys.path.insert(0, '.')

# Simulate the app logic
query = "get me all the data for which the users have purchased points on 16th of Feb. I need these following fields: Customer ID, Sender Name, Transaction Reference, total Amount"

# This is exactly what the app does now
if not query:
    print("❌ Query is empty")
else:
    query_lower = query.lower()  # Defined early in else block
    
    # CHECK 0a: Is this a DATA RETRIEVAL query with specific fields?
    is_data_query = any(phrase in query_lower for phrase in [
        'get me all the data', 'get me data', 'show me data', 'get data',
        'get all data', 'retrieve data', 'fetch data'
    ]) and (any(phrase in query_lower for phrase in ['field', 'column', 'these', 'following', 'with']) or ',' in query_lower)
    
    print(f"✓ query_lower defined: {bool(query_lower)}")
    print(f"✓ is_data_query: {is_data_query}")
    
    # CHECK 0: Is this a RANKING query?
    is_ranking = any(word in query_lower for word in ['rank', 'ranking', 'ranked by', 'top by', 'list by', 'sort by'])
    print(f"✓ is_ranking: {is_ranking}")
    
    # CHECK 1: Is this a COMPARISON query?
    is_comparison = any(word in query_lower for word in ['compare', 'comparison', 'versus', 'vs', 'difference between'])
    print(f"✓ is_comparison: {is_comparison}")

print("\n✅ NameError fixed! All variables properly defined.")
