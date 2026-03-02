#!/usr/bin/env python
"""Quick test for user's specific queries"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters"))

from db_keyword_router_fuzzy import detect_database
from collection_router import detect_collection
from action_detector import detect_action
from action_executor import execute_action
from advanced_filter_detector import detect_advanced_filters


queries = [
    "show me customers added in 2026",
    "show me customers who had their transactions in 2026",
]

print("=" * 80)
print("TESTING USER'S SPECIFIC QUERIES")
print("=" * 80)

for query in queries:
    print(f"\nQuery: '{query}'")
    print("-" * 80)
    
    # Detect database
    db_result = detect_database(query)
    database_name = db_result.get('selected_dbs', [db_result.get('selected_database')])[0]
    
    # Detect collection
    coll_result = detect_collection(query, database_name)
    collection_name = coll_result.get('selected_collection')
    
    # Detect action
    action_result = detect_action(query)
    
    # Detect advanced filters
    advanced_filters = detect_advanced_filters(query)
    
    # Execute
    exec_result = execute_action(
        action_result,
        database_name,
        collection_name,
        advanced_filters=advanced_filters
    )
    
    print(f"Database: {database_name}")
    print(f"Collection: {collection_name}")
    
    if exec_result.get('filter_messages'):
        for msg in exec_result['filter_messages']:
            print(f"  {msg}")
    
    if 'result' in exec_result:
        result = exec_result['result']
        if isinstance(result, list):
            print(f"Found: {len(result)} records")
            if len(result) > 0:
                sample = result[0]
                if 'createdDt' in sample:
                    print(f"  Sample createdDt: {sample['createdDt']}")
                if 'transactionDate' in sample:
                    print(f"  Sample transactionDate: {sample['transactionDate']}")
    
    print()

print("=" * 80)
print("RESULT SUMMARY:")
print("=" * 80)
print("Query 1: 'customers added in 2026'")
print("  Should use: customerDetails with createdDt field")
print("  Shows: When customers registered on nuMoni platform")
print()
print("Query 2: 'customers who had transactions in 2026'")
print("  Should use: transaction_history with transactionDate field")
print("  Shows: When customers made wallet top-ups/payments")
print("=" * 80)
