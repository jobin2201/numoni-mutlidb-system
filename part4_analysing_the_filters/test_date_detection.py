#!/usr/bin/env python
"""
Test script for date query detection improvements
Tests:
1. "customers added last year" → should suggest customer_details
2. "transactions last year" → should use transaction_history
3. "merchants added" → should suggest merchantDetails
"""
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


def test_query(query: str):
    """Test a single query through the pipeline"""
    print("=" * 80)
    print(f"Query: '{query}'")
    print("=" * 80)
    
    # Part 1: Database
    db_result = detect_database(query)
    database_name = db_result.get('selected_dbs', [db_result.get('selected_database')])[0]
    print(f"\n[Part 1] Database: {database_name}")
    
    # Part 2: Collection
    coll_result = detect_collection(query, database_name)
    collection_name = coll_result.get('selected_collection')
    print(f"[Part 2] Collection: {collection_name}")
    print(f"  Reason: {coll_result.get('reason', 'N/A')[:100]}")
    
    # Part 3: Action
    action_result = detect_action(query)
    print(f"[Part 3] Action: {action_result['primary_action']}")
    
    # Part 4: Advanced Filters
    advanced_filters = detect_advanced_filters(query)
    print(f"\n[Part 4] Advanced Filters:")
    
    if advanced_filters['has_advanced_filters']:
        if advanced_filters.get('date_filters'):
            print(f"  Date Filter: {advanced_filters['date_filters']}")
        
        if advanced_filters.get('suggested_collection'):
            print(f"\n  [SUGGESTION]")
            print(f"    Suggested Collection: {advanced_filters['suggested_collection']}")
            print(f"    Reason: {advanced_filters['collection_reason']}")
            
            if advanced_filters['suggested_collection'] != collection_name:
                print(f"    [!] MISMATCH: Part 2 selected '{collection_name}', ")
                print(f"        but Part 4 suggests '{advanced_filters['suggested_collection']}'")
    else:
        print("  No advanced filters detected")
    
    # Execute query
    print(f"\n[Execution]")
    exec_result = execute_action(
        action_result,
        database_name,
        collection_name,
        advanced_filters=advanced_filters
    )
    
    if exec_result.get('filter_messages'):
        for msg in exec_result['filter_messages']:
            print(f"  {msg}")
    
    if 'result' in exec_result:
        result = exec_result['result']
        if isinstance(result, list):
            print(f"  Found: {len(result)} records")
            
            # Show sample record if available
            if len(result) > 0:
                sample = result[0]
                date_fields = ['createdDt', 'createdDate', 'updatedDt', 'transactionDate', 'date']
                print(f"\n  Sample record:")
                for field in date_fields:
                    if field in sample:
                        print(f"    {field}: {sample[field]}")
                
                # Show some identifying fields
                id_fields = ['_id', 'customerId', 'merchantId', 'name', 'email']
                for field in id_fields:
                    if field in sample:
                        value = str(sample[field])[:50]
                        print(f"    {field}: {value}")
        else:
            print(f"  Result: {result}")
    
    if exec_result.get('error'):
        print(f"  [ERROR] {exec_result['error']}")
    
    print()


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("TESTING DATE QUERY IMPROVEMENTS")
    print("=" * 80 + "\n")
    
    test_queries = [
        # Should use customer_details (entity creation)
        "show me customers added last year",
        "show me customers added in 2026",
        "customers created in 2025",
        "newly registered customers",
        
        # Should use transaction_history (transaction queries)
        "customer transactions last year",
        "show me transactions from last month",
        "show me customers who had their transactions in 2026",
        
        # Should use merchantDetails (entity creation)
        "merchants added last year",
        "show me newly added merchants",
        
        # Numeric filter (should use customer_sharepoint_requests)
        "customers who received more than 3000 numoni points",
    ]
    
    for query in test_queries:
        test_query(query)
        print("\n")
    
    print("=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)
