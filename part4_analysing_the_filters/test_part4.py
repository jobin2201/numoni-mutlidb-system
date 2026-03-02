#!/usr/bin/env python
"""Test Part 4 integration - No emoji output"""
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

test_queries = [
    "show me customers added last year",
    "which customers have received more than 1000 numoni points",
    "who were the newly added customers"
]

print("=" * 80)
print("TESTING 4-PART PIPELINE (Date & Numeric Filters)")
print("=" * 80)

for i, query in enumerate(test_queries, 1):
    print(f"\n--- Query {i}: {query}")
    
    # Part 1: Database
    db_result = detect_database(query)
    db_list = db_result.get('selected_dbs') or [db_result.get('selected_database')]
    database_name = db_list[0] if db_list else None
    print(f"  [DB] {database_name}")
    
    # Part 2: Collection
    coll_result = detect_collection(query, database_name)
    collection_name = coll_result.get('selected_collection')
    print(f"  [Collection] {collection_name}")
    
    # Part 3: Action
    action_result = detect_action(query)
    print(f"  [Action] {action_result.get('primary_action')}")
    
    # Part 4: Advanced Filters
    advanced_filters = detect_advanced_filters(query)
    if advanced_filters['has_advanced_filters']:
        if advanced_filters.get('date_filters'):
            df = advanced_filters['date_filters']
            print(f"  [Date Filter] Type: {df.get('type')}, Year: {df.get('year', 'N/A')}")
        
        if advanced_filters.get('numeric_filters'):
            for nf in advanced_filters['numeric_filters']:
                print(f"  [Numeric Filter] {nf['description']}")
    
    # Execute
    try:
        exec_result = execute_action(
            action_result,
            database_name,
            collection_name,
            advanced_filters=advanced_filters
        )
        
        if 'result' in exec_result:
            result = exec_result['result']
            if isinstance(result, list):
                print(f"  [Result] {len(result)} records")
            elif isinstance(result, dict):
                print(f"  [Result] {result}")
        
        if 'error' in exec_result:
            print(f"  [Error] {exec_result['error']}")
    except Exception as e:
        print(f"  [Error] {e}")

print("\n" + "=" * 80)
