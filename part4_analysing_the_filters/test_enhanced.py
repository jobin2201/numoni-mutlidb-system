#!/usr/bin/env python
"""Test script for enhanced features"""
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
    "show me customers added 2 months back",
    "show me customers added 12 months back",
    "show me merchants added in 2025",
    "sum of all transaction amounts",
    "average balance of customers",
]

print("=" * 80)
print("TESTING ENHANCED FEATURES")
print("=" * 80)

for query in queries:
    print(f"\nQuery: '{query}'")
    print("-" * 80)
    
    db_result = detect_database(query)
    database_name = db_result.get('selected_dbs', [db_result.get('selected_database')])[0]
    
    coll_result = detect_collection(query, database_name)
    collection_name = coll_result.get('selected_collection')
    
    action_result = detect_action(query)
    advanced_filters = detect_advanced_filters(query)
    
    exec_result = execute_action(
        action_result,
        database_name,
        collection_name,
        advanced_filters=advanced_filters
    )
    
    print(f"Database: {database_name}")
    print(f"Collection: {collection_name}")
    print(f"Action: {action_result['primary_action']}")
    
    if advanced_filters.get('date_filters'):
        df = advanced_filters['date_filters']
        if 'months_ago' in df:
            print(f"Date: Last {df['months_ago']} months")
        elif 'year' in df:
            print(f"Date: Year {df['year']}")
    
    if exec_result.get('filter_messages'):
        for msg in exec_result['filter_messages']:
            print(f"  {msg}")
    
    result = exec_result.get('result')
    if isinstance(result, list):
        print(f"Records: {len(result)}")
        if len(result) > 0:
            print(f"Sample fields: {list(result[0].keys())}")
            if 'name' in result[0]:
                print(f"First name: {result[0]['name']}")
            elif 'businessName' in result[0]:
                print(f"First name: {result[0]['businessName']}")
    elif isinstance(result, dict):
        if 'sums' in result:
            print(f"Sums: {result['sums']}")
        elif 'averages' in result:
            print(f"Averages: {result['averages']}")
    
    print()

print("=" * 80)
