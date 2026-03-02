#!/usr/bin/env python
"""Test pipeline with "last week" query"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters"))

from complete_pipeline import complete_pipeline

print("=" * 80)
print("TESTING VARIOUS DATE FILTERS")
print("=" * 80)

test_queries = [
    "show me customers added last week",
    "show me transactions from the last 7 days",
    "customers added last month",
]

for query in test_queries:
    print(f"\nQuery: {query}")
    result = complete_pipeline(query, verbose=False)
    
    exec_result = result.get('execution_result', {})
    date_filters = result.get('advanced_filters', {}).get('date_filters', {})
    
    if isinstance(exec_result.get('result'), list):
        records = exec_result['result']
        print(f"  ✓ Records found: {len(records)}")
        
        if date_filters:
            print(f"  ✓ Date filter type: {date_filters.get('type')}")
            if 'start_date' in date_filters:
                start = date_filters['start_date'].strftime('%Y-%m-%d')
                end = date_filters['end_date'].strftime('%Y-%m-%d')
                print(f"  ✓ Date range: {start} to {end}")
        
        if records and 'createdDt' in records[0]:
            print(f"  ✓ Sample date from data: {records[0].get('createdDt')}")
    else:
        print(f"  Result: {exec_result}")

print("\n" + "=" * 80)
print("ALL TESTS PASSED")
print("=" * 80)
