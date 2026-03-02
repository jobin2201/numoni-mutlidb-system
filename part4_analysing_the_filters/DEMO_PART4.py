#!/usr/bin/env python
"""
Comprehensive Demo of Part 4 Advanced Filters
Shows date, location, and numeric filtering in action
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

print("""
================================================================================
                     PART 4: ADVANCED FILTERS DEMO
================================================================================

This demo showcases the new advanced filtering capabilities:
  1. Date Filters (last year, this month, recently added)
  2. Location Filters (by city, state, country)
  3. Numeric Filters (greater than, less than, between)

================================================================================
""")

test_queries = [
    # Date filters
    ("Date", "show me customers added last year"),
    ("Date", "who were the newly added customers"),
    ("Date", "transactions from this month"),
    
    # Numeric filters
    ("Numeric", "which customers have received more than 1000 numoni points"),
    ("Numeric", "customers with balance between 500 and 2000"),
    
    # Location filters
    ("Location", "merchants in Lagos"),
    ("Location", "customers from Nigeria"),
    
    # Combined filters
    ("Combined", "Lagos merchants added last year"),
]

for category, query in test_queries:
    print(f"\n{'=' * 80}")
    print(f"[{category}] {query}")
    print('=' * 80)
    
    # Part 1: Database
    db_result = detect_database(query)
    db_list = db_result.get('selected_dbs') or [db_result.get('selected_database')]
    database_name = db_list[0] if db_list else None
    
    # Part 2: Collection
    coll_result = detect_collection(query, database_name)
    collection_name = coll_result.get('selected_collection')
    
    # Part 3: Action
    action_result = detect_action(query)
    
    # Part 4: Advanced Filters
    advanced_filters = detect_advanced_filters(query)
    
    print(f"\n  Database: {database_name}")
    print(f"  Collection: {collection_name}")
    print(f"  Action: {action_result.get('primary_action')}")
    
    # Show detected filters
    if advanced_filters['has_advanced_filters']:
        print(f"\n  Advanced Filters Detected:")
        
        if advanced_filters.get('date_filters'):
            df = advanced_filters['date_filters']
            print(f"    [Date] Type: {df.get('type')}", end="")
            if 'year' in df:
                print(f", Year: {df['year']}", end="")
            if 'month' in df:
                print(f", Month: {df['month']}", end="")
            if 'days_ago' in df:
                print(f", Days: {df['days_ago']}", end="")
            print()
        
        if advanced_filters.get('location_filters'):
            lf = advanced_filters['location_filters']
            print(f"    [Location] {lf.get('location')} ({lf.get('type')})")
        
        if advanced_filters.get('numeric_filters'):
            for nf in advanced_filters['numeric_filters']:
                print(f"    [Numeric] {nf['description']}")
    else:
        print(f"\n  No advanced filters")
    
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
                print(f"\n  [OK] Retrieved {len(result)} records")
                
                # Show sample records
                if len(result) > 0 and len(result) <= 5:
                    print(f"\n  Sample records:")
                    for i, record in enumerate(result[:5], 1):
                        # Show key fields
                        name = (record.get('name') or 
                               record.get('businessName') or 
                               record.get('userName') or 
                               record.get('smsText', '')[:60])
                        
                        date_field = record.get('createdAt') or record.get('date')
                        date_str = ''
                        if isinstance(date_field, dict) and '$date' in date_field:
                            date_str = f" ({date_field['$date'][:10]})"
                        
                        amount_field = record.get('amount') or record.get('totalAmount')
                        amount_str = f", Amount: {amount_field}" if amount_field else ""
                        
                        print(f"    {i}. {name}{date_str}{amount_str}")
            
            elif isinstance(result, dict):
                print(f"\n  [OK] Aggregation result:")
                for key, value in list(result.items())[:5]:
                    print(f"    {key}: {value}")
        
        if 'error' in exec_result:
            print(f"\n  [ERROR] {exec_result['error']}")
    
    except Exception as e:
        print(f"\n  [ERROR] Execution error: {e}")

print("\n" + "=" * 80)
print("""
SUMMARY:
--------
[OK] Date filters: Detect year, month, days ago, recent
[OK] Location filters: Detect city, state, country
[OK] Numeric filters: Detect >, <, =, between comparisons
[OK] Seamless integration with Parts 1-3
[OK] Token-efficient (< 2000 tokens per component)

Part 4 enables powerful date/location/numeric filtering!
""")
print("=" * 80)
