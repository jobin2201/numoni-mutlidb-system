#!/usr/bin/env python
"""
Complete 4-Part Pipeline Integration
Part 1: Database Detection
Part 2: Collection Detection  
Part 3: Action Detection
Part 4: Advanced Filters (Date, Location, Numeric)
"""
import sys
from pathlib import Path
from datetime import datetime

# Add all part directories to path
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


def complete_pipeline(user_query: str, verbose: bool = True) -> dict:
    """
    Execute complete 4-part pipeline
    
    Returns:
        Complete pipeline results with all 4 parts
    """
    
    if verbose:
        print("=" * 80)
        print("COMPLETE 4-PART PIPELINE")
        print("=" * 80)
        print(f"\nQuery: '{user_query}'")
    
    # PART 1: Database Detection
    if verbose:
        print("\n" + "-" * 80)
        print("PART 1: Database Detection")
        print("-" * 80)
    
    db_result = detect_database(user_query)
    db_list = db_result.get('selected_dbs') or [db_result.get('selected_database')]
    database_name = db_list[0] if db_list else None
    
    if verbose:
        print(f"Database: {database_name}")
        print(f"Reason: {db_result.get('reason', 'N/A')[:100]}")
    
    if not database_name:
        return {"error": "No database detected"}
    
    # PART 2: Collection Detection
    if verbose:
        print("\n" + "-" * 80)
        print("PART 2: Collection Detection")
        print("-" * 80)
    
    coll_result = detect_collection(user_query, database_name)
    collection_name = coll_result.get('selected_collection')
    
    if verbose:
        print(f"Collection: {collection_name}")
        print(f"Confidence: {coll_result.get('confidence', 0)}")
        print(f"Reason: {coll_result.get('reason', 'N/A')[:120]}")
    
    if not collection_name:
        return {"error": "No collection detected"}
    
    # PART 3: Action Detection
    if verbose:
        print("\n" + "-" * 80)
        print("PART 3: Action Detection")
        print("-" * 80)
    
    action_result = detect_action(user_query)
    
    if verbose:
        print(f"Action: {action_result.get('primary_action', 'N/A')}")
        if action_result.get('filters'):
            print(f"Basic Filters: {action_result['filters']}")
    
    # PART 4: Advanced Filter Detection
    if verbose:
        print("\n" + "-" * 80)
        print("PART 4: Advanced Filter Detection")
        print("-" * 80)
    
    advanced_filters = detect_advanced_filters(user_query)
    
    if verbose:
        if advanced_filters['has_advanced_filters']:
            if advanced_filters.get('date_filters'):
                df = advanced_filters['date_filters']
                print(f"Date Filter: {df.get('type', 'N/A')}")
                if 'year' in df:
                    print(f"  Year: {df['year']}")
                if 'month' in df:
                    print(f"  Month: {df['month']}")
                if 'days_ago' in df:
                    print(f"  Days ago: {df['days_ago']}")
            
            if advanced_filters.get('location_filters'):
                lf = advanced_filters['location_filters']
                print(f"Location Filter: {lf.get('location', 'N/A')} ({lf.get('type', 'any')})")
            
            if advanced_filters.get('numeric_filters'):
                for nf in advanced_filters['numeric_filters']:
                    print(f"Numeric Filter: {nf['description']}")
        else:
            print("No advanced filters detected")
    
    # Execute with advanced filters
    if verbose:
        print("\n" + "-" * 80)
        print("EXECUTION")
        print("-" * 80)
    
    exec_result = execute_action(
        action_result,
        database_name,
        collection_name,
        advanced_filters=advanced_filters
    )
    
    if verbose:
        if 'result' in exec_result:
            result = exec_result['result']
            if isinstance(result, list):
                print(f"Results: {len(result)} records")
            elif isinstance(result, dict):
                print(f"Result: {list(result.keys())}")
        
        if 'error' in exec_result:
            print(f"Error: {exec_result['error']}")
    
    # Return complete results
    return {
        'query': user_query,
        'database': database_name,
        'collection': collection_name,
        'collection_confidence': coll_result.get('confidence', 0),
        'action': action_result.get('primary_action'),
        'advanced_filters': advanced_filters,
        'execution_result': exec_result,
        'timestamp': datetime.now().isoformat()
    }


if __name__ == "__main__":
    print("=" * 80)
    print("4-PART PIPELINE TEST")
    print("=" * 80)
    
    test_queries = [
        "show me customers added last year",
        "who were the newly added customers",
        "which customers have received more than 1000 numoni points",
        "merchants in Lagos",
        "customers from this month"
    ]
    
    for query in test_queries:
        print("\n" + "=" * 80)
        result = complete_pipeline(query, verbose=True)
        print("\n")
