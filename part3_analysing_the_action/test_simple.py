#!/usr/bin/env python
"""Test the 3 specific queries - Silent mode"""
import sys
import os

# Add parent directories to path
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part1_analysing_the_db'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part2_analysing_the_collection'))

from db_keyword_router_fuzzy import detect_database
from collection_router import detect_collection
from action_detector import detect_action

test_queries = [
    "which customer received the highest numoni points",
    "what are the similar merchants names in deals",
    "show me any merchant starting with J"
]

print("=" * 80)
print("TESTING 3 QUERIES")
print("=" * 80)

for i, query in enumerate(test_queries, 1):
    print(f"\n--- Query {i}: {query}")
    
    # Part 1: Database detection
    db_result = detect_database(query)
    # Handle both old (selected_dbs) and new (selected_database) formats
    db_list = db_result.get('selected_dbs') or [db_result.get('selected_database')]
    db_list = [db for db in db_list if db]  # Remove None values
    
    if db_list:
        database_name = db_list[0] if len(db_list) == 1 else db_list[0]
        print(f"Database: {database_name}")
        
        # Part 2: Collection detection
        coll_result = detect_collection(query, database_name)
        print(f"Collection: {coll_result.get('selected_collection', 'N/A')}")
        print(f"Confidence: {coll_result.get('confidence', 0)}")
        print(f"Reason: {coll_result.get('reason', 'N/A')[:120]}")
        
        # Part 3: Action detection
        action_result = detect_action(query)
        print(f"Action: {action_result.get('primary_action', 'N/A')}")
        print(f"Filters: {action_result.get('filters', {})}")
    else:
        print("Database: N/A - NO DATABASE MATCHED")
    
    print()

print("=" * 80)
