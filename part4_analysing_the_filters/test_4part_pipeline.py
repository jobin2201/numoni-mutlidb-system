#!/usr/bin/env python
"""Test full 4-part pipeline for both queries"""
import sys
import os

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part1_analysing_the_db'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part2_analysing_the_collection'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part3_analysing_the_action'))
sys.path.insert(0, os.path.dirname(__file__))

# Import the complete functions
from db_keyword_router_fuzzy import detect_database
from collection_router import detect_collection
from action_detector import detect_action
from advanced_filter_detector import detect_advanced_filters
from advanced_filter_executor import apply_advanced_filters

print("=" * 80)
print("COMPLETE 4-PART PIPELINE - BOTH QUERIES")
print("=" * 80)

# Test 1: Top 2 regions in Nigeria
print("\n1. TOP 2 REGIONS IN NIGERIA")
print("-" * 80)
query1 = "show me top 2 regions in Nigeria"

# Part 1: Database
db_result = detect_database(query1)
database = db_result['selected_dbs'][0]
print(f"PART 1: Database = {database}")

# Part 2: Collection
coll_result = detect_collection(query1, database)
collection = coll_result['selected_collection']
print(f"PART 2: Collection = {collection}")

# Part 3: Action
action_result = detect_action(query1)
action = action_result['primary_action']
limit = action_result.get('limit', 10)
print(f"PART 3: Action = {action} (limit={limit})")

# Part 4: Filters
filters = detect_advanced_filters(query1)
print(f"PART 4: Filters = {filters['location_filters']}")

print("\n" + "=" * 80)
print("2. CUSTOMERS WHO RECEIVED 1000 NUMONI POINTS")
print("-" * 80)
query2 = "which customers have received 1000 numoni points from someone else"

# Part 1: Database
db_result = detect_database(query2)
database = db_result['selected_dbs'][0]
print(f"PART 1: Database = {database}")

# Part 2: Collection
coll_result = detect_collection(query2, database)
collection = coll_result['selected_collection']
print(f"PART 2: Collection = {collection}")

# Part 3: Action
action_result = detect_action(query2)
action = action_result['primary_action']
print(f"PART 3: Action = {action}")

# Part 4: Filters
filters = detect_advanced_filters(query2)
print(f"PART 4: Text Filters = {filters['text_filters']}")
