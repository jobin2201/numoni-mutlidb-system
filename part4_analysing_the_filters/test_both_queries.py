#!/usr/bin/env python
"""Test both queries: TOP regions and received points"""
import sys
import os

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part1_analysing_the_db'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part2_analysing_the_collection'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part3_analysing_the_action'))
sys.path.insert(0, os.path.dirname(__file__))

from app_4part_pipeline import complete_pipeline

# Test Query 1: TOP 2 regions in Nigeria
print("=" * 80)
print("TEST 1: TOP 2 REGIONS IN NIGERIA")
print("=" * 80)
query1 = "show me top 2 regions in Nigeria"
result1 = complete_pipeline(query1)
print(f"Query: {query1}")
print(f"Collection: {result1.get('collection_name', 'N/A')}")
print(f"Records: {len(result1.get('data', []))}")
if result1.get('data'):
    print(f"First record: {result1['data'][0]}")
    if len(result1.get('data', [])) > 1:
        print(f"Second record: {result1['data'][1]}")
print()

# Test Query 2: Customers received points
print("=" * 80)
print("TEST 2: CUSTOMERS WHO RECEIVED 1000 NUMONI POINTS FROM SOMEONE ELSE")
print("=" * 80)
query2 = "which customers have received 1000 numoni points from someone else"
result2 = complete_pipeline(query2)
print(f"Query: {query2}")
print(f"Collection: {result2.get('collection_name', 'N/A')}")
print(f"Records: {len(result2.get('data', []))}")
if result2.get('data'):
    # Show first few records with key fields
    for i, record in enumerate(result2.get('data', [])[:3]):
        print(f"Record {i+1}: {record}")
else:
    print("No records found")
