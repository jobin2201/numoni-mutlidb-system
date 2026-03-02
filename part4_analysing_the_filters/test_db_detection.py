#!/usr/bin/env python
"""Test database detection"""
import sys
import os

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part1_analysing_the_db'))

from db_keyword_router_fuzzy import detect_database

query = "which customers have received 1000 numoni points from someone else"
result = detect_database(query)
print(f"Query: {query}")
print(f"Result: {result}")
print(f"Result keys: {result.keys() if isinstance(result, dict) else type(result)}")
