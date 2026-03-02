#!/usr/bin/env python
"""Test both queries without Streamlit imports"""
import sys
import os

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part1_analysing_the_db'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part2_analysing_the_collection'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part3_analysing_the_action'))
sys.path.insert(0, os.path.dirname(__file__))

# Test with simple function calls
from advanced_filter_detector import detect_advanced_filters, extract_text_filters

# Test Query 1: TOP 2 regions in Nigeria
print("=" * 80)
print("TEST 1: PATTERN DETECTION FOR TOP 2 REGIONS")
print("=" * 80)
query1 = "show me top 2 regions in Nigeria"
filters1 = detect_advanced_filters(query1)
print(f"Query: {query1}")
print(f"Location filters: {filters1.get('location_filters')}")
print(f"Has advanced filters: {filters1.get('has_advanced_filters')}")
print()

# Test Query 2: Customers received points
print("=" * 80)
print("TEST 2: PATTERN DETECTION FOR RECEIVED POINTS")
print("=" * 80)
query2 = "which customers have received 1000 numoni points from someone else"
filters2 = extract_text_filters(query2.lower())
print(f"Query: {query2}")
print(f"Text filters: {filters2}")
print()

# Test the full pattern matching
print("=" * 80)
print("TEST 3: FULL FILTER DETECTION FOR RECEIVED POINTS")
print("=" * 80)
filters2_full = detect_advanced_filters(query2)
print(f"All filters: {filters2_full}")
