#!/usr/bin/env python
"""Test collection routing for received points query"""
import sys
import os

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part1_analysing_the_db'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part2_analysing_the_collection'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part3_analysing_the_action'))
sys.path.insert(0, os.path.dirname(__file__))

from collection_router import detect_collection

# Test Query 2: Customers received points
print("=" * 80)
print("TEST: COLLECTION ROUTING FOR RECEIVED POINTS")
print("=" * 80)
query2 = "which customers have received 1000 numoni points from someone else"
result = detect_collection(query2, "numoni_customer")  # Using the CUSTOMER database (correct!)
print(f"Query: {query2}")
print(f"Collection: {result['selected_collection']}")
print(f"Confidence: {result['confidence']:.2f}")
print(f"Reason: {result['reason']}")
