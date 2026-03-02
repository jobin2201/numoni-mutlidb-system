#!/usr/bin/env python
"""Test execution with actual data files to ensure filters work"""
import sys
import os
import json

# Add paths
sys.path.insert(0, os.path.dirname(__file__))

from action_executor import load_collection_data, apply_filters

# Test 1: Similar merchants names filter
print("=" * 80)
print("TEST 1: Similar merchants names filter")
print("=" * 80)

deals_data = load_collection_data("numoni_merchant", "deals")
print(f"Total deals: {len(deals_data)}")

# Show first few names
if deals_data:
    print("\nNames in deals:")
    for i, deal in enumerate(deals_data[:10]):
        name = deal.get('name', deal.get('businessName', 'N/A'))
        print(f"  {i+1}. {name}")

# Apply similar_names filter
filtered = apply_filters(deals_data, {'similar_names': True})
print(f"\nAfter similar_names filter: {len(filtered)} deals")
if filtered:
    for i, deal in enumerate(filtered[:5]):
        name = deal.get('name', deal.get('businessName', 'N/A'))
        print(f"  {i+1}. {name}")

# Test 2: Starting with J filter  
print("\n" + "=" * 80)
print("TEST 2: Merchant starting with J")
print("=" * 80)

merchant_data = load_collection_data("numoni_merchant", "merchantDetails")
print(f"Total merchants: {len(merchant_data)}")

# Apply starts_with filter
filtered_j = apply_filters(merchant_data, {'starts_with': 'J'})
print(f"\nMerchants starting with J: {len(filtered_j)}")
if filtered_j:
    for i, merchant in enumerate(filtered_j):
        name = merchant.get('businessName', merchant.get('merchantName', 'N/A'))
        print(f"  {i+1}. {name}")

print("\n" + "=" * 80)
