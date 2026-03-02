#!/usr/bin/env python
"""Test the merchant/customer router"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'part1_analysing_the_db'))

from db_keyword_router_fuzzy import detect_database, MERCHANT_NAMES, CUSTOMER_NAMES

# Check data loaded
print("=" * 70)
print("🔍 Router Data Analysis")
print("=" * 70)
print(f"Total merchant names loaded: {len(MERCHANT_NAMES)}")
print(f"Total customer names loaded: {len(CUSTOMER_NAMES)}")

print("\n--- First 10 Merchant Names ---")
for i, name in enumerate(list(MERCHANT_NAMES.keys())[:10]):
    print(f"  {i+1}. '{name}' (len={len(name)})")

print("\n--- First 10 Customer Names (length >= 4) ---")
for i, name in enumerate(list(CUSTOMER_NAMES.keys())[:10]):
    print(f"  {i+1}. '{name}' (len={len(name)})")

# Check for MONI/POINT in data
print("\n--- Searching for 'moni' or 'point' in databases ---")
merchant_moni = [n for n in MERCHANT_NAMES.keys() if 'moni' in n]
merchant_point = [n for n in MERCHANT_NAMES.keys() if 'point' in n]
print(f"Merchant names with 'moni': {merchant_moni if merchant_moni else 'NONE'}")
print(f"Merchant names with 'point': {merchant_point if merchant_point else 'NONE'}")

# Test cases
print("\n" + "=" * 70)
print("🧪 Test Cases")
print("=" * 70)

test_queries = [
    "show me the details of MONIEPOINT MICROFINANCE",
    "MONIEPOINT",
    "moniepoint microfinance",
    "show merchant details",
    "customer name is John",
    "show me customers",
    "bank details",
    "pos terminal",
]

for query in test_queries:
    result = detect_database(query)
    print(f"\n❓ Query: '{query}'")
    print(f"✅ DB: {result['selected_dbs']}")
    print(f"📌 Reason: {result['reason']}")
