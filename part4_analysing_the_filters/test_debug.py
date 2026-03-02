#!/usr/bin/env python
"""Test merchant filtering and regions query"""
import sys
from pathlib import Path
from difflib import SequenceMatcher

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters"))

from complete_pipeline import complete_pipeline

print("=" * 80)
print("TESTING MERCHANT FILTERING & REGIONS")
print("=" * 80)

# Test 1: Regions query
print("\n1. Testing REGIONS QUERY")
print("-" * 40)
query = "show me top 2 regions in Nigeria"
print(f"Query: {query}")

result = complete_pipeline(query, verbose=False)
collection = result.get('collection')
exec_result = result.get('execution_result', {})
action = exec_result.get('action')
data = exec_result.get('result')

print(f"Collection: {collection}")
print(f"Action: {action}")

if isinstance(data, list):
    print(f"Records returned: {len(data)}")
    if data:
        print(f"Sample record keys: {list(data[0].keys())}")
        print(f"First record: {data[0]}")
else:
    print(f"Result type: {type(data)} - {data}")

# Test 2: Merchant filtering
print("\n" + "=" * 80)
print("2. Testing MERCHANT FILTERING")
print("-" * 40)

queries = [
    "show me transactions of Chicken Republic last month",
    "show me transactions of AWOOF last month",
]

for query in queries:
    print(f"\nQuery: {query}")
    result = complete_pipeline(query, verbose=False)
    
    exec_result = result.get('execution_result', {})
    data = exec_result.get('result')
    
    if isinstance(data, list):
        print(f"Records: {len(data)}")
        if data:
            # Check what merchant name we got
            merchants = set()
            for rec in data[:3]:  # Check first 3
                for field in ['name', 'businessName', 'merchantName', 'transactionMerchant']:
                    if field in rec:
                        merchants.add(str(rec.get(field, '')))
            print(f"Merchants found: {merchants}")
    else:
        print(f"Result: {data}")

print("\n" + "=" * 80)
