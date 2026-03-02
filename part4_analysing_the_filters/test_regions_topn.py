#!/usr/bin/env python
"""Debug top N regions"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters"))

from complete_pipeline import complete_pipeline

print("=" * 80)
print("DEBUG: TOP 2 REGIONS QUERY")
print("=" * 80)

query = "show me top 2 regions in Nigeria"
print(f"\nQuery: {query}")

result = complete_pipeline(query, verbose=True)

print("\n" + "=" * 80)
print("EXECUTION RESULT")
print("=" * 80)

exec_result = result.get('execution_result', {})
print(f"Action: {exec_result.get('action')}")
print(f"Collection: {exec_result.get('collection')}")

data_result = exec_result.get('result')
print(f"Result type: {type(data_result)}")
print(f"Result: {data_result}")

if isinstance(data_result, list):
    print(f"Records count: {len(data_result)}")
    if data_result:
        print(f"First record: {data_result[0]}")
