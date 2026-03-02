#!/usr/bin/env python
"""Test the complete pipeline to verify filtering"""
import sys
from pathlib import Path
import json

# Add paths
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters"))

from complete_pipeline import complete_pipeline

print("=" * 80)
print("TESTING PIPELINE WITH 3 MONTHS BACK")
print("=" * 80)

result = complete_pipeline("show me customers added 3 months back", verbose=True)

print("\n" + "=" * 80)
print("RESULT SUMMARY")
print("=" * 80)

# Get the execution result
exec_result = result.get('execution_result', {})
print(f"Action: {exec_result.get('action')}")
print(f"Collection: {exec_result.get('collection')}")

if isinstance(exec_result.get('result'), list):
    print(f"Records returned: {len(exec_result['result'])}")
    if exec_result['result']:
        sample = exec_result['result'][0]
        print(f"\nSample record columns: {list(sample.keys())}")
        print(f"Number of columns: {len(sample.keys())}")
        print(f"\nSample record (first 3 fields):")
        for i, (k, v) in enumerate(list(sample.items())[:3]):
            print(f"  {k}: {v}")
        
        # Check for dates
        if 'createdDt' in sample:
            print(f"\n✓ createdDt found: {sample['createdDt']}")
else:
    print(f"Result type: {type(exec_result.get('result'))}")
    print(f"Result: {exec_result.get('result')}")
