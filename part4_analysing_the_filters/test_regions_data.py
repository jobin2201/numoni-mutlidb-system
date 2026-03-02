#!/usr/bin/env python
"""Debug regions query"""
import sys
import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent / "part3_analysing_the_action"
sys.path.insert(0, str(BASE_DIR.parent / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR.parent / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR))

from action_executor import load_collection_data

# Load nigeria_regions data directly
print("Loading nigeria_regions data...")
data = load_collection_data("numoni_merchant", "nigeria_regions")

if data:
    print(f"Total regions: {len(data)}")
    print(f"Sample fields: {list(data[0].keys())}")
    print(f"First region: {data[0]}")
else:
    print("No data loaded!")
