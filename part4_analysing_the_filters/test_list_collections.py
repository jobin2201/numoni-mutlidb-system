#!/usr/bin/env python
"""List all collections to find sharepoint collection"""
import sys
import os
import json

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part2_analysing_the_collection'))

from collection_router import load_metadata

# Load metadata to see all collections
metadata = load_metadata("numoni_merchant")
print("Available collections:")
for coll_name in sorted(metadata.keys()):
    if 'share' in coll_name.lower() or 'points' in coll_name.lower() or 'request' in coll_name.lower():
        print(f"  ✓ {coll_name}")
    else:
        print(f"    {coll_name}")
