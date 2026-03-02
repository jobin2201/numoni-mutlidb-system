#!/usr/bin/env python
"""List all collections in customer database"""
import sys
import os
import json

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part2_analysing_the_collection'))

from collection_router import load_metadata

# Load metadata for customer database
metadata = load_metadata("numoni_customer")
print("Available collections in numoni_customer:")
for coll_name in sorted(metadata.keys()):
    if 'share' in coll_name.lower() or 'points' in coll_name.lower() or 'request' in coll_name.lower():
        print(f"  ✓ {coll_name}")
    else:
        print(f"    {coll_name}")
