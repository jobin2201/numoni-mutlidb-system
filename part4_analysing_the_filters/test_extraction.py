#!/usr/bin/env python
"""Debug text filter extraction"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from advanced_filter_detector import extract_text_filters, detect_advanced_filters

print("=" * 80)
print("DEBUG: TEXT FILTER EXTRACTION")
print("=" * 80)

queries = [
    "show me transactions of Chicken Republic last month",
    "show me transactions of AWOOF last month",
    "show me regions in Nigeria",
    "show me top 2 regions in Nigeria",
]

for query in queries:
    print(f"\nQuery: '{query}'")
    
    # Test direct extraction
    filters = extract_text_filters(query.lower())
    print(f"Text filters: {filters}")
    
    # Test full detection
    result = detect_advanced_filters(query)
    print(f"Has advanced filters: {result.get('has_advanced_filters')}")
    print(f"Text filters (full): {result.get('text_filters')}")
