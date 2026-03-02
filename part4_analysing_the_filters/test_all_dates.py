#!/usr/bin/env python
"""Test all date filter patterns"""
import sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from advanced_filter_detector import detect_advanced_filters

print("=" * 80)
print("TESTING ALL DATE FILTER PATTERNS")
print("=" * 80)
print(f"Current date: {datetime.now().strftime('%B %d, %Y')}\n")

test_queries = [
    "show me customers added last week",
    "customers from the last 2 weeks",
    "show me transactions from the last 7 days",
    "customers added 3 months back",
    "show me merchants from last month",
    "data added in the last 24 hours",
    "show me records from last 30 minutes",
    "last 5 seconds of transactions",
    "customers added 1 year back",
    "show me merchants from last year",
    "newly added customers",
]

for query in test_queries:
    print(f"Query: '{query}'")
    result = detect_advanced_filters(query)
    
    if result.get('has_advanced_filters'):
        filters = result.get('date_filters', {})
        if filters:
            date_type = filters.get('type', 'N/A')
            print(f"  ✓ Date Type: {date_type}")
            
            if 'start_date' in filters and 'end_date' in filters:
                start = filters['start_date'].strftime('%Y-%m-%d %H:%M:%S')
                end = filters['end_date'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"    Range: {start} to {end}")
            
            if 'year' in filters:
                print(f"    Year: {filters['year']}")
            if 'month' in filters:
                print(f"    Month: {filters['month']}")
    else:
        print(f"  ✗ No date filter detected")
    
    print()
