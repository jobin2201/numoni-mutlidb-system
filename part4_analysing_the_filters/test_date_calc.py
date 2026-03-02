from datetime import datetime
from advanced_filter_detector import detect_advanced_filters

print("=== Testing Date Filters ===\n")

# Test 2 months back
print("Query: 'show me customers added 2 months back'")
result = detect_advanced_filters('show me customers added 2 months back')
date_filters = result.get('date_filters', {})
print(f"Start date: {date_filters.get('start_date')}")
print(f"End date: {date_filters.get('end_date')}")
if date_filters.get('start_date'):
    print(f"Month/Year: {date_filters['start_date'].strftime('%B %Y')}")
print(f"Expected: December 2025 ONLY\n")

# Test 3 months back
print("Query: 'show me customers added 3 months back'")
result = detect_advanced_filters('show me customers added 3 months back')
date_filters = result.get('date_filters', {})
print(f"Start date: {date_filters.get('start_date')}")
print(f"End date: {date_filters.get('end_date')}")
if date_filters.get('start_date'):
    print(f"Month/Year: {date_filters['start_date'].strftime('%B %Y')}")
print(f"Expected: November 2025 ONLY\n")

# Test 6 months back
print("Query: 'show me customers added 6 months back'")
result = detect_advanced_filters('show me customers added 6 months back')
date_filters = result.get('date_filters', {})
print(f"Start date: {date_filters.get('start_date')}")
print(f"End date: {date_filters.get('end_date')}")
if date_filters.get('start_date'):
    print(f"Month/Year: {date_filters['start_date'].strftime('%B %Y')}")
print(f"Expected: August 2025 ONLY")
