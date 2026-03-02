#!/usr/bin/env python
"""Test complete flow for heading and description extraction"""

from field_extractor import extract_requested_fields, map_fields_to_columns
import sys
sys.path.append('../part3_analysing_the_action')
from action_executor import filter_important_columns

# Simulate the query
query = "show me customer sponsored deals . I want heading and description"

print(f"Query: {query}")
print("=" * 80)

# Step 1: Field extraction
print("\n[1] Field Extraction:")
fields = extract_requested_fields(query)
print(f"    Extracted fields: {fields}")

# Step 2: Simulate DataFrame columns from customer_sponsored_deals collection
# Based on typical deals collection structure
sample_columns = ['_id', 'heading', 'description', 'merchantId', 'amount', 
                  'discount', 'startDate', 'endDate', 'status', 'createdDt']
print(f"\n[2] Available columns in collection: {sample_columns}")

# Step 3: Map fields to columns
if fields:
    print(f"\n[3] Field Mapping:")
    field_map = map_fields_to_columns(fields, sample_columns)
    print(f"    Mapped fields: {field_map}")
    matched_cols = [v for k, v in field_map.items() if v]
    print(f"    Matched columns: {matched_cols}")
else:
    print("\n[3] No fields extracted - would show all columns")

# Step 4: Test filter_important_columns for deals
print(f"\n[4] Testing filter_important_columns for 'customer_sponsored_deals':")
sample_data = [
    {'_id': '123', 'heading': 'Deal 1', 'description': 'Great deal', 'merchantId': 'M1', 
     'amount': 1000, 'discount': 10, 'startDate': '2024-01-01', 'endDate': '2024-12-31', 
     'status': 'active', 'createdDt': '2024-01-01'}
]
filtered = filter_important_columns(sample_data, 'customer_sponsored_deals')
print(f"    Filtered fields: {list(filtered[0].keys()) if filtered else 'None'}")
