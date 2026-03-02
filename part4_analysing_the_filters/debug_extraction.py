#!/usr/bin/env python
"""Debug field extraction"""
import re
from field_extractor import extract_requested_fields, is_likely_field_name, parse_fields_from_text

queries_to_test = [
    'show me the regions in nigeria, i only want regions and state',
    'show me customer details, i want name, phone, email',
    'show me the regions in nigeria, i only want for Abia State',
]

print("\n" + "="*70)
print("TESTING FIELD EXTRACTION")
print("="*70 + "\n")

for query in queries_to_test:
    result = extract_requested_fields(query)
    print(f"Query: {query}")
    print(f"Result: {result}")
    print()

