#!/usr/bin/env python
"""Test field extraction for heading and description"""

from field_extractor import extract_requested_fields, is_likely_field_name

query = "show me customer sponsored deals . I want heading and description"

print(f"Query: {query}")
print(f"\n1. Checking if 'heading and description' looks like field names...")
result = is_likely_field_name("heading and description")
print(f"   is_likely_field_name('heading and description') = {result}")

print(f"\n2. Extracting fields from full query...")
fields = extract_requested_fields(query)
print(f"   extract_requested_fields() returned: {fields}")

print(f"\n3. Breaking down why...")
import re
query_lower = query.lower()
want_pattern = r'[,\.\s]\s*i\s+(?:only\s+)?want\s+([a-z,\s]+?)(?:\s*$)'
match = re.search(want_pattern, query_lower)
if match:
    print(f"   Pattern matched!")
    fields_text = match.group(1)
    print(f"   Captured text: '{fields_text}'")
    print(f"   is_likely_field_name('{fields_text}') = {is_likely_field_name(fields_text)}")
else:
    print(f"   Pattern DID NOT match")
