#!/usr/bin/env python
"""Debug field extraction"""
import re
from field_extractor import is_likely_field_name, parse_fields_from_text

query = 'show me regions in Nigeria, i only want for state and lga'
query_lower = query.lower()

# Check pattern matching
want_pattern = r'[,]\s*i\s+(?:only\s+)?want\s+([a-z,\s]+?)(?:\s*$)'
match = re.search(want_pattern, query_lower)

print(f"Query: {query}")
print(f"Pattern match: {bool(match)}")

if match:
    fields_text = match.group(1)
    print(f"Extracted text: '{fields_text}'")
    
    print(f"\nis_likely_field_name('{fields_text}'): {is_likely_field_name(fields_text)}")
    
    # Now test after "for" removal
    if fields_text.strip().startswith('for '):
        cleaned = fields_text.strip()[4:].strip()
        print(f"After removing 'for ': '{cleaned}'")
        print(f"is_likely_field_name('{cleaned}'): {is_likely_field_name(cleaned)}")
    
    print(f"\nParsed fields: {parse_fields_from_text(fields_text)}")
