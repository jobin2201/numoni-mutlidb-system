#!/usr/bin/env python
"""Test field extraction for show me queries"""
from field_extractor import extract_requested_fields, parse_fields_from_text

test_queries = [
    "show me customer details, I want Customer name, phone number, date",
    "show me the merchant, I need name, phone and email",
    "display zenith bank with code, processing_fee, settlement_date",
    "show me regions in Nigeria, I want region and state",
    "give me customer info, looking for email, phone",
]

print("\n" + "="*70)
print("TESTING FIELD EXTRACTION FOR 'show me' QUERIES")
print("="*70 + "\n")

for query in test_queries:
    fields = extract_requested_fields(query)
    if fields:
        print(f"✅ Query: {query}")
        print(f"   Extracted fields: {fields}\n")
    else:
        print(f"❌ Query: {query}")
        print(f"   No fields extracted (will show all columns)\n")

print("="*70)
print("HOW IT WORKS:")
print("="*70)
print("""
1. User enters: "show me customer details, I want Customer name, phone number, date"

2. The system:
   a) Runs through normal 4-part pipeline (DB → Collection → Action → Filters)
   b) Gets results with ALL columns
   c) Calls extract_requested_fields()
   d) Finds the pattern "I want Customer name, phone number, date"
   e) Maps those fields to actual DataFrame columns:
      - "Customer name" → "customerName"
      - "phone number" → "phone"
      - "date" → "date"
   f) Shows ONLY those 3 columns in the output

3. If no field specification is found:
   - Shows all columns (normal behavior, not affected)

4. Examples that now work:
   ✅ "show me X, I want A, B, C"
   ✅ "show me X, I need A and B"
   ✅ "show me X, looking for A, B, C"
   ✅ "show me X with A, B, C"
   ✅ "display X, I want A, B"
""")
