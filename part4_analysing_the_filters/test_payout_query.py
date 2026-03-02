"""
Test the payout notification query
"""
import sys
sys.path.append('.')
from field_extractor import extract_requested_fields, parse_fields_from_text, is_likely_field_name
import pandas as pd

# Test query that's failing
query = "show me payout notification. I want currency and merchant fee"

print("="*80)
print(f"Query: {query}")
print("="*80)

# Step 1: Extract fields
print(f"\n[1] Testing extract_requested_fields()...")
extracted = extract_requested_fields(query)
print(f"    Result: {extracted}")

# Step 2: Test with lowercase
query_lower = query.lower()
print(f"\n[2] Query (lowercase): {query_lower}")

# Step 3: Manual regex test
import re
want_pattern = r'[,\.\s]\s*i\s+want\s+([a-z,\s]+?)(?:\s*$)'
match = re.search(want_pattern, query_lower)
if match:
    print(f"\n[3] Regex pattern matched!")
    print(f"    Full match: '{match.group(0)}'")
    print(f"    Captured text: '{match.group(1)}'")
    
    # Test if it passes validation
    captured_text = match.group(1)
    print(f"\n[4] Testing is_likely_field_name('{captured_text}')...")
    is_valid = is_likely_field_name(captured_text)
    print(f"    Result: {is_valid}")
    
    if is_valid:
        print(f"\n[5] Parsing fields from text...")
        fields = parse_fields_from_text(captured_text)
        print(f"    Parsed fields: {fields}")
else:
    print(f"\n[3] ❌ Regex pattern DID NOT match!")
    print(f"    Pattern: {want_pattern}")

# Test what columns might be available in payout notification
print(f"\n" + "="*80)
print("Simulating DataFrame columns from payout notification collection:")
sample_columns = ['_id', 'type', 'currency', 'merchantFee', 'payoutAmount', 'status']
print(f"Available columns: {sample_columns}")

if extracted:
    from field_extractor import map_fields_to_columns
    mapping = map_fields_to_columns(extracted, sample_columns)
    print(f"\nField mapping: {mapping}")
    cols_to_show = [mapping[f] for f in extracted if f in mapping]
    print(f"Columns to show: {cols_to_show}")
    
    # Capitalize
    rename_map = {col: col[0].upper() + col[1:] if len(col) > 0 else col for col in cols_to_show}
    print(f"Display names: {list(rename_map.values())}")
