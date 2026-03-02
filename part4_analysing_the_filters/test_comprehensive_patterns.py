"""
Comprehensive test for various query patterns
"""
import sys
sys.path.append('.')
from field_extractor import extract_requested_fields, map_fields_to_columns

test_cases = [
    {
        "query": "show me regions in Nigeria, i only want for state and lga",
        "available_cols": ["_id", "region", "state", "lga", "population"],
        "description": "Comma separator, 'for' prefix"
    },
    {
        "query": "show me payout notification. I want currency and merchant fee",
        "available_cols": ["_id", "type", "currency", "merchantFee", "payoutAmount", "status"],
        "description": "Period separator, camelCase column"
    },
    {
        "query": "show me customers I need customer name and phone number",
        "available_cols": ["_id", "customerName", "phoneNumber", "email", "status"],
        "description": "No separator (space only), multi-word fields"
    },
    {
        "query": "get merchants, looking for merchant id and bank name",
        "available_cols": ["_id", "merchantId", "bankName", "status"],
        "description": "'looking for' pattern"
    },
    {
        "query": "show transactions with amount and transaction date",
        "available_cols": ["_id", "amount", "transactionDate", "status"],
        "description": "'with' pattern"
    },
]

print("="*90)
print("COMPREHENSIVE FIELD EXTRACTION TESTS")
print("="*90)

for i, test in enumerate(test_cases, 1):
    query = test["query"]
    available_cols = test["available_cols"]
    desc = test["description"]
    
    print(f"\n[Test {i}] {desc}")
    print(f"Query: {query}")
    print(f"Available columns: {available_cols}")
    
    # Extract fields
    fields = extract_requested_fields(query)
    
    if fields:
        print(f"✅ Extracted fields: {fields}")
        
        # Map to columns
        mapping = map_fields_to_columns(fields, available_cols)
        print(f"✅ Field mapping: {mapping}")
        
        # Show what would be displayed
        cols_to_show = [mapping[f] for f in fields if f in mapping and mapping[f] in available_cols]
        if cols_to_show:
            rename_map = {col: col[0].upper() + col[1:] if len(col) > 0 else col for col in cols_to_show}
            display_names = list(rename_map.values())
            print(f"✅ Display names: {display_names}")
        else:
            print(f"❌ No matching columns found")
    else:
        print(f"❌ No fields extracted")
    
    print("-" * 90)

print("\n" + "="*90)
print("TEST COMPLETE")
print("="*90)
