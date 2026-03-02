import sys
sys.path.append('.')
from field_extractor import extract_requested_fields, map_fields_to_columns

# Test with different queries
test_cases = [
    {
        "query": "show me regions in Nigeria, i only want for state and lga",
        "available_cols": ["_id", "region", "state", "lga", "population"]
    },
    {
        "query": "show me merchants, i want for name and phone",
        "available_cols": ["_id", "merchantName", "merchantPhone", "merchantEmail"]
    },
    {
        "query": "show me customers, i only want for email and region",
        "available_cols": ["_id", "customerName", "customerEmail", "region"]
    },
]

for test in test_cases:
    query = test["query"]
    available_cols = test["available_cols"]
    
    print(f"\n{'='*70}")
    print(f"Query: {query}")
    print('='*70)
    
    # Step 1: Extract fields
    fields = extract_requested_fields(query)
    print(f"✅ Extracted fields (lowercase): {fields}")
    
    if fields:
        # Step 2: Map to columns
        field_mapping = map_fields_to_columns(fields, available_cols)
        cols_to_show = [field_mapping[f] for f in fields if f in field_mapping]
        
        print(f"✅ DataFrame columns to show: {cols_to_show}")
        
        # Step 3: Capitalize for display
        rename_map = {col: col[0].upper() + col[1:] if len(col) > 0 else col for col in cols_to_show}
        display_names = list(rename_map.values())
        
        print(f"✅ Display names (capitalized): {display_names}")
        print(f"📊 Column rename mapping: {rename_map}")
        print(f"📌 Info message: Showing requested fields: {', '.join(display_names)}")
