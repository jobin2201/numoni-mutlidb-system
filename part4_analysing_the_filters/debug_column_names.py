import sys
sys.path.append('.')
from field_extractor import extract_requested_fields, map_fields_to_columns

# Test with different queries
queries = [
    "show me regions in Nigeria, i only want for state and lga",
    "show me merchants, i want for name and phone",
    "show me customers, i only want for email and region",
]

for query in queries:
    print(f"\n{'='*60}")
    print(f"Query: {query}")
    print('='*60)
    
    # Step 1: Extract fields
    fields = extract_requested_fields(query)
    print(f"1. Extracted fields: {fields}")
    
    # Step 2: Map to columns (simulate typical DataFrame columns)
    # Simulate what different collections might have
    if "region" in query:
        available_cols = ["_id", "region", "state", "lga", "population"]
    elif "merchant" in query:
        available_cols = ["_id", "merchantName", "merchantPhone", "merchantEmail"]
    else:
        available_cols = ["_id", "customerName", "customerEmail", "region"]
    
    print(f"2. Available columns in DataFrame: {available_cols}")
    
    if fields:
        mapping = map_fields_to_columns(fields, available_cols)
        print(f"3. Column mapping returned: {mapping}")
        
        # Step 3: Show what would be displayed
        cols_to_show = [mapping[f] for f in fields if f in mapping]
        print(f"4. Columns to show in DataFrame: {cols_to_show}")
        print(f"5. Display message: Showing requested fields: {', '.join(cols_to_show)}")
