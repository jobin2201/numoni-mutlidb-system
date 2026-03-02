import sys
sys.path.append('.')
from field_extractor import extract_requested_fields, map_fields_to_columns

# Test query
query = "show me regions in Nigeria, i only want for state and lga"

# Step 1: Extract fields (already parsed)
fields = extract_requested_fields(query)
print(f"1. Extracted fields: {fields}")

# Step 2: Map to columns (simulate DataFrame columns)
if fields:
    available_cols = ["_id", "region", "state", "lga", "population"]
    mapping = map_fields_to_columns(fields, available_cols)
    print(f"2. Column mapping: {mapping}")
    
    # Step 3: Show what would be displayed
    cols_to_show = [mapping[f] for f in fields if f in mapping]
    print(f"3. Columns to show: {cols_to_show}")
    print(f"4. Display message: Showing requested fields: {', '.join(cols_to_show)}")
