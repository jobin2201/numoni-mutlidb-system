import sys
sys.path.append('.')
from field_extractor import extract_requested_fields
import pandas as pd

# Simulate the exact query
query = "show me regions in Nigeria, i only want for state and lga"

print("="*70)
print(f"Query: {query}")
print("="*70)

# Step 1: Extract requested fields
requested_fields = extract_requested_fields(query)
print(f"\n1. extract_requested_fields() returned: {requested_fields}")
print(f"   Type: {type(requested_fields)}")

# Step 2: Simulate what app does if fields are extracted
if requested_fields:
    print(f"\n2. Fields detected, setting up column mapping...")
    print(f"   requested_fields = {requested_fields}")
    
    # Simulate DataFrame columns from Nigeria regions query
    test_data = {
        'region': ['Abia', 'Abia', 'Abia'],
        'state': ['Abia', 'Abia', 'Abia'],
        'lga': ['Aba North', 'Aba South', 'Arochukwu']
    }
    df = pd.DataFrame(test_data)
    print(f"\n3. Original DataFrame columns: {list(df.columns)}")
    
    from field_extractor import map_fields_to_columns
    
    # Map fields to columns
    available_cols = list(df.columns)
    field_mapping = map_fields_to_columns(requested_fields, available_cols)
    print(f"\n4. field_mapping returned: {field_mapping}")
    
    # Get columns to show
    cols_to_show = [field_mapping[user_field] for user_field in requested_fields if user_field in field_mapping]
    print(f"\n5. cols_to_show: {cols_to_show}")
    
    # Filter DataFrame
    df = df[cols_to_show]
    print(f"\n6. DataFrame after filtering - columns: {list(df.columns)}")
    
    # Rename columns to capitalize
    rename_map = {col: col[0].upper() + col[1:] if len(col) > 0 else col for col in cols_to_show}
    print(f"\n7. rename_map: {rename_map}")
    
    df = df.rename(columns=rename_map)
    print(f"\n8. DataFrame after rename - columns: {list(df.columns)}")
    print(f"\n9. Final DataFrame:")
    print(df)
    
else:
    print(f"\n❌ No fields extracted! This explains why 'for state' is showing.")
