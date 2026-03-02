"""
End-to-end test simulating the exact app flow
"""
import sys
sys.path.append('.')
from field_extractor import extract_requested_fields, map_fields_to_columns
import pandas as pd

# Simulate the exact MongoDB query result for regions in Nigeria
mongo_result = [
    {'state': 'Abia', 'lga': 'Aba North', 'region': 'Abia', 'country': 'Nigeria'},
    {'state': 'Abia', 'lga': 'Aba South', 'region': 'Abia', 'country': 'Nigeria'},
    {'state': 'Abia', 'lga': 'Arochukwu', 'region': 'Abia', 'country': 'Nigeria'},
]

# The user's exact query
query = "show me regions in Nigeria, i only want for state and lga"

print("="*80)
print(f"QUERY: {query}")
print("="*80)

# Step 1: Create DataFrame from MongoDB result
df = pd.DataFrame(mongo_result)
print(f"\n[1] Original DataFrame from MongoDB:")
print(f"    Columns: {list(df.columns)}")
print(f"    Data:")
print(df)

# Step 2: Extract requested fields
print(f"\n[2] Extracting requested fields from query...")
requested_fields = extract_requested_fields(query)
print(f"    extract_requested_fields() returned:  {requested_fields}")

if requested_fields:
    # Step 3: Get available columns
    available_cols = list(df.columns)
    print(f"\n[3] Available DataFrame columns: {available_cols}")
    
    # Step 4: Map field names to actual column names
    field_mapping = map_fields_to_columns(requested_fields, available_cols)
    print(f"\n[4] Field mapping (user_field → actual_column):")
    for user_field, actual_col in field_mapping.items():
        print(f"    '{user_field}' → '{ actual_col}'")
    
    # Step 5: Get columns to show
    cols_to_show = [field_mapping[user_field] for user_field in requested_fields if user_field in field_mapping]
    cols_to_show = [col for col in cols_to_show if col in available_cols]
    print(f"\n[5] Columns to show in DataFrame: {cols_to_show}")
    
    if cols_to_show:
        # Step 6: Filter DataFrame
        df_filtered = df[cols_to_show]
        print(f"\n[6] DataFrame after filtering:")
        print(f"    Columns: {list(df_filtered.columns)}")
        print(df_filtered)
        
        # Step 7: Create rename mapping
        rename_map = {col: col[0].upper() + col[1:] if len(col) > 0 else col for col in cols_to_show}
        print(f"\n[7] Rename mapping (DataFrame_column → Display_name):")
        for old_col, new_col in rename_map.items():
            print(f"    '{old_col}' → '{new_col}'")
        
        # Step 8: Rename columns
        df_final = df_filtered.rename(columns=rename_map)
        print(f"\n[8] DataFrame after renaming columns:")
        print(f"    Columns: {list(df_final.columns)}")
        print(f"    Data:")
        print(df_final)
        
        # Step 9: What gets displayed
        display_names = list(rename_map.values())
        print(f"\n[9] FINAL RESULT:")
        print(f"    Column headers that will be displayed: {display_names}")
        print(f"    Info message: 'Showing requested fields: {', '.join(display_names)}'")
        print(f"\n✅ SUCCESS! Columns will show as: {', '.join(display_names)}")
    else:
        print(f"\n❌ FAILED: Could not find any of the requested fields in the DataFrame")
else:
    print(f"\n❌ FAILED: Field extraction returned None - the pattern didn't match or validation failed")

print("\n" + "="*80)
