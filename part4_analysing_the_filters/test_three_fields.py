"""
Test the problematic query: currency, account number and merchant fee
"""
import sys
from pathlib import Path
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters"))

from action_executor import filter_important_columns
from field_extractor import extract_requested_fields, map_fields_to_columns
import pandas as pd

# Test query
query = "show me payout notification. I want currency, account number and merchant fee"

print("="*80)
print(f"Query: {query}")
print("="*80)

# Step 1: Simulate MongoDB data
mongo_data = [
    {'_id': '1', 'type': 'payout', 'currency': 'NGN', 'merchantFee': '50.00', 
     'accountNumber': '1234567890', 'accountName': 'John Doe',
     'payoutAmount': '10000', 'status': 'completed', 'otherField': 'xyz'},
    {'_id': '2', 'type': 'transfer', 'currency': 'USD', 'merchantFee': '25.00', 
     'accountNumber': '0987654321', 'accountName': 'Jane Smith',
     'payoutAmount': '5000', 'status': 'pending', 'extraField': 'abc'},
]

print(f"\n[1] Raw MongoDB data columns: {list(mongo_data[0].keys())}")

# Step 2: Apply filter_important_columns
filtered_data = filter_important_columns(mongo_data, collection_name='payoutNotification')
print(f"[2] After filter_important_columns: {list(filtered_data[0].keys())}")

if 'accountNumber' not in filtered_data[0]:
    print(f"    ❌ accountNumber was FILTERED OUT!")
else:
    print(f"    ✅ accountNumber is preserved")

# Step 3: Convert to DataFrame
df = pd.DataFrame(filtered_data)
print(f"[3] DataFrame columns: {list(df.columns)}")

# Step 4: Extract requested fields
requested_fields = extract_requested_fields(query)
print(f"[4] Extracted fields from query: {requested_fields}")

if requested_fields:
    # Step 5: Map to DataFrame columns
    available_cols = list(df.columns)
    field_mapping = map_fields_to_columns(requested_fields, available_cols)
    print(f"[5] Field mapping:")
    for user_field, actual_col in field_mapping.items():
        match_status = "✅" if actual_col in available_cols else "❌"
        print(f"    {match_status} '{user_field}' → '{actual_col}'")
    
    # Step 6: Filter DataFrame
    cols_to_show = [field_mapping[f] for f in requested_fields 
                   if f in field_mapping and field_mapping[f] in available_cols]
    print(f"\n[6] Columns to show: {cols_to_show}")
    
    if cols_to_show:
        df_filtered = df[cols_to_show]
        
        # Step 7: Capitalize column names
        rename_map = {col: col[0].upper() + col[1:] for col in cols_to_show}
        df_final = df_filtered.rename(columns=rename_map)
        
        print(f"[7] Final columns (capitalized): {list(df_final.columns)}")
        print(f"\n[8] Final DataFrame:")
        print(df_final)
        print(f"\n✅ SUCCESS! User will see columns: {list(df_final.columns)}")
    else:
        print(f"\n❌ FAILED: No matching columns found")
        print(f"    Requested fields: {requested_fields}")
        print(f"    Available columns: {available_cols}")
        print(f"    Field mapping: {field_mapping}")
else:
    print(f"\n❌ FAILED: Field extraction returned None")

print("="*80)
