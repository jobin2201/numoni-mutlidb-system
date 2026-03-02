import sys
from pathlib import Path
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters"))

from action_executor import filter_important_columns
from field_extractor import extract_requested_fields, map_fields_to_columns
import pandas as pd

query = "show me payout notification. I want currency, account number and merchant fee"

print(f"Query: {query}\n")

# Simulate MongoDB data with accountNumber
mongo_data = [
    {'type': 'payout', 'currency': 'NGN', 'merchantFee': '50', 'accountNumber': '1234567890', 
     'payoutAmount': '10000', 'bankName': 'GTBank'},
]

print(f"[1] Raw data: {list(mongo_data[0].keys())}")

# Step 2: Filter important columns
filtered = filter_important_columns(mongo_data, 'payoutNotification')
print(f"[2] After filter: {list(filtered[0].keys())}")

# Step 3: Extract fields
fields = extract_requested_fields(query)
print(f"[3] Extracted: {fields}")

if fields:
    df = pd.DataFrame(filtered)
    mapping = map_fields_to_columns(fields, list(df.columns))
    print(f"[4] Mapping: {mapping}")
    
    cols = [mapping[f] for f in fields if f in mapping and mapping[f] in df.columns]
    print(f"[5] Columns to show: {cols}")
    
    if cols:
        print(f"✅ SUCCESS - Will display: {cols}")
    else:
        print(f"❌ FAILED - No columns matched")
