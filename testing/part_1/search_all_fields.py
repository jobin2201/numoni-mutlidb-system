import json

with open('databases/numoni_merchant/merchantDetails.json') as f:
    merchants = json.load(f)

print("Searching all fields for 'moniepoint'...\n")

found_any = False
for m in merchants:
    for key, value in m.items():
        if isinstance(value, str) and 'moniepoint' in value.lower():
            print(f"Found in field '{key}': {value}")
            print(f"  Full merchant: {m.get('businessName')} / {m.get('brandName')}")
            found_any = True

if not found_any:
    print("'moniepoint' NOT FOUND in any merchant record")

print("\n\nChecking bankInformation.json for moniepoint...")
try:
    with open('databases/numoni_merchant/bankInformation.json') as f:
        bank_data = json.load(f)
    
    moniepoint_banks = [b for b in bank_data if 'moniepoint' in json.dumps(b).lower()]
    print(f"Found {len(moniepoint_banks)} bank records with moniepoint")
    
    if moniepoint_banks:
        print(f"\nSample bank record: {moniepoint_banks[0]}")
except Exception as e:
    print(f"Error: {e}")
