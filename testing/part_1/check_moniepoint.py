import json

# Load merchant data
with open('databases/numoni_merchant/merchantDetails.json') as f:
    merchants = json.load(f)

print(f"Total merchants: {len(merchants)}\n")

# Search for moniepoint
moniepoint_records = []
for m in merchants:
    full_text = json.dumps(m).lower()
    if 'moniepoint' in full_text:
        moniepoint_records.append(m)

print(f"Found {len(moniepoint_records)} records with 'moniepoint'\n")

for rec in moniepoint_records[:5]:
    print(f"businessName: {rec.get('businessName')}")
    print(f"brandName: {rec.get('brandName')}")
    print(f"registeredBusiness: {rec.get('registeredBusiness')}")
    print(f"description: {rec.get('description', '')[:100] if rec.get('description') else 'N/A'}")
    print("---")

# Also check what all unique business names are
business_names = set()
for m in merchants:
    name = m.get('businessName', '').strip().lower()
    if name:
        business_names.add(name)

print(f"\nTotal unique business names: {len(business_names)}")
print("\nFirst 20 business names:")
for name in sorted(business_names)[:20]:
    print(f"  - {name}")
