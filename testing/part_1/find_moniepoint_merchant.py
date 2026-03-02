import json

# Get merchant ID from bank info
with open('databases/numoni_merchant/bankInformation.json') as f:
    bank_data = json.load(f)

moniepoint_banks = [b for b in bank_data if 'moniepoint' in json.dumps(b).lower()]
if moniepoint_banks:
    merchant_id = moniepoint_banks[0].get('merchantId')
    print(f"Merchant ID for MONIEPOINT: {merchant_id}\n")
    
    # Find the merchant with this ID
    with open('databases/numoni_merchant/merchantDetails.json') as f:
        merchants = json.load(f)
    
    # Handle both string and dict ID formats
    matching_merchant = []
    for m in merchants:
        mid = m.get('_id')
        if isinstance(mid, dict) and mid.get('$oid') == merchant_id:
            matching_merchant.append(m)
        elif isinstance(mid, str) and mid == merchant_id:
            matching_merchant.append(m)
    
    if matching_merchant:
        m = matching_merchant[0]
        print(f"Found merchant:\n")
        print(f"  businessName: {m.get('businessName')}")
        print(f"  brandName: {m.get('brandName')}")
        print(f"  registeredBusiness: {m.get('registeredBusiness')}")
        print(f"  description: {m.get('description')}")
    else:
        print(f"Merchant with ID {merchant_id} not found in merchantDetails.json")
        print("\nFirst few merchant IDs:")
        for m in merchants[:3]:
            print(f"  {m.get('_id')}")
