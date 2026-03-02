"""
Verify other collection types are NOT affected
"""
import sys
from pathlib import Path
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))

from action_executor import filter_important_columns

test_cases = [
    {
        'name': 'Customer Details',
        'collection': 'customerDetails',
        'data': [{'_id': '1', 'customerName': 'John', 'email': 'john@test.com', 
                 'phoneNumber': '123456', 'createdDt': '2024-01-01', 'otherField': 'xyz'}],
        'expected_fields': ['customerName', 'email', 'phoneNumber', 'createdDt']
    },
    {
        'name': 'Transactions',
        'collection': 'transactionHistory',
        'data': [{'_id': '1', 'customerId': 'C1', 'merchantId': 'M1', 
                 'totalAmountPaid': '1000', 'status': 'completed', 'currency': 'NGN'}],
        'expected_fields': ['customerId', 'merchantId', 'totalAmountPaid', 'status', 'currency']
    },
    {
        'name': 'Regions',
        'collection': 'regions',
        'data': [{'_id': '1', 'regionName': 'Abia', 'state': 'Abia', 
                 'country': 'Nigeria', 'lga': 'Aba North', 'population': '100000'}],
        'expected_fields': ['regionName', 'state', 'country', 'lga']
    },
]

print("="*80)
print("Testing that other collections are NOT affected")
print("="*80)

all_pass = True
for test in test_cases:
    print(f"\n[{test['name']}] Collection: {test['collection']}")
    filtered = filter_important_columns(test['data'], collection_name=test['collection'])
    
    if filtered:
        actual_fields = list(filtered[0].keys())
        print(f"  Original: {list(test['data'][0].keys())}")
        print(f"  Filtered: {actual_fields}")
        
        # Check that expected fields are present
        missing = [f for f in test['expected_fields'] if f not in actual_fields]
        if missing:
            print(f"  ❌ MISSING: {missing}")
            all_pass = False
        else:
            print(f"  ✅ All expected fields present")
    else:
        print(f"  ❌ No data returned after filtering!")
        all_pass = False

print("\n" + "="*80)
if all_pass:
    print("✅ ALL TESTS PASSED - Other collections work normally")
else:
    print("❌ SOME TESTS FAILED")
