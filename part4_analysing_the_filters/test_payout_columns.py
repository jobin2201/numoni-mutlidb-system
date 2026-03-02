"""
Test that payout notification columns are preserved
"""
import sys
from pathlib import Path
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))

from action_executor import filter_important_columns

# Simulate payout notification data from MongoDB
payout_data = [
    {
        '_id': '123',
        'type': 'payout',
        'currency': 'NGN',
        'merchantFee': '50.00',
        'payoutAmount': '10000.00',
        'status': 'completed',
        'createdDt': '2024-01-15',
        'merchantId': 'M123',
        'otherField': 'should be filtered'
    },
    {
        '_id': '124',
        'type': 'transfer',
        'currency': 'USD',
        'merchantFee': '25.00',
        'payoutAmount': '5000.00',
        'status': 'pending',
        'createdDt': '2024-01-16',
        'merchantId': 'M124',
    }
]

print("="*80)
print("Testing filter_important_columns with payout notification data")
print("="*80)

# Test 1: With 'payout' in collection name
print("\n[Test 1] Collection: 'payoutNotification'")
filtered = filter_important_columns(payout_data, collection_name='payoutNotification')
print(f"Original fields: {list(payout_data[0].keys())}")
print(f"Filtered fields: {list(filtered[0].keys())}")

if 'currency' in filtered[0] and 'merchantFee' in filtered[0]:
    print("✅ SUCCESS: currency and merchantFee are preserved!")
else:
    print("❌ FAILED: currency or merchantFee missing!")
    print(f"Available: {list(filtered[0].keys())}")

# Test 2: With 'notification' in collection name
print("\n[Test 2] Collection: 'notification'")
filtered2 = filter_important_columns(payout_data, collection_name='notification')
print(f"Filtered fields: {list(filtered2[0].keys())}")

if 'currency' in filtered2[0] and 'merchantFee' in filtered2[0]:
    print("✅ SUCCESS: currency and merchantFee are preserved!")
else:
    print("❌ FAILED: currency or merchantFee missing!")

# Test 3: Default (no specific collection)
print("\n[Test 3] Collection: 'someOtherCollection' (default)")
filtered3 = filter_important_columns(payout_data, collection_name='someOtherCollection')
print(f"Filtered fields: {list(filtered3[0].keys())}")

if 'currency' in filtered3[0] and 'merchantFee' in filtered3[0]:
    print("✅ SUCCESS: currency and merchantFee in default list too!")
else:
    print("⚠️  WARNING: Not in default, but should work for payout queries")

print("\n" + "="*80)
