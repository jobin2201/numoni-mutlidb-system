"""Test improved field matching"""
import sys
sys.path.insert(0, '.')

from field_filter import normalize_field_name

# Mock transaction_history record
sample_record = {
    'customerId': 'CUST123',
    'senderName': 'John Doe',
    'transactionReferenceId': 'TXN456789',
    'totalAmountPaid': 5000.00,
    'transactionDate': '2025-12-15',
    'status': 'SUCCESS',
}

# Test user field names
test_fields = [
    'Customer ID',
    'Sender Name',
    'Transaction Reference',
    'total Amount',
]

print("=" * 60)
print("FIELD MATCHING TEST")
print("=" * 60)

for user_field in test_fields:
    db_field = normalize_field_name(user_field, sample_record)
    if db_field:
        print(f"✓ '{user_field}' → '{db_field}'")
    else:
        print(f"✗ '{user_field}' → NOT FOUND")

print("\n" + "=" * 60)
print("✅ All expected fields should match now")
print("=" * 60)
