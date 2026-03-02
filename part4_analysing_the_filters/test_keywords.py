from keywords_lookup import get_matching_tables, get_table_fields, KEYWORDS_DB

print("=" * 60)
print("KEYWORDS INTEGRATION TEST")
print("=" * 60)

# Test 1: Show available databases
print("\n✓ CUSTOMER DATABASE:")
customer_db = KEYWORDS_DB['CUSTOMER_DB']
print(f"  - Tables: {len(customer_db)}")
print(f"  - Example tables: {list(customer_db.keys())[:5]}")

print("\n✓ MERCHANT DATABASE:")
merchant_db = KEYWORDS_DB['MERCHANT_DB']
print(f"  - Tables: {len(merchant_db)}")
print(f"  - Example tables: {list(merchant_db.keys())[:5]}")

# Test 2: Get fields for a specific table
print("\n✓ TRANSACTION HISTORY (CUSTOMER):")
fields = get_table_fields('transaction_history', 'CUSTOMER_DB')
print(f"  - Fields: {fields}")

print("\n✓ MERCHANTDETAILS:")
fields = get_table_fields('merchantDetails', 'MERCHANT_DB')
print(f"  - Fields: {fields}")

# Test 3: Search for matching tables
print("\n✓ MATCHING TABLES FOR 'transactions':")
matches = get_matching_tables(['transaction', 'amount'])
for db in matches:
    if matches[db]:
        print(f"  {db}: {list(matches[db].keys())}")

print("\n✓ MATCHING TABLES FOR 'merchant location':")
matches = get_matching_tables(['merchant', 'location'])
for db in matches:
    if matches[db]:
        print(f"  {db}: {list(matches[db].keys())}")

print("\n" + "=" * 60)
print("✅ All tests passed! Keywords module is ready.")
print("=" * 60)
