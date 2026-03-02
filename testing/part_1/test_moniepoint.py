from part1_analysing_the_db.db_keyword_router_fuzzy import detect_database, MERCHANT_NAMES, BANK_NAMES

print(f"Merchant names loaded: {len(MERCHANT_NAMES)}")
print(f"Bank names loaded: {len(BANK_NAMES)}")

# Check if moniepoint is loaded
moniepoint_keys = [k for k in MERCHANT_NAMES.keys() if 'moniepoint' in k]
print(f"\nMonepoint merchant keys: {moniepoint_keys}")

# Test queries
test_queries = [
    "moniepoint",
    "MONIEPOINT",
    "moniepoint microfinance",
    "show me moniepoint",
    "moniepoint microfinance bank",
]

print("\n" + "="*70)
for query in test_queries:
    result = detect_database(query)
    print(f"\nQuery: '{query}'")
    print(f"DB: {result['selected_dbs']}")
    print(f"Reason: {result['reason']}")
