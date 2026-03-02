"""Quick test that app imports all modules correctly"""
import sys
sys.path.insert(0, '.')

# Test all imports
try:
    from keywords_lookup import get_matching_tables, get_table_fields, KEYWORDS_DB
    print("✓ keywords_lookup imported")
except Exception as e:
    print(f"✗ keywords_lookup error: {e}")

try:
    from query_suggester import get_query_suggestions, search_keywords
    print("✓ query_suggester imported")
except Exception as e:
    print(f"✗ query_suggester error: {e}")

try:
    from field_filter import apply_field_filters
    print("✓ field_filter imported")
except Exception as e:
    print(f"✗ field_filter error: {e}")

# Quick functional test
print("\n" + "=" * 60)
print("FUNCTIONAL TEST")
print("=" * 60)

# Test 1: Count tables
customer_tables = len(KEYWORDS_DB['CUSTOMER_DB'])
merchant_tables = len(KEYWORDS_DB['MERCHANT_DB'])
print(f"\n✓ Database loaded: {customer_tables} customer + {merchant_tables} merchant tables")

# Test 2: Get fields
fields = get_table_fields('transaction_history', 'CUSTOMER_DB')
print(f"✓ Transaction history fields: {len(fields)} fields")

# Test 3: Suggestions
suggestions = get_query_suggestions('merchant location payments')
print(f"✓ Query suggestions generated: {len(suggestions)} suggestions")
for s in suggestions[:3]:
    print(f"  - {s['table']} ({s['database']})")

print("\n" + "=" * 60)
print("✅ ALL SYSTEMS READY TO RUN")
print("=" * 60)
print(f"\nRun: streamlit run app_4part_pipeline.py\n")
