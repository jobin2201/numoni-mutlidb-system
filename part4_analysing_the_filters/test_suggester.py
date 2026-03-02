from query_suggester import get_query_suggestions, search_keywords

print("=" * 70)
print("QUERY SUGGESTER TEST")
print("=" * 70)

# Test 1
print("\n📝 Query: 'show me transaction from merchants'")
suggestions = get_query_suggestions('show me transaction from merchants')
for sugg in suggestions:
    print(f"  • {sugg['table']:<30s} ({sugg['database']:<15s}) score: {sugg['score']}")

# Test 2
print("\n📝 Query: 'customer location and city'")
suggestions = get_query_suggestions('customer location and city')
for sugg in suggestions:
    print(f"  • {sugg['table']:<30s} ({sugg['database']:<15s}) score: {sugg['score']}")

# Test 3
print("\n📝 Keyword search for 'location':")
results = search_keywords('location', limit=8)
for table, db in results.items():
    print(f"  • {table:<30s} ({db})")

# Test 4
print("\n📝 Keyword search for 'merchant':")
results = search_keywords('merchant', limit=8)
for table, db in results.items():
    print(f"  • {table:<30s} ({db})")

print("\n" + "=" * 70)
print("✅ QUERY SUGGESTER IS READY")
print("=" * 70)
