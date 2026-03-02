import sys
sys.path.append('.')
from field_extractor import is_likely_field_name, extract_requested_fields

test_texts = [
    "for state and lga",
    "state and lga",
    "state, lga",
    "for name and phone",
]

print("Testing is_likely_field_name():")
print("="*70)
for text in test_texts:
    result = is_likely_field_name(text)
    print(f"is_likely_field_name('{text}'): {result}")

print("\n" + "="*70)
print("Testing extract_requested_fields():")
print("="*70)

queries = [
    "show me regions in Nigeria, i only want for state and lga",
    "show me regions, i only want state and lga",
    "show me merchants, i want for name and phone",
]

for query in queries:
    result = extract_requested_fields(query)
    print(f"\nQuery: {query}")
    print(f"Result: {result}")
