import re

query = "customers from the last 2 weeks"
print(f"Query: {query}")

# Test pattern
weeks_pattern = r'(?:last\s+)?(\d+)\s+weeks?\s+(?:back|behind|ago|old)?'
print(f"\nPattern: {weeks_pattern}")

match = re.search(weeks_pattern, query)
print(f"Match: {match}")

if match:
    print(f"Matched text: '{match.group(0)}'")
    print(f"Group 1 (number): {match.group(1)}")
else:
    print("No match found!")

# Try alternative pattern
print("\n--- Testing alternative patterns ---")

patterns_to_test = [
    (r'last\s+(\d+)\s+weeks?', "last N weeks"),
    (r'(?:last|past)\s+(\d+)\s+weeks?', "last/past N weeks"),
    (r'(\d+)\s+weeks?(?:\s+ago|back|old)?', "N weeks (flexible)"),
]

for pattern, desc in patterns_to_test:
    match = re.search(pattern, query)
    print(f"{desc}: {match.group(0) if match else 'NO MATCH'}")
