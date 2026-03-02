import re

query = "show me regions in Nigeria, i only want for state and lga"
query_lower = query.lower()

# Pattern 1: "...I want field1, field2, field3" (after comma, with or without "only")
want_pattern = r'[,]\s*i\s+(?:only\s+)?want\s+([a-z,\s]+?)(?:\s*$)'

print(f"Query: {query}")
print(f"Query (lowercase): {query_lower}")
print(f"\nPattern: {want_pattern}")
print("\nTesting regex match...")

match = re.search(want_pattern, query_lower)
if match:
    print(f"✅ MATCH FOUND!")
    print(f"Full match: '{match.group(0)}'")
    print(f"Captured group 1: '{match.group(1)}'")
else:
    print(f"❌ NO MATCH")

# Also print what the pattern is looking for
print("\n" + "="*70)
print("Pattern breakdown:")
print("[,]        - literal comma")
print(r"\s*        - zero or more whitespace")
print("i\s+       - literal 'i' followed by one or more whitespace")
print("(?:only\s+)?  - optional 'only ' (non-capturing)")
print("want\s+    - literal 'want' followed by one or more whitespace")
print(r"([a-z,\s]+?)  - capture group: one or more letters/commas/spaces (non-greedy)")
print(r"(?:\s*$)   - optional whitespace at end of string")

# Try a simpler version to debug
print("\n" + "="*70)
print("Debugging simpler patterns:")
patterns_to_try = [
    (r'[,].*want\s+([a-z,\s]+?)$', "simplified"),
    (r'i only want\s+([a-z,\s]+?)$', "key part only"),
    (r'want\s+for\s+state\s+and\s+lga', "literal match"),
]

for pattern, desc in patterns_to_try:
    m = re.search(pattern, query_lower)
    if m:
        print(f"✅ {desc}: MATCH - captured: '{m.group(1) if m.groups() else m.group(0)}'")
    else:
        print(f"❌ {desc}: NO MATCH")
